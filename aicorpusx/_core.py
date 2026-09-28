"""Public translation API and resilient multi-key scheduler."""

from __future__ import annotations

import hashlib
import json
import math
import os
import queue
import random
import re
import sqlite3
import threading
import time
from contextlib import closing
from dataclasses import dataclass, field
from numbers import Integral, Real
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Union,
)

from ._glossary import Glossary, TermConstraint, load_glossary
from ._progress import TranslationProgress
from ._providers import ProviderError, TranslationProvider, make_provider
from ._tables import (
    DEFAULT_CSV_DELIMITER,
    DEFAULT_CSV_ENCODING,
    DEFAULT_FORMULA_POLICY,
    MemoryTable,
    SheetName,
    Table,
    _atomic_output,
    _csv_output_value,
    _validate_csv_options,
    _validate_formula_policy,
    is_filled,
    iter_records,
    open_table,
    read_headers,
    write_records,
)


PathLike = Union[str, os.PathLike[str]]


def _source_signature(path: Path) -> Dict[str, Any]:
    before = path.stat()
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise RuntimeError(f"File changed while it was being inspected: {path}")
    return {
        "path": str(path),
        "size": after.st_size,
        "mtime_ns": after.st_mtime_ns,
        "sha256": digest.hexdigest(),
    }


def _canonical_identity_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, os.PathLike):
        return str(Path(value).expanduser().resolve())
    if isinstance(value, Mapping):
        return {
            str(key): _canonical_identity_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_canonical_identity_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        items = [_canonical_identity_value(item) for item in value]
        return sorted(
            items,
            key=lambda item: json.dumps(
                item,
                ensure_ascii=False,
                sort_keys=True,
                default=repr,
            ),
        )
    return {
        "type": f"{type(value).__module__}.{type(value).__qualname__}",
        "repr": repr(value),
    }


def _configuration_digest(value: Any) -> str:
    payload = json.dumps(
        _canonical_identity_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _glossary_identity(
    glossary: Optional[Union[Mapping[Any, Any], PathLike]],
) -> Optional[Dict[str, Any]]:
    if glossary is None:
        return None
    if isinstance(glossary, Mapping):
        return {"kind": "mapping", "sha256": _configuration_digest(glossary)}
    path = Path(glossary).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Glossary file does not exist: {path}")
    return {"kind": "file", "source": _source_signature(path)}


def _provider_identity(
    provider: Optional[Union[str, TranslationProvider]],
) -> str:
    if provider is None:
        return "openai-compatible"
    if isinstance(provider, str):
        return provider.strip().lower()
    return f"{type(provider).__module__}.{type(provider).__qualname__}"


def _task_id(row_index: int, target_language: str) -> str:
    return f"{row_index}:{target_language}"


def _validate_integer_option(name: str, value: Any, *, minimum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise TypeError(f"{name} must be an integer.")
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}.")


def _validate_time_option(
    name: str,
    value: Any,
    *,
    allow_zero: bool,
) -> None:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise TypeError(f"{name} must be a number.")
    try:
        numeric = float(value)
    except OverflowError:
        raise ValueError(f"{name} must be finite.") from None
    if not math.isfinite(numeric):
        raise ValueError(f"{name} must be finite.")
    if numeric < 0 or (numeric == 0 and not allow_zero):
        qualifier = "non-negative" if allow_zero else "greater than zero"
        raise ValueError(f"{name} must be {qualifier}.")
    if numeric > threading.TIMEOUT_MAX:
        raise ValueError(
            f"{name} exceeds the platform timeout limit "
            f"({threading.TIMEOUT_MAX:g} seconds)."
        )


class _Checkpoint:
    def __init__(
        self,
        path: Path,
        *,
        enabled: bool,
        identity: Mapping[str, Any],
        resume: bool,
    ) -> None:
        self.path = path
        self.enabled = enabled
        self.identity = dict(identity)
        self.lock = threading.Lock()
        self.completed: Dict[str, str] = {}
        self.failures: Dict[str, Dict[str, Any]] = {}
        if enabled and resume:
            self._load()

    def _load(self) -> None:
        if not self.path.is_file():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        if data.get("version") != 1 or data.get("identity") != self.identity:
            return
        completed = data.get("completed")
        failures = data.get("failures")
        if isinstance(completed, dict):
            self.completed = {str(key): str(value) for key, value in completed.items()}
        if isinstance(failures, dict):
            self.failures = {
                str(key): dict(value) for key, value in failures.items() if isinstance(value, dict)
            }

    def _save_locked(self) -> None:
        if not self.enabled:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "version": 1,
            "identity": self.identity,
            "completed": self.completed,
            "failures": self.failures,
        }
        with _atomic_output(self.path) as temporary:
            temporary.write_text(
                json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8",
            )

    def record_success(self, task: "_Task", result: str) -> None:
        if not self.enabled:
            return
        with self.lock:
            self.completed[task.uid] = result
            self.failures.pop(task.uid, None)
            self._save_locked()

    def record_failure(self, task: "_Task", message: str) -> None:
        if not self.enabled:
            return
        with self.lock:
            self.failures[task.uid] = {
                "row_index": task.row_index,
                "target_language": task.target_language,
                "output_column": task.output_column,
                "source_text": task.text,
                "attempts": task.failures,
                "error": message,
            }
            self._save_locked()

    def completed_items(
        self, row_start: int = 0, row_count: Optional[int] = None
    ) -> Iterable[tuple[str, str]]:
        row_end = None if row_count is None else row_start + row_count
        for uid, value in self.completed.items():
            try:
                row_index = int(uid.split(":", 1)[0])
            except (ValueError, IndexError):
                continue
            if row_index < row_start or (row_end is not None and row_index >= row_end):
                continue
            yield uid, value

    def close(self) -> None:
        return


class _SQLiteCheckpoint(_Checkpoint):
    """Disk-backed checkpoint for memory-bounded streaming translation."""

    def __init__(
        self,
        path: Path,
        *,
        enabled: bool,
        identity: Mapping[str, Any],
        resume: bool,
    ) -> None:
        self.path = path
        self.enabled = enabled
        self.identity = dict(identity)
        self.lock = threading.Lock()
        self.connection: Optional[sqlite3.Connection] = None
        if not enabled:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, check_same_thread=False)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                row_index INTEGER NOT NULL,
                target_language TEXT NOT NULL,
                output_column TEXT NOT NULL,
                source_text TEXT NOT NULL,
                status TEXT NOT NULL,
                result TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                PRIMARY KEY (row_index, target_language)
            )
            """
        )
        identity_text = json.dumps(self.identity, ensure_ascii=False, sort_keys=True)
        existing = self.connection.execute(
            "SELECT value FROM metadata WHERE key = 'identity'"
        ).fetchone()
        if not resume or existing is None or existing[0] != identity_text:
            self.connection.execute("DELETE FROM tasks")
        self.connection.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('identity', ?)",
            (identity_text,),
        )
        self.connection.commit()

    def completed_items(
        self, row_start: int = 0, row_count: Optional[int] = None
    ) -> Iterable[tuple[str, str]]:
        if not self.enabled or self.connection is None:
            return []
        if row_count is None:
            rows = self.connection.execute(
                "SELECT row_index, target_language, result FROM tasks "
                "WHERE status = 'success' AND row_index >= ? ORDER BY row_index",
                (row_start,),
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT row_index, target_language, result FROM tasks "
                "WHERE status = 'success' AND row_index >= ? AND row_index < ? "
                "ORDER BY row_index",
                (row_start, row_start + row_count),
            ).fetchall()
        return [
            (_task_id(int(row_index), str(language)), str(result))
            for row_index, language, result in rows
        ]

    def record_success(self, task: "_Task", result: str) -> None:
        if not self.enabled or self.connection is None:
            return
        with self.lock:
            self.connection.execute(
                """
                INSERT OR REPLACE INTO tasks
                (
                    row_index, target_language, output_column, source_text,
                    status, result, attempts, error
                )
                VALUES (?, ?, ?, ?, 'success', ?, ?, NULL)
                """,
                (
                    task.checkpoint_index,
                    task.target_language,
                    task.output_column,
                    task.text,
                    result,
                    task.failures,
                ),
            )
            self.connection.commit()

    def record_failure(self, task: "_Task", message: str) -> None:
        if not self.enabled or self.connection is None:
            return
        with self.lock:
            self.connection.execute(
                """
                INSERT OR REPLACE INTO tasks
                (
                    row_index, target_language, output_column, source_text,
                    status, result, attempts, error
                )
                VALUES (?, ?, ?, ?, 'failure', NULL, ?, ?)
                """,
                (
                    task.checkpoint_index,
                    task.target_language,
                    task.output_column,
                    task.text,
                    task.failures,
                    message,
                ),
            )
            self.connection.commit()

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None


def _detect_mode(text: str) -> str:
    compact = text.strip()
    if len(compact) <= 40 and "\n" not in compact and not re.search(r"[.!?。！？；;]", compact):
        return "term"
    if len(compact) <= 300 and compact.count("\n") <= 1:
        return "sentence"
    return "text"


@dataclass
class _Task:
    row_index: int
    target_language: str
    output_column: str
    text: str
    owner: int
    checkpoint_index: Optional[int] = None
    failures: int = 0
    done: bool = False
    uid: str = field(init=False)

    def __post_init__(self) -> None:
        if self.checkpoint_index is None:
            self.checkpoint_index = self.row_index
        self.uid = _task_id(self.checkpoint_index, self.target_language)


@dataclass
class _WorkerState:
    processed: int = 0
    consecutive_failures: int = 0
    cooldown_count: int = 0
    disabled: bool = False
    started_at: float = field(default_factory=time.monotonic)

    @property
    def rate(self) -> float:
        elapsed = max(0.001, time.monotonic() - self.started_at)
        return self.processed / elapsed


class _Scheduler:
    def __init__(
        self,
        *,
        tasks: Sequence[_Task],
        table: Table,
        checkpoint: _Checkpoint,
        glossary: Glossary,
        glossary_mode: str,
        source_language: str,
        mode: str,
        apis: Sequence[str],
        strategy: str,
        provider: Optional[Union[str, TranslationProvider]],
        model: str,
        base_url: str,
        timeout: float,
        provider_options: Optional[Mapping[str, Any]],
        request_sleep: float,
        max_retries: int,
        backoff_base: float,
        max_backoff: float,
        api_failure_threshold: int,
        api_cooldown: float,
        max_api_cooldown: float,
        show_progress: bool,
    ) -> None:
        self.tasks = list(tasks)
        self.table = table
        self.checkpoint = checkpoint
        self.glossary = glossary
        self.glossary_mode = glossary_mode
        self.source_language = source_language
        self.mode = mode
        self.apis = list(apis)
        self.strategy = strategy
        self.provider_arg = provider
        self.model = model
        self.base_url = base_url
        self.timeout = timeout
        self.provider_options = provider_options
        self.request_sleep = request_sleep
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.max_backoff = max_backoff
        self.api_failure_threshold = api_failure_threshold
        self.api_cooldown = api_cooldown
        self.max_api_cooldown = max_api_cooldown

        self._state_lock = threading.Lock()
        self._result_lock = threading.Lock()
        self._stop = threading.Event()
        self._fatal_error: Optional[BaseException] = None
        self._remaining = len(tasks)
        self._states = [_WorkerState() for _ in apis]
        self._disabled_workers: set[int] = set()
        self._dynamic_queue: "queue.Queue[_Task]" = queue.Queue()
        self._rescue_queue: "queue.Queue[_Task]" = queue.Queue()
        self._balanced_queues: List["queue.Queue[_Task]"] = [queue.Queue() for _ in apis]

        api_totals: Dict[int, Optional[int]] = {index: None for index in range(len(apis))}
        if strategy == "dynamic":
            for task in tasks:
                self._dynamic_queue.put(task)
        else:
            counts = {index: 0 for index in range(len(apis))}
            for task in tasks:
                self._balanced_queues[task.owner].put(task)
                counts[task.owner] += 1
            api_totals = counts
        self.progress = TranslationProgress(
            total=len(tasks), api_totals=api_totals, enabled=show_progress
        )

    def run(self) -> None:
        if not self.tasks:
            return
        with self.progress:
            threads = [
                threading.Thread(
                    target=self._worker_entry,
                    args=(worker_id, api_key),
                    name=f"aicorpusx-api-{worker_id + 1}",
                    daemon=True,
                )
                for worker_id, api_key in enumerate(self.apis)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        if self._fatal_error is not None:
            raise self._fatal_error

    def _worker_entry(self, worker_id: int, api_key: str) -> None:
        try:
            self._worker(worker_id, api_key)
        except BaseException as exc:
            with self._state_lock:
                if self._fatal_error is None:
                    self._fatal_error = exc
            self._stop.set()

    def _make_worker_provider(self) -> TranslationProvider:
        return make_provider(
            self.provider_arg,
            model=self.model,
            base_url=self.base_url,
            timeout=self.timeout,
            options=self.provider_options,
        )

    def _take_task(self, worker_id: int) -> Optional[_Task]:
        if self.strategy == "dynamic":
            try:
                return self._dynamic_queue.get(timeout=0.1)
            except queue.Empty:
                return None

        try:
            return self._balanced_queues[worker_id].get_nowait()
        except queue.Empty:
            pass
        try:
            return self._rescue_queue.get(timeout=0.1)
        except queue.Empty:
            return None

    def _requeue(self, task: _Task) -> None:
        if task.done:
            return
        if self.strategy == "dynamic":
            self._dynamic_queue.put(task)
        else:
            self._rescue_queue.put(task)

    def _worker(self, worker_id: int, api_key: str) -> None:
        state = self._states[worker_id]
        provider = self._make_worker_provider()

        while not self._stop.is_set():
            task = self._take_task(worker_id)
            if task is None:
                with self._state_lock:
                    if self._remaining == 0:
                        self._stop.set()
                        break
                continue
            if task.done:
                continue

            constraints = self.glossary.matches(task.text, task.target_language)
            selected_mode = _detect_mode(task.text) if self.mode == "auto" else self.mode
            try:
                result = provider.translate(
                    api_key=api_key,
                    text=task.text,
                    source_language=self.source_language,
                    target_language=task.target_language,
                    mode=selected_mode,
                    glossary_terms=[item.as_prompt_item() for item in constraints],
                    glossary_mode=self.glossary_mode,
                )
                if not isinstance(result, str) or not result.strip():
                    raise ProviderError("Provider returned an empty translation.", retryable=True)
                result = result.strip()
                if self.glossary_mode == "strict":
                    missing = [item.target for item in constraints if item.target not in result]
                    if missing:
                        raise ProviderError(
                            "Translation did not satisfy strict glossary constraints.",
                            retryable=True,
                        )
            except Exception as exc:
                error = self._normalize_error(exc)
                task.failures += 1
                state.consecutive_failures += 1
                if error.disable_api:
                    self._disable_worker(worker_id, task, str(error))
                    return
                if not error.retryable or task.failures > self.max_retries:
                    self._finalize_failure(task, str(error))
                    self._update_worker(worker_id, "Active")
                    continue
                if self._stop.is_set():
                    self._finalize_failure(task, str(error))
                    continue

                if state.consecutive_failures >= self.api_failure_threshold:
                    state.cooldown_count += 1
                    duration = min(
                        self.max_api_cooldown,
                        self.api_cooldown * (2 ** max(0, state.cooldown_count - 1)),
                    )
                    state.consecutive_failures = 0
                    self._requeue(task)
                    self._update_worker(worker_id, "Cooldown")
                    self._stop.wait(duration)
                    self._update_worker(worker_id, "Active")
                    continue

                delay = min(
                    self.max_backoff,
                    self.backoff_base * (2 ** max(0, task.failures - 1)),
                )
                jitter = random.uniform(0.0, min(1.0, delay * 0.25)) if delay > 0 else 0.0
                self._requeue(task)
                self._update_worker(worker_id, "Cooldown")
                self._stop.wait(min(self.max_backoff, delay + jitter))
                self._update_worker(worker_id, "Active")
                continue

            state.processed += 1
            state.consecutive_failures = 0
            state.cooldown_count = 0
            self._finalize_success(task, result)
            self._update_worker(worker_id, "Active")
            if self.request_sleep:
                self._stop.wait(self.request_sleep)

        if not state.disabled:
            self._update_worker(worker_id, "Completed")

    @staticmethod
    def _normalize_error(exc: Exception) -> ProviderError:
        if isinstance(exc, ProviderError):
            return exc
        if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
            return ProviderError(
                f"Provider connection failed ({type(exc).__name__}).", retryable=True
            )
        return ProviderError(f"Provider failed ({type(exc).__name__}).", retryable=False)

    def _update_worker(self, worker_id: int, status: str) -> None:
        state = self._states[worker_id]
        self.progress.update_worker(
            worker_id, processed=state.processed, rate=state.rate, status=status
        )

    def _finalize_success(self, task: _Task, result: str) -> None:
        with self._result_lock:
            if task.done:
                return
            self.table.set(task.row_index, task.output_column, result)
            self.checkpoint.record_success(task, result)
            self._mark_done(task)

    def _finalize_failure(self, task: _Task, message: str) -> None:
        with self._result_lock:
            if task.done:
                return
            self.checkpoint.record_failure(task, message)
            self._mark_done(task)

    def _mark_done(self, task: _Task) -> None:
        task.done = True
        with self._state_lock:
            self._remaining -= 1
            remaining = self._remaining
        self.progress.advance_overall()
        if self.strategy == "balanced":
            self.progress.advance_balanced_owner(task.owner)
        if remaining == 0:
            self._stop.set()

    def _disable_worker(
        self, worker_id: int, current_task: Optional[_Task], message: str
    ) -> None:
        state = self._states[worker_id]
        state.disabled = True
        self._update_worker(worker_id, "Disabled")
        with self._state_lock:
            self._disabled_workers.add(worker_id)
            no_workers_left = len(self._disabled_workers) == len(self.apis)

        if current_task is not None:
            self._requeue(current_task)
        if self.strategy == "balanced":
            while True:
                try:
                    self._rescue_queue.put(self._balanced_queues[worker_id].get_nowait())
                except queue.Empty:
                    break

        if no_workers_left:
            self._stop.set()
            self._fail_all_queued(message)

    def _fail_all_queued(self, message: str) -> None:
        queues: Iterable["queue.Queue[_Task]"] = (
            [self._dynamic_queue]
            if self.strategy == "dynamic"
            else [self._rescue_queue, *self._balanced_queues]
        )
        for task_queue in queues:
            while True:
                try:
                    task = task_queue.get_nowait()
                except queue.Empty:
                    break
                task.failures += 1
                self._finalize_failure(task, message)


def _validate_inputs(
    *,
    targets: Mapping[str, str],
    apis: Sequence[str],
    model: str,
    base_url: str,
    source_language: str,
    mode: str,
    glossary_mode: str,
    strategy: str,
    timeout: float,
    request_sleep: float,
    max_retries: int,
    backoff_base: float,
    max_backoff: float,
    api_failure_threshold: int,
    api_cooldown: float,
    max_api_cooldown: float,
) -> None:
    if not targets:
        raise ValueError("targets must contain at least one language-to-column mapping.")
    if any(
        not str(language).strip() or not str(column).strip()
        for language, column in targets.items()
    ):
        raise ValueError("Target language codes and output column names cannot be empty.")
    if len(set(str(value) for value in targets.values())) != len(targets):
        raise ValueError("Each target language must use a distinct output column.")
    if not apis or any(not key for key in apis):
        raise ValueError("apis must contain at least one non-empty API key.")
    if not str(model).strip():
        raise ValueError("model cannot be empty.")
    if not str(base_url).strip():
        raise ValueError("base_url cannot be empty.")
    if not str(source_language).strip():
        raise ValueError("source_language cannot be empty.")
    if mode not in {"auto", "term", "sentence", "text"}:
        raise ValueError("mode must be one of: auto, term, sentence, text.")
    if glossary_mode not in {"strict", "prefer", "off"}:
        raise ValueError("glossary_mode must be one of: strict, prefer, off.")
    if strategy not in {"dynamic", "balanced"}:
        raise ValueError("strategy must be either dynamic or balanced.")
    _validate_time_option("timeout", timeout, allow_zero=False)
    _validate_time_option("sleep", request_sleep, allow_zero=True)
    _validate_time_option("backoff_base", backoff_base, allow_zero=True)
    _validate_time_option("max_backoff", max_backoff, allow_zero=True)
    _validate_integer_option("max_retries", max_retries, minimum=0)
    _validate_integer_option(
        "api_failure_threshold",
        api_failure_threshold,
        minimum=1,
    )
    _validate_time_option("api_cooldown", api_cooldown, allow_zero=True)
    _validate_time_option(
        "max_api_cooldown",
        max_api_cooldown,
        allow_zero=True,
    )


def _translate_table(
    table: Table,
    *,
    source_column: str,
    target_map: Mapping[str, str],
    api_keys: Sequence[str],
    source_language: str,
    mode: str,
    glossary: Optional[Union[Mapping[Any, Any], PathLike]],
    glossary_mode: str,
    glossary_source_column: Optional[str],
    glossary_target_columns: Optional[Mapping[str, str]],
    glossary_sheet_name: SheetName,
    strategy: str,
    provider: Optional[Union[str, TranslationProvider]],
    model: str,
    base_url: str,
    timeout: float,
    provider_options: Optional[Mapping[str, Any]],
    sleep: float,
    max_retries: int,
    backoff_base: float,
    max_backoff: float,
    api_failure_threshold: int,
    api_cooldown: float,
    max_api_cooldown: float,
    overwrite: bool,
    show_progress: bool,
    checkpoint_store: _Checkpoint,
    checkpoint_row_offset: int = 0,
    task_batch_size: Optional[int] = None,
    loaded_glossary: Optional[Glossary] = None,
) -> None:
    if source_column not in table.headers:
        raise ValueError(f"Source column does not exist: {source_column!r}")
    for output_column in target_map.values():
        table.ensure_column(output_column)

    if task_batch_size is not None and task_batch_size < 1:
        raise ValueError("batch_size must be at least 1.")

    if not overwrite:
        for uid, value in checkpoint_store.completed_items(
            checkpoint_row_offset, table.row_count
        ):
            try:
                row_text, language = uid.split(":", 1)
                row_index = int(row_text) - checkpoint_row_offset
                output_column = target_map[language]
            except (ValueError, KeyError):
                continue
            if 0 <= row_index < table.row_count and not is_filled(
                table.get(row_index, output_column)
            ):
                table.set(row_index, output_column, value)

    glossary_data = loaded_glossary
    if glossary_data is None:
        glossary_data = (
            Glossary()
            if glossary_mode == "off"
            else load_glossary(
                glossary,
                target_languages=list(target_map),
                target_output_columns=target_map,
                source_column=glossary_source_column,
                target_columns=glossary_target_columns,
                sheet_name=glossary_sheet_name,
            )
        )

    rows_per_batch = task_batch_size or max(1, table.row_count)
    for row_start in range(0, table.row_count, rows_per_batch):
        row_end = min(table.row_count, row_start + rows_per_batch)
        tasks: List[_Task] = []
        for row_index in range(row_start, row_end):
            source_value = table.get(row_index, source_column)
            if not is_filled(source_value):
                continue
            source_text = str(source_value)
            for target_language, output_column in target_map.items():
                if not overwrite and is_filled(table.get(row_index, output_column)):
                    continue
                owner = len(tasks) % len(api_keys)
                tasks.append(
                    _Task(
                        row_index=row_index,
                        target_language=target_language,
                        output_column=output_column,
                        text=source_text,
                        owner=owner,
                        checkpoint_index=checkpoint_row_offset + row_index,
                    )
                )

        scheduler = _Scheduler(
            tasks=tasks,
            table=table,
            checkpoint=checkpoint_store,
            glossary=glossary_data,
            glossary_mode=glossary_mode,
            source_language=source_language,
            mode=mode,
            apis=api_keys,
            strategy=strategy,
            provider=provider,
            model=model,
            base_url=base_url,
            timeout=timeout,
            provider_options=provider_options,
            request_sleep=sleep,
            max_retries=max_retries,
            backoff_base=backoff_base,
            max_backoff=max_backoff,
            api_failure_threshold=api_failure_threshold,
            api_cooldown=api_cooldown,
            max_api_cooldown=max_api_cooldown,
            show_progress=show_progress,
        )
        scheduler.run()


def _translate_stream_file(
    source_path: Path,
    output_path: Path,
    *,
    sheet_name: SheetName,
    source_column: str,
    target_map: Mapping[str, str],
    api_keys: Sequence[str],
    source_language: str,
    mode: str,
    glossary: Optional[Union[Mapping[Any, Any], PathLike]],
    glossary_mode: str,
    glossary_source_column: Optional[str],
    glossary_target_columns: Optional[Mapping[str, str]],
    glossary_sheet_name: SheetName,
    strategy: str,
    provider: Optional[Union[str, TranslationProvider]],
    model: str,
    base_url: str,
    timeout: float,
    provider_options: Optional[Mapping[str, Any]],
    sleep: float,
    max_retries: int,
    backoff_base: float,
    max_backoff: float,
    api_failure_threshold: int,
    api_cooldown: float,
    max_api_cooldown: float,
    checkpoint: bool,
    overwrite: bool,
    show_progress: bool,
    batch_size: int,
    identity: Mapping[str, Any],
    csv_encoding: str,
    csv_delimiter: str,
    data_only: bool,
    formula_policy: str,
) -> Path:
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1.")
    checkpoint_path = output_path.with_name(
        output_path.name + ".aicorpusx.checkpoint.sqlite3"
    )
    checkpoint_store = _SQLiteCheckpoint(
        checkpoint_path,
        enabled=checkpoint,
        identity=identity,
        resume=not overwrite,
    )
    loaded_glossary = (
        Glossary()
        if glossary_mode == "off"
        else load_glossary(
            glossary,
            target_languages=list(target_map),
            target_output_columns=target_map,
            source_column=glossary_source_column,
            target_columns=glossary_target_columns,
            sheet_name=glossary_sheet_name,
        )
    )

    headers = read_headers(
        source_path,
        sheet_name,
        csv_encoding=csv_encoding,
        csv_delimiter=csv_delimiter,
        data_only=data_only,
    )
    if source_column not in headers:
        raise ValueError(f"Source column does not exist: {source_column!r}")
    for output_column in target_map.values():
        if output_column not in headers:
            headers.append(output_column)
    source_records = iter_records(
        source_path,
        sheet_name,
        csv_encoding=csv_encoding,
        csv_delimiter=csv_delimiter,
        data_only=data_only,
    )

    def translated_records() -> Iterator[Dict[str, Any]]:
        row_offset = 0
        batch: List[Mapping[str, Any]] = []

        def translate_batch(items: List[Mapping[str, Any]], offset: int) -> List[Dict[str, Any]]:
            table = MemoryTable(items)
            _translate_table(
                table,
                source_column=source_column,
                target_map=target_map,
                api_keys=api_keys,
                source_language=source_language,
                mode=mode,
                glossary=glossary,
                glossary_mode=glossary_mode,
                glossary_source_column=glossary_source_column,
                glossary_target_columns=glossary_target_columns,
                glossary_sheet_name=glossary_sheet_name,
                strategy=strategy,
                provider=provider,
                model=model,
                base_url=base_url,
                timeout=timeout,
                provider_options=provider_options,
                sleep=sleep,
                max_retries=max_retries,
                backoff_base=backoff_base,
                max_backoff=max_backoff,
                api_failure_threshold=api_failure_threshold,
                api_cooldown=api_cooldown,
                max_api_cooldown=max_api_cooldown,
                overwrite=overwrite,
                show_progress=show_progress,
                checkpoint_store=checkpoint_store,
                checkpoint_row_offset=offset,
                loaded_glossary=loaded_glossary,
            )
            return table.records()

        for record in source_records:
            batch.append(record)
            if len(batch) >= batch_size:
                yield from translate_batch(batch, row_offset)
                row_offset += len(batch)
                batch = []
        if batch:
            yield from translate_batch(batch, row_offset)

    try:
        write_records(
            translated_records(),
            output_path,
            sheet_name=str(sheet_name) if isinstance(sheet_name, str) else "Sheet1",
            fieldnames=headers,
            csv_encoding=csv_encoding,
            csv_delimiter=csv_delimiter,
            formula_policy=formula_policy,
        )
    finally:
        checkpoint_store.close()
    return output_path


def _output_matches_source_data(
    source_path: Path,
    output_path: Path,
    *,
    sheet_name: SheetName,
    source_column: str,
    target_columns: Iterable[str],
    csv_encoding: str,
    csv_delimiter: str,
    formula_policy: str,
) -> bool:
    """Check that a resumable output still contains the current source rows."""

    if source_path == output_path:
        return True
    source_headers = read_headers(
        source_path,
        sheet_name,
        csv_encoding=csv_encoding,
        csv_delimiter=csv_delimiter,
    )
    try:
        output_headers = read_headers(
            output_path,
            sheet_name,
            csv_encoding=csv_encoding,
            csv_delimiter=csv_delimiter,
        )
    except ValueError:
        return False
    translated_columns = set(target_columns)
    source_columns = [
        name
        for name in source_headers
        if name not in translated_columns or name == source_column
    ]
    if any(name not in output_headers for name in source_columns):
        return False

    source_records = iter_records(
        source_path,
        sheet_name,
        csv_encoding=csv_encoding,
        csv_delimiter=csv_delimiter,
    )
    output_records = iter_records(
        output_path,
        sheet_name,
        csv_encoding=csv_encoding,
        csv_delimiter=csv_delimiter,
    )
    missing = object()
    with closing(source_records), closing(output_records):
        while True:
            source_record = next(source_records, missing)
            try:
                output_record = next(output_records, missing)
            except ValueError:
                return False
            if source_record is missing or output_record is missing:
                return source_record is missing and output_record is missing
            for column in source_columns:
                source_value = source_record[column]  # type: ignore[index]
                if source_path.suffix.lower() == ".csv":
                    source_value = _csv_output_value(source_value, formula_policy)
                if source_value != output_record[column]:  # type: ignore[index]
                    return False


def trans(
    input_file: PathLike,
    *,
    source_column: str,
    targets: Mapping[str, str],
    apis: Union[str, Sequence[str]],
    model: str,
    base_url: str,
    output: Optional[PathLike] = None,
    sheet_name: SheetName = 0,
    source_language: str = "auto",
    mode: str = "auto",
    glossary: Optional[Union[Mapping[Any, Any], PathLike]] = None,
    glossary_mode: str = "prefer",
    glossary_source_column: Optional[str] = None,
    glossary_target_columns: Optional[Mapping[str, str]] = None,
    glossary_sheet_name: SheetName = 0,
    strategy: str = "dynamic",
    provider: Optional[Union[str, TranslationProvider]] = None,
    provider_options: Optional[Mapping[str, Any]] = None,
    timeout: float = 60.0,
    sleep: float = 0.2,
    max_retries: int = 5,
    backoff_base: float = 1.0,
    max_backoff: float = 60.0,
    api_failure_threshold: int = 5,
    api_cooldown: float = 30.0,
    max_api_cooldown: float = 300.0,
    checkpoint: bool = True,
    checkpoint_tag: Optional[str] = None,
    overwrite: bool = False,
    show_progress: bool = True,
    batch_size: int = 500,
    memory_mode: str = "auto",
    csv_encoding: str = DEFAULT_CSV_ENCODING,
    csv_delimiter: str = DEFAULT_CSV_DELIMITER,
    data_only: bool = False,
    formula_policy: str = DEFAULT_FORMULA_POLICY,
) -> Path:
    """Translate a CSV/XLSX column and save the translated file.

    Args:
        input_file: Source ``.csv`` or ``.xlsx`` file.
        source_column: Header of the column containing source text. This is a
            header name, not an Excel column number.
        targets: Mapping of target-language identifiers to output column names,
            for example ``{"en": "English", "ar": "Arabic"}``.
        apis: One API key or a sequence of keys. Each key creates one worker.
        model: Model name understood by the selected provider.
        base_url: OpenAI-compatible API root or full ``/chat/completions`` URL.
        output: Destination file. Defaults to ``<name>_translated.<extension>``.
        sheet_name: XLSX worksheet name or zero-based worksheet index.
        source_language: Source language code/name, or ``"auto"``.
        mode: Content mode: ``"auto"``, ``"term"``, ``"sentence"``, or
            ``"text"``.
        glossary: Terminology mapping or path to a CSV/XLSX glossary.
        glossary_mode: ``"prefer"`` adds terminology guidance; ``"strict"``
            also validates required terms; ``"off"`` ignores the glossary.
        glossary_source_column: Source-term column in a glossary file.
        glossary_target_columns: Target language to glossary-column mapping.
        glossary_sheet_name: Glossary XLSX worksheet name or index.
        strategy: ``"dynamic"`` shares a queue; ``"balanced"`` initially
            divides work evenly among API keys.
        provider: Provider name or custom object implementing ``translate()``.
        provider_options: Extra request fields, such as ``temperature``.
        timeout: Per-request timeout in seconds.
        sleep: Delay in seconds after each successful request per worker.
        max_retries: Maximum retries for each translation task.
        backoff_base: Initial retry backoff in seconds.
        max_backoff: Maximum retry backoff in seconds.
        api_failure_threshold: Consecutive failures before worker cooldown.
        api_cooldown: Initial API worker cooldown in seconds.
        max_api_cooldown: Maximum API worker cooldown in seconds.
        checkpoint: Save resumable JSON/SQLite task state when true.
        checkpoint_tag: Optional caller-defined identity tag. Change it when a
            custom provider's behavior or hidden configuration changes.
        overwrite: Retranslate existing target values and reset resume state.
        show_progress: Display Rich progress bars.
        batch_size: Maximum source rows scheduled together. In stream mode this
            also bounds the number of rows retained in memory.
        memory_mode: ``"auto"``, ``"preserve"``, or ``"stream"``. Preserve
            mode retains XLSX structure; stream mode bounds memory but writes a
            value-only XLSX workbook.
        csv_encoding: Text encoding used for CSV input and output.
        csv_delimiter: One-character CSV field delimiter.
        data_only: For streamed XLSX input, translate cached formula results
            instead of formula expressions. In auto mode this selects stream
            mode because saving a data-only workbook would discard formulas.
        formula_policy: ``"escape"`` writes formula-like strings as literal
            text; ``"preserve"`` allows spreadsheet formulas.

    Returns:
        Absolute path to the completed output file.

    Raises:
        FileNotFoundError: If the input or glossary file does not exist.
        ValueError: If a path, column, mode, mapping, or numeric option is invalid.

    Note:
        Stream mode uses a SQLite checkpoint and rebuilds output through a
        temporary file. Preserve mode uses a JSON checkpoint.
    """

    source_path = Path(input_file).expanduser().resolve()
    if not source_path.is_file():
        raise FileNotFoundError(f"Input file does not exist: {source_path}")
    if source_path.suffix.lower() not in {".csv", ".xlsx"}:
        raise ValueError("Only .csv and .xlsx input files are supported.")

    output_path = (
        Path(output).expanduser().resolve()
        if output is not None
        else source_path.with_name(f"{source_path.stem}_translated{source_path.suffix}")
    )
    if output_path.suffix.lower() != source_path.suffix.lower():
        raise ValueError("Input and output files must use the same .csv or .xlsx format.")
    _validate_integer_option("batch_size", batch_size, minimum=1)
    if memory_mode not in {"auto", "preserve", "stream"}:
        raise ValueError("memory_mode must be one of: auto, preserve, stream.")
    if checkpoint_tag is not None and not isinstance(checkpoint_tag, str):
        raise TypeError("checkpoint_tag must be a string or None.")
    _validate_formula_policy(formula_policy)
    if source_path.suffix.lower() == ".csv":
        _validate_csv_options(csv_encoding, csv_delimiter)

    target_map = {str(language): str(column) for language, column in targets.items()}
    api_keys = [apis] if isinstance(apis, str) else [str(key) for key in apis]
    _validate_inputs(
        targets=target_map,
        apis=api_keys,
        model=str(model),
        base_url=str(base_url).strip(),
        source_language=str(source_language),
        mode=mode,
        glossary_mode=glossary_mode,
        strategy=strategy,
        timeout=timeout,
        request_sleep=sleep,
        max_retries=max_retries,
        backoff_base=backoff_base,
        max_backoff=max_backoff,
        api_failure_threshold=api_failure_threshold,
        api_cooldown=api_cooldown,
        max_api_cooldown=max_api_cooldown,
    )

    json_checkpoint_path = output_path.with_name(
        output_path.name + ".aicorpusx.checkpoint.json"
    )
    sqlite_checkpoint_path = output_path.with_name(
        output_path.name + ".aicorpusx.checkpoint.sqlite3"
    )
    selected_memory_mode = memory_mode
    if selected_memory_mode == "auto":
        if data_only and source_path.suffix.lower() == ".xlsx":
            selected_memory_mode = "stream"
        elif source_path.suffix.lower() == ".xlsx":
            selected_memory_mode = "preserve"
        elif sqlite_checkpoint_path.is_file():
            selected_memory_mode = "stream"
        elif json_checkpoint_path.is_file() or (output_path.is_file() and not overwrite):
            selected_memory_mode = "preserve"
        else:
            selected_memory_mode = "stream"
    if (
        data_only
        and source_path.suffix.lower() == ".xlsx"
        and selected_memory_mode == "preserve"
    ):
        raise ValueError(
            "data_only=True requires memory_mode='stream' for XLSX input."
        )

    identity = {
        "identity_version": 2,
        "source": _source_signature(source_path),
        "source_column": source_column,
        "targets": target_map,
        "sheet_name": sheet_name,
        "source_language": str(source_language),
        "mode": mode,
        "model": str(model),
        "base_url": str(base_url).strip(),
        "provider": _provider_identity(provider),
        "provider_options_sha256": _configuration_digest(provider_options),
        "glossary": (
            None if glossary_mode == "off" else _glossary_identity(glossary)
        ),
        "glossary_mode": glossary_mode,
        "glossary_source_column": glossary_source_column,
        "glossary_target_columns_sha256": _configuration_digest(
            glossary_target_columns
        ),
        "glossary_sheet_name": glossary_sheet_name,
        "csv_encoding": csv_encoding,
        "csv_delimiter": csv_delimiter,
        "data_only": data_only,
        "formula_policy": formula_policy,
        "checkpoint_tag": checkpoint_tag,
    }
    if selected_memory_mode == "stream":
        return _translate_stream_file(
            source_path,
            output_path,
            sheet_name=sheet_name,
            source_column=source_column,
            target_map=target_map,
            api_keys=api_keys,
            source_language=str(source_language),
            mode=mode,
            glossary=glossary,
            glossary_mode=glossary_mode,
            glossary_source_column=glossary_source_column,
            glossary_target_columns=glossary_target_columns,
            glossary_sheet_name=glossary_sheet_name,
            strategy=strategy,
            provider=provider,
            model=str(model),
            base_url=str(base_url).strip(),
            timeout=timeout,
            provider_options=provider_options,
            sleep=sleep,
            max_retries=max_retries,
            backoff_base=backoff_base,
            max_backoff=max_backoff,
            api_failure_threshold=api_failure_threshold,
            api_cooldown=api_cooldown,
            max_api_cooldown=max_api_cooldown,
            checkpoint=checkpoint,
            overwrite=overwrite,
            show_progress=show_progress,
            batch_size=batch_size,
            identity=identity,
            csv_encoding=csv_encoding,
            csv_delimiter=csv_delimiter,
            data_only=data_only,
            formula_policy=formula_policy,
        )

    use_existing_output = output_path.is_file() and not overwrite
    if use_existing_output:
        use_existing_output = _output_matches_source_data(
            source_path,
            output_path,
            sheet_name=sheet_name,
            source_column=source_column,
            target_columns=target_map.values(),
            csv_encoding=csv_encoding,
            csv_delimiter=csv_delimiter,
            formula_policy=formula_policy,
        )
    working_path = output_path if use_existing_output else source_path
    table = open_table(
        working_path,
        sheet_name,
        csv_encoding=csv_encoding,
        csv_delimiter=csv_delimiter,
        formula_policy=formula_policy,
    )
    checkpoint_store = _Checkpoint(
        json_checkpoint_path,
        enabled=checkpoint,
        identity=identity,
        resume=not overwrite,
    )
    try:
        _translate_table(
            table,
            source_column=source_column,
            target_map=target_map,
            api_keys=api_keys,
            source_language=str(source_language),
            mode=mode,
            glossary=glossary,
            glossary_mode=glossary_mode,
            glossary_source_column=glossary_source_column,
            glossary_target_columns=glossary_target_columns,
            glossary_sheet_name=glossary_sheet_name,
            strategy=strategy,
            provider=provider,
            model=str(model),
            base_url=str(base_url).strip(),
            timeout=timeout,
            provider_options=provider_options,
            sleep=sleep,
            max_retries=max_retries,
            backoff_base=backoff_base,
            max_backoff=max_backoff,
            api_failure_threshold=api_failure_threshold,
            api_cooldown=api_cooldown,
            max_api_cooldown=max_api_cooldown,
            overwrite=overwrite,
            show_progress=show_progress,
            checkpoint_store=checkpoint_store,
            task_batch_size=batch_size,
        )
        table.save(output_path)
    finally:
        try:
            table.close()
        finally:
            checkpoint_store.close()
    return output_path


def translate_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    source_column: str,
    targets: Mapping[str, str],
    apis: Union[str, Sequence[str]],
    model: str,
    base_url: str,
    source_language: str = "auto",
    mode: str = "auto",
    glossary: Optional[Union[Mapping[Any, Any], PathLike]] = None,
    glossary_mode: str = "prefer",
    glossary_source_column: Optional[str] = None,
    glossary_target_columns: Optional[Mapping[str, str]] = None,
    glossary_sheet_name: SheetName = 0,
    strategy: str = "dynamic",
    provider: Optional[Union[str, TranslationProvider]] = None,
    provider_options: Optional[Mapping[str, Any]] = None,
    timeout: float = 60.0,
    sleep: float = 0.2,
    max_retries: int = 5,
    backoff_base: float = 1.0,
    max_backoff: float = 60.0,
    api_failure_threshold: int = 5,
    api_cooldown: float = 30.0,
    max_api_cooldown: float = 300.0,
    overwrite: bool = False,
    show_progress: bool = True,
) -> List[Dict[str, Any]]:
    """Translate dictionary-like rows entirely in memory.

    Args:
        rows: Iterable of mappings. Rows are copied and are never mutated.
        source_column: Mapping key containing source text.
        targets: Target-language identifier to output-key mapping.
        apis: One API key or a sequence of keys.
        model: Provider model name.
        base_url: OpenAI-compatible API root or chat-completions URL.
        source_language: Source language code/name, or ``"auto"``.
        mode: ``"auto"``, ``"term"``, ``"sentence"``, or ``"text"``.
        glossary: Terminology mapping or CSV/XLSX glossary path.
        glossary_mode: ``"prefer"``, ``"strict"``, or ``"off"``.
        glossary_source_column: Source-term column in a glossary file.
        glossary_target_columns: Target language to glossary-column mapping.
        glossary_sheet_name: Glossary XLSX worksheet name or index.
        strategy: ``"dynamic"`` or ``"balanced"`` worker scheduling.
        provider: Provider name or custom object implementing ``translate()``.
        provider_options: Extra provider request fields.
        timeout: Per-request timeout in seconds.
        sleep: Delay after successful requests per worker.
        max_retries: Maximum retries for each task.
        backoff_base: Initial retry delay in seconds.
        max_backoff: Maximum retry delay in seconds.
        api_failure_threshold: Consecutive failures before cooldown.
        api_cooldown: Initial cooldown in seconds.
        max_api_cooldown: Maximum cooldown in seconds.
        overwrite: Replace existing non-empty target values when true.
        show_progress: Display Rich progress bars.

    Returns:
        New dictionaries in input order, including target output keys.

    Raises:
        ValueError: If required mappings, columns, modes, or options are invalid.

    Note:
        This in-memory API does not create a checkpoint.
    """

    target_map = {str(language): str(column) for language, column in targets.items()}
    api_keys = [apis] if isinstance(apis, str) else [str(key) for key in apis]
    _validate_inputs(
        targets=target_map,
        apis=api_keys,
        model=str(model),
        base_url=str(base_url).strip(),
        source_language=str(source_language),
        mode=mode,
        glossary_mode=glossary_mode,
        strategy=strategy,
        timeout=timeout,
        request_sleep=sleep,
        max_retries=max_retries,
        backoff_base=backoff_base,
        max_backoff=max_backoff,
        api_failure_threshold=api_failure_threshold,
        api_cooldown=api_cooldown,
        max_api_cooldown=max_api_cooldown,
    )
    table = MemoryTable(rows)
    if table.row_count == 0:
        return []
    checkpoint_store = _Checkpoint(
        Path("<memory>"), enabled=False, identity={}, resume=False
    )
    _translate_table(
        table,
        source_column=source_column,
        target_map=target_map,
        api_keys=api_keys,
        source_language=str(source_language),
        mode=mode,
        glossary=glossary,
        glossary_mode=glossary_mode,
        glossary_source_column=glossary_source_column,
        glossary_target_columns=glossary_target_columns,
        glossary_sheet_name=glossary_sheet_name,
        strategy=strategy,
        provider=provider,
        model=str(model),
        base_url=str(base_url).strip(),
        timeout=timeout,
        provider_options=provider_options,
        sleep=sleep,
        max_retries=max_retries,
        backoff_base=backoff_base,
        max_backoff=max_backoff,
        api_failure_threshold=api_failure_threshold,
        api_cooldown=api_cooldown,
        max_api_cooldown=max_api_cooldown,
        overwrite=overwrite,
        show_progress=show_progress,
        checkpoint_store=checkpoint_store,
    )
    return table.records()


def translate_rows_iter(
    rows: Iterable[Mapping[str, Any]],
    *,
    source_column: str,
    targets: Mapping[str, str],
    apis: Union[str, Sequence[str]],
    model: str,
    base_url: str,
    batch_size: int = 500,
    source_language: str = "auto",
    mode: str = "auto",
    glossary: Optional[Union[Mapping[Any, Any], PathLike]] = None,
    glossary_mode: str = "prefer",
    glossary_source_column: Optional[str] = None,
    glossary_target_columns: Optional[Mapping[str, str]] = None,
    glossary_sheet_name: SheetName = 0,
    strategy: str = "dynamic",
    provider: Optional[Union[str, TranslationProvider]] = None,
    provider_options: Optional[Mapping[str, Any]] = None,
    timeout: float = 60.0,
    sleep: float = 0.2,
    max_retries: int = 5,
    backoff_base: float = 1.0,
    max_backoff: float = 60.0,
    api_failure_threshold: int = 5,
    api_cooldown: float = 30.0,
    max_api_cooldown: float = 300.0,
    overwrite: bool = False,
    show_progress: bool = True,
) -> Iterator[Dict[str, Any]]:
    """Translate rows in bounded batches and yield them in input order.

    Args:
        rows: Iterable of dictionary-like source rows.
        source_column: Mapping key containing source text.
        targets: Target-language identifier to output-key mapping.
        apis: One API key or a sequence of keys.
        model: Provider model name.
        base_url: OpenAI-compatible API root or chat-completions URL.
        batch_size: Maximum number of source rows retained per batch.
        source_language: Source language code/name, or ``"auto"``.
        mode: ``"auto"``, ``"term"``, ``"sentence"``, or ``"text"``.
        glossary: Terminology mapping or glossary file path.
        glossary_mode: ``"prefer"``, ``"strict"``, or ``"off"``.
        glossary_source_column: Source-term column in a glossary file.
        glossary_target_columns: Target language to glossary-column mapping.
        glossary_sheet_name: Glossary worksheet name or index.
        strategy: ``"dynamic"`` or ``"balanced"`` scheduling.
        provider: Provider name or custom translation provider.
        provider_options: Extra provider request fields.
        timeout: Per-request timeout in seconds.
        sleep: Delay after successful requests per worker.
        max_retries: Maximum retries for each task.
        backoff_base: Initial retry delay in seconds.
        max_backoff: Maximum retry delay in seconds.
        api_failure_threshold: Consecutive failures before cooldown.
        api_cooldown: Initial cooldown in seconds.
        max_api_cooldown: Maximum cooldown in seconds.
        overwrite: Replace existing target values when true.
        show_progress: Display Rich progress bars for each batch.

    Yields:
        Translated row dictionaries in the original input order.

    Raises:
        ValueError: If ``batch_size`` or translation options are invalid.

    Note:
        This iterator does not create a checkpoint. Use
        ``trans(memory_mode="stream")`` for resumable file streaming.
    """

    _validate_integer_option("batch_size", batch_size, minimum=1)
    batch: List[Mapping[str, Any]] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            yield from translate_rows(
                batch,
                source_column=source_column,
                targets=targets,
                apis=apis,
                model=model,
                base_url=base_url,
                source_language=source_language,
                mode=mode,
                glossary=glossary,
                glossary_mode=glossary_mode,
                glossary_source_column=glossary_source_column,
                glossary_target_columns=glossary_target_columns,
                glossary_sheet_name=glossary_sheet_name,
                strategy=strategy,
                provider=provider,
                provider_options=provider_options,
                timeout=timeout,
                sleep=sleep,
                max_retries=max_retries,
                backoff_base=backoff_base,
                max_backoff=max_backoff,
                api_failure_threshold=api_failure_threshold,
                api_cooldown=api_cooldown,
                max_api_cooldown=max_api_cooldown,
                overwrite=overwrite,
                show_progress=show_progress,
            )
            batch.clear()
    if batch:
        yield from translate_rows(
            batch,
            source_column=source_column,
            targets=targets,
            apis=apis,
            model=model,
            base_url=base_url,
            source_language=source_language,
            mode=mode,
            glossary=glossary,
            glossary_mode=glossary_mode,
            glossary_source_column=glossary_source_column,
            glossary_target_columns=glossary_target_columns,
            glossary_sheet_name=glossary_sheet_name,
            strategy=strategy,
            provider=provider,
            provider_options=provider_options,
            timeout=timeout,
            sleep=sleep,
            max_retries=max_retries,
            backoff_base=backoff_base,
            max_backoff=max_backoff,
            api_failure_threshold=api_failure_threshold,
            api_cooldown=api_cooldown,
            max_api_cooldown=max_api_cooldown,
            overwrite=overwrite,
            show_progress=show_progress,
        )


def translate_texts(
    texts: Iterable[Any],
    *,
    target_languages: Sequence[str],
    apis: Union[str, Sequence[str]],
    model: str,
    base_url: str,
    source_language: str = "auto",
    mode: str = "auto",
    glossary: Optional[Union[Mapping[Any, Any], PathLike]] = None,
    glossary_mode: str = "prefer",
    glossary_source_column: Optional[str] = None,
    glossary_target_columns: Optional[Mapping[str, str]] = None,
    glossary_sheet_name: SheetName = 0,
    strategy: str = "dynamic",
    provider: Optional[Union[str, TranslationProvider]] = None,
    provider_options: Optional[Mapping[str, Any]] = None,
    timeout: float = 60.0,
    sleep: float = 0.2,
    max_retries: int = 5,
    backoff_base: float = 1.0,
    max_backoff: float = 60.0,
    api_failure_threshold: int = 5,
    api_cooldown: float = 30.0,
    max_api_cooldown: float = 300.0,
    show_progress: bool = True,
) -> List[Dict[str, Any]]:
    """Translate multiple plain values into one or more languages.

    Args:
        texts: Values to translate. Results preserve this input order.
        target_languages: Target language codes or names.
        apis: One API key or a sequence of keys.
        model: Provider model name.
        base_url: OpenAI-compatible API root or chat-completions URL.
        source_language: Source language code/name, or ``"auto"``.
        mode: ``"auto"``, ``"term"``, ``"sentence"``, or ``"text"``.
        glossary: Terminology mapping or glossary file path.
        glossary_mode: ``"prefer"``, ``"strict"``, or ``"off"``.
        glossary_source_column: Source-term column in a glossary file.
        glossary_target_columns: Target language to glossary-column mapping.
        glossary_sheet_name: Glossary worksheet name or index.
        strategy: ``"dynamic"`` or ``"balanced"`` scheduling.
        provider: Provider name or custom translation provider.
        provider_options: Extra provider request fields.
        timeout: Per-request timeout in seconds.
        sleep: Delay after successful requests per worker.
        max_retries: Maximum retries per task.
        backoff_base: Initial retry delay in seconds.
        max_backoff: Maximum retry delay in seconds.
        api_failure_threshold: Consecutive failures before cooldown.
        api_cooldown: Initial cooldown in seconds.
        max_api_cooldown: Maximum cooldown in seconds.
        show_progress: Display Rich progress bars.

    Returns:
        A list of ``{"source": value, "translations": {language: text}}``
        dictionaries in input order.

    Raises:
        ValueError: If target languages or translation options are invalid.
    """

    languages = [str(language) for language in target_languages]
    if not languages or any(not language.strip() for language in languages):
        raise ValueError("target_languages must contain at least one non-empty language.")
    if len(set(languages)) != len(languages):
        raise ValueError("target_languages cannot contain duplicates.")
    translated = translate_rows(
        ({"source": text} for text in texts),
        source_column="source",
        targets={language: language for language in languages},
        apis=apis,
        model=model,
        base_url=base_url,
        source_language=source_language,
        mode=mode,
        glossary=glossary,
        glossary_mode=glossary_mode,
        glossary_source_column=glossary_source_column,
        glossary_target_columns=glossary_target_columns,
        glossary_sheet_name=glossary_sheet_name,
        strategy=strategy,
        provider=provider,
        provider_options=provider_options,
        timeout=timeout,
        sleep=sleep,
        max_retries=max_retries,
        backoff_base=backoff_base,
        max_backoff=max_backoff,
        api_failure_threshold=api_failure_threshold,
        api_cooldown=api_cooldown,
        max_api_cooldown=max_api_cooldown,
        show_progress=show_progress,
    )
    return [
        {
            "source": row.get("source"),
            "translations": {language: row.get(language, "") for language in languages},
        }
        for row in translated
    ]


def translate_text(
    text: Any,
    *,
    target_language: str,
    apis: Union[str, Sequence[str]],
    model: str,
    base_url: str,
    source_language: str = "auto",
    mode: str = "auto",
    glossary: Optional[Union[Mapping[Any, Any], PathLike]] = None,
    glossary_mode: str = "prefer",
    glossary_source_column: Optional[str] = None,
    glossary_target_columns: Optional[Mapping[str, str]] = None,
    glossary_sheet_name: SheetName = 0,
    strategy: str = "dynamic",
    provider: Optional[Union[str, TranslationProvider]] = None,
    provider_options: Optional[Mapping[str, Any]] = None,
    timeout: float = 60.0,
    sleep: float = 0.2,
    max_retries: int = 5,
    backoff_base: float = 1.0,
    max_backoff: float = 60.0,
    api_failure_threshold: int = 5,
    api_cooldown: float = 30.0,
    max_api_cooldown: float = 300.0,
    show_progress: bool = True,
) -> str:
    """Translate one value and return only the translated text.

    Args:
        text: Source value to translate.
        target_language: Target language code or name.
        apis: One API key or a sequence of keys.
        model: Provider model name.
        base_url: OpenAI-compatible API root or chat-completions URL.
        source_language: Source language code/name, or ``"auto"``.
        mode: ``"auto"``, ``"term"``, ``"sentence"``, or ``"text"``.
        glossary: Terminology mapping or glossary file path.
        glossary_mode: ``"prefer"``, ``"strict"``, or ``"off"``.
        glossary_source_column: Source-term column in a glossary file.
        glossary_target_columns: Target language to glossary-column mapping.
        glossary_sheet_name: Glossary worksheet name or index.
        strategy: ``"dynamic"`` or ``"balanced"`` scheduling.
        provider: Provider name or custom translation provider.
        provider_options: Extra provider request fields.
        timeout: Per-request timeout in seconds.
        sleep: Delay after a successful request.
        max_retries: Maximum retries for the translation.
        backoff_base: Initial retry delay in seconds.
        max_backoff: Maximum retry delay in seconds.
        api_failure_threshold: Consecutive failures before cooldown.
        api_cooldown: Initial cooldown in seconds.
        max_api_cooldown: Maximum cooldown in seconds.
        show_progress: Display a Rich progress bar.

    Returns:
        The translated string, or an empty string if the task ultimately fails.

    Raises:
        ValueError: If the language or translation options are invalid.
    """

    result = translate_texts(
        [text],
        target_languages=[target_language],
        apis=apis,
        model=model,
        base_url=base_url,
        source_language=source_language,
        mode=mode,
        glossary=glossary,
        glossary_mode=glossary_mode,
        glossary_source_column=glossary_source_column,
        glossary_target_columns=glossary_target_columns,
        glossary_sheet_name=glossary_sheet_name,
        strategy=strategy,
        provider=provider,
        provider_options=provider_options,
        timeout=timeout,
        sleep=sleep,
        max_retries=max_retries,
        backoff_base=backoff_base,
        max_backoff=max_backoff,
        api_failure_threshold=api_failure_threshold,
        api_cooldown=api_cooldown,
        max_api_cooldown=max_api_cooldown,
        show_progress=show_progress,
    )
    return str(result[0]["translations"][str(target_language)])
