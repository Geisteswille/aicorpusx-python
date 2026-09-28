"""Rich progress rendering without exposing credentials."""

from __future__ import annotations

from typing import Dict, Optional


class TranslationProgress:
    def __init__(self, *, total: int, api_totals: Dict[int, Optional[int]], enabled: bool) -> None:
        self.enabled = enabled
        self._progress = None
        self._overall = None
        self._api_tasks: Dict[int, int] = {}
        if not enabled:
            return
        try:
            from rich.progress import (
                BarColumn,
                Progress,
                SpinnerColumn,
                TaskProgressColumn,
                TextColumn,
                TimeElapsedColumn,
            )
        except ImportError:
            self.enabled = False
            return

        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("{task.description:<9}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("{task.fields[details]}"),
            TimeElapsedColumn(),
        )
        self._overall = self._progress.add_task("Overall", total=total, details="")
        for worker_id, worker_total in api_totals.items():
            self._api_tasks[worker_id] = self._progress.add_task(
                f"API {worker_id + 1}",
                total=worker_total,
                details="0 processed | 0.00/s | Active",
            )

    def __enter__(self) -> "TranslationProgress":
        if self.enabled and self._progress is not None:
            self._progress.start()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self.enabled and self._progress is not None:
            self._progress.stop()

    def advance_overall(self, amount: int = 1) -> None:
        if self.enabled and self._progress is not None and self._overall is not None:
            self._progress.advance(self._overall, amount)

    def advance_balanced_owner(self, worker_id: int, amount: int = 1) -> None:
        if self.enabled and self._progress is not None:
            self._progress.advance(self._api_tasks[worker_id], amount)

    def update_worker(self, worker_id: int, *, processed: int, rate: float, status: str) -> None:
        if self.enabled and self._progress is not None:
            self._progress.update(
                self._api_tasks[worker_id],
                details=f"{processed} processed | {rate:.2f}/s | {status}",
            )

