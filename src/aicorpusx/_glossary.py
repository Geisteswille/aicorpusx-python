"""Glossary loading and longest-first local matching."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

from ._tables import SheetName, read_records


@dataclass(frozen=True)
class TermConstraint:
    source: str
    target: str
    start: int
    end: int

    def as_prompt_item(self) -> Dict[str, str]:
        return {"source": self.source, "target": self.target}


class Glossary:
    def __init__(self, mappings: Optional[Mapping[str, Mapping[str, str]]] = None) -> None:
        self._mappings: Dict[str, Dict[str, str]] = {
            str(language): {
                str(source): str(target)
                for source, target in terms.items()
                if str(source)
            }
            for language, terms in (mappings or {}).items()
        }

    def matches(self, text: str, target_language: str) -> List[TermConstraint]:
        terms = self._mappings.get(target_language, {})
        candidates: List[TermConstraint] = []
        for source, target in terms.items():
            if not target:
                continue
            start = 0
            while True:
                found = text.find(source, start)
                if found < 0:
                    break
                candidates.append(TermConstraint(source, target, found, found + len(source)))
                start = found + max(1, len(source))

        # Select longer overlapping terms first, then return them in source order.
        candidates.sort(key=lambda item: (-(item.end - item.start), item.start, item.source))
        occupied: List[Tuple[int, int]] = []
        selected: List[TermConstraint] = []
        for item in candidates:
            if any(item.start < end and item.end > start for start, end in occupied):
                continue
            occupied.append((item.start, item.end))
            selected.append(item)
        return sorted(selected, key=lambda item: (item.start, item.end))


def _from_dict(
    data: Mapping[Any, Any], target_languages: Sequence[str]
) -> Dict[str, Dict[str, str]]:
    languages = [str(item) for item in target_languages]
    result: Dict[str, Dict[str, str]] = {language: {} for language in languages}
    if not data:
        return result

    # Language-first: {"ar": {"source": "target"}, ...}
    if all(isinstance(value, Mapping) for value in data.values()) and any(
        str(key) in result for key in data
    ):
        for language, terms in data.items():
            language = str(language)
            if language not in result or not isinstance(terms, Mapping):
                continue
            for source, target in terms.items():
                if target is not None and str(source):
                    result[language][str(source)] = str(target)
        return result

    # Source-first nested: {"source": {"ar": "...", "en": "..."}}
    if all(isinstance(value, Mapping) for value in data.values()):
        for source, translations in data.items():
            for language in languages:
                target = translations.get(language)  # type: ignore[union-attr]
                if target is not None and str(source):
                    result[language][str(source)] = str(target)
        return result

    # Flat mappings are useful for a single target and are applied to every target.
    for source, target in data.items():
        if target is None or not str(source):
            continue
        for language in languages:
            result[language][str(source)] = str(target)
    return result


def load_glossary(
    glossary: Optional[Union[Mapping[Any, Any], str, Path]],
    *,
    target_languages: Sequence[str],
    target_output_columns: Mapping[str, str],
    source_column: Optional[str],
    target_columns: Optional[Mapping[str, str]],
    sheet_name: SheetName,
) -> Glossary:
    if glossary is None:
        return Glossary()
    if isinstance(glossary, Mapping):
        return Glossary(_from_dict(glossary, target_languages))

    path = Path(glossary).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Glossary file does not exist: {path}")
    headers, records = read_records(path, sheet_name)
    if not headers:
        return Glossary()
    source_name = source_column or ("source" if "source" in headers else headers[0])
    if source_name not in headers:
        raise ValueError(f"Glossary source column does not exist: {source_name!r}")

    explicit = {str(key): str(value) for key, value in (target_columns or {}).items()}
    mappings: Dict[str, Dict[str, str]] = {language: {} for language in target_languages}
    for language in target_languages:
        target_name = explicit.get(language)
        if target_name is None:
            if language in headers:
                target_name = language
            elif target_output_columns[language] in headers:
                target_name = target_output_columns[language]
        if target_name is None:
            continue
        if target_name not in headers:
            raise ValueError(
                f"Glossary target column for {language!r} does not exist: {target_name!r}"
            )
        for row in records:
            source = row.get(source_name)
            target = row.get(target_name)
            if source is None or target is None:
                continue
            source_text = str(source).strip()
            target_text = str(target).strip()
            if source_text and target_text:
                mappings[language][source_text] = target_text
    return Glossary(mappings)
