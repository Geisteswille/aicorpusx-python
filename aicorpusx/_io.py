"""Public CSV/XLSX record helpers."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Union

from ._tables import (
    DEFAULT_CSV_DELIMITER,
    DEFAULT_CSV_ENCODING,
    DEFAULT_FORMULA_POLICY,
    SheetName,
    iter_records,
    write_records,
)


PathLike = Union[str, os.PathLike[str]]


def _input_path(value: PathLike) -> Path:
    path = Path(value).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Input file does not exist: {path}")
    if path.suffix.lower() not in {".csv", ".xlsx"}:
        raise ValueError("Only .csv and .xlsx files are supported.")
    return path


def _output_path(value: PathLike) -> Path:
    path = Path(value).expanduser().resolve()
    if path.suffix.lower() not in {".csv", ".xlsx"}:
        raise ValueError("Only .csv and .xlsx files are supported.")
    return path


def read_rows(
    input_file: PathLike,
    *,
    sheet_name: SheetName = 0,
    csv_encoding: str = DEFAULT_CSV_ENCODING,
    csv_delimiter: str = DEFAULT_CSV_DELIMITER,
    data_only: bool = False,
) -> List[Dict[str, Any]]:
    """Read every CSV/XLSX data row into memory.

    Args:
        input_file: Existing ``.csv`` or ``.xlsx`` file.
        sheet_name: XLSX worksheet name or zero-based worksheet index. Ignored
            for CSV input.
        csv_encoding: Text encoding used for CSV input.
        csv_delimiter: One-character CSV field delimiter.
        data_only: For XLSX input, return cached formula results instead of
            formula expressions when true.

    Returns:
        A list of dictionaries keyed by column header. CSV values are strings;
        XLSX values retain the scalar types returned by OpenPyXL.

    Raises:
        FileNotFoundError: If ``input_file`` does not exist.
        ValueError: If the format, worksheet, or headers are invalid.

    Note:
        The complete table is retained in memory. Use ``read_rows_iter()`` for
        memory-bounded reading.
    """

    return list(
        iter_records(
            _input_path(input_file),
            sheet_name,
            csv_encoding=csv_encoding,
            csv_delimiter=csv_delimiter,
            data_only=data_only,
        )
    )


def read_rows_iter(
    input_file: PathLike,
    *,
    sheet_name: SheetName = 0,
    csv_encoding: str = DEFAULT_CSV_ENCODING,
    csv_delimiter: str = DEFAULT_CSV_DELIMITER,
    data_only: bool = False,
) -> Iterator[Dict[str, Any]]:
    """Yield CSV/XLSX rows incrementally.

    Args:
        input_file: Existing ``.csv`` or ``.xlsx`` file.
        sheet_name: XLSX worksheet name or zero-based worksheet index.
        csv_encoding: Text encoding used for CSV input.
        csv_delimiter: One-character CSV field delimiter.
        data_only: For XLSX input, return cached formula results instead of
            formula expressions when true.

    Yields:
        One dictionary per data row, in file order.

    Raises:
        FileNotFoundError: If ``input_file`` does not exist.
        ValueError: If the format, worksheet, or headers are invalid.

    Note:
        XLSX input uses OpenPyXL read-only mode and yields cell values only.
    """

    yield from iter_records(
        _input_path(input_file),
        sheet_name,
        csv_encoding=csv_encoding,
        csv_delimiter=csv_delimiter,
        data_only=data_only,
    )


def write_rows(
    rows: Iterable[Mapping[str, Any]],
    output_file: PathLike,
    *,
    sheet_name: str = "Sheet1",
    columns: Optional[Sequence[str]] = None,
    csv_encoding: str = DEFAULT_CSV_ENCODING,
    csv_delimiter: str = DEFAULT_CSV_DELIMITER,
    formula_policy: str = DEFAULT_FORMULA_POLICY,
) -> Path:
    """Write dictionary-like rows to CSV or XLSX.

    Args:
        rows: Iterable of mappings to write.
        output_file: Destination ending in ``.csv`` or ``.xlsx``.
        sheet_name: Worksheet name for XLSX output.
        columns: Ordered output columns. When omitted, keys from the first row
            are used. Required when ``rows`` is empty.
        csv_encoding: Text encoding used for CSV output.
        csv_delimiter: One-character CSV field delimiter.
        formula_policy: ``"escape"`` writes formula-like strings as literal
            text; ``"preserve"`` allows spreadsheet formulas.

    Returns:
        Absolute path to the written output file.

    Raises:
        TypeError: If a column name or row key is not a string.
        ValueError: If the format or schema is invalid, an empty iterable has
            no explicit columns, or any row contains an unknown column.

    Note:
        XLSX output is value-only and does not preserve source workbook styles,
        formulas, charts, merged cells, or macros.
    """

    return write_records(
        rows,
        _output_path(output_file),
        sheet_name=sheet_name,
        fieldnames=columns,
        csv_encoding=csv_encoding,
        csv_delimiter=csv_delimiter,
        formula_policy=formula_policy,
    )


def write_rows_iter(
    rows: Iterable[Mapping[str, Any]],
    output_file: PathLike,
    *,
    sheet_name: str = "Sheet1",
    columns: Optional[Sequence[str]] = None,
    csv_encoding: str = DEFAULT_CSV_ENCODING,
    csv_delimiter: str = DEFAULT_CSV_DELIMITER,
    formula_policy: str = DEFAULT_FORMULA_POLICY,
) -> Path:
    """Incrementally consume and write dictionary-like rows.

    Args:
        rows: Iterable or generator of mappings.
        output_file: Destination ending in ``.csv`` or ``.xlsx``.
        sheet_name: Worksheet name for XLSX output.
        columns: Ordered output columns. When omitted, keys from the first row
            are used. Required when ``rows`` is empty.
        csv_encoding: Text encoding used for CSV output.
        csv_delimiter: One-character CSV field delimiter.
        formula_policy: ``"escape"`` writes formula-like strings as literal
            text; ``"preserve"`` allows spreadsheet formulas.

    Returns:
        Absolute path to the written output file.

    Raises:
        TypeError: If a column name or row key is not a string.
        ValueError: If the output format or row columns are invalid.

    Note:
        This is the streaming-oriented alias of ``write_rows()``; both consume
        their input incrementally.
    """

    return write_rows(
        rows,
        output_file,
        sheet_name=sheet_name,
        columns=columns,
        csv_encoding=csv_encoding,
        csv_delimiter=csv_delimiter,
        formula_policy=formula_policy,
    )
