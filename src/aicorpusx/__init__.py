"""Public package interface for AI-Corpus X."""

from ._core import (
    trans,
    translate_rows,
    translate_rows_iter,
    translate_text,
    translate_texts,
)
from ._io import read_rows, read_rows_iter, write_rows, write_rows_iter

__all__ = [
    "read_rows",
    "read_rows_iter",
    "trans",
    "translate_rows",
    "translate_rows_iter",
    "translate_text",
    "translate_texts",
    "write_rows",
    "write_rows_iter",
]
__version__ = "0.1.7"
