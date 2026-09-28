# aicorpusx

`aicorpusx` is a concurrent translation library for Python programs and CSV/XLSX corpora. It supports OpenAI-compatible `/chat/completions` services, multiple API keys, terminology constraints, retries, resumable file translation, in-memory workflows, and memory-bounded streaming pipelines.

## Features

- Translate a single string, a sequence of strings, Python dictionaries, CSV files, or XLSX workbooks.
- Continue processing translated data directly in Python without writing an intermediate file.
- Read, translate, and write large datasets in bounded batches.
- Share work dynamically across multiple API keys or split it evenly.
- Retry rate limits, server failures, timeouts, and connection errors with exponential backoff.
- Resume `trans()` jobs with JSON checkpoints in structure-preserving mode or SQLite checkpoints in streaming mode.
- Validate checkpoint identity with source/glossary content hashes and translation-affecting options.
- Apply multilingual glossaries in `prefer`, `strict`, or `off` mode.
- Treat formula-like spreadsheet values as literal text by default.
- Use any provider that implements the OpenAI-compatible chat-completions protocol, or supply a custom provider object.

## Installation

```bash
python -m pip install aicorpusx
```

From a local checkout:

```bash
python -m pip install .
```

### Editor support

The package ships inline type annotations and a PEP 561 `py.typed` marker. Editors using Pylance or Pyright can display public function signatures, parameter types, defaults, return types, and docstring guidance on hover. Restart the Python language server after upgrading if an editor still shows information from an older installed version.

## Choose an interface

| Goal | Interface | Memory behavior | Checkpoint |
| --- | --- | --- | --- |
| Translate one value | `translate_text()` | In memory | No |
| Translate a list of values | `translate_texts()` | In memory | No |
| Translate Python dictionaries | `translate_rows()` | In memory | No |
| Translate an iterable in batches | `translate_rows_iter()` | Bounded by `batch_size` | No |
| Read all file rows | `read_rows()` | In memory | No |
| Stream file rows | `read_rows_iter()` | Incremental | No |
| Write file rows | `write_rows()` / `write_rows_iter()` | Incremental | No |
| Translate a CSV/XLSX file directly | `trans()` | Configurable | JSON or SQLite checkpoint |

Use `trans()` for the simplest resumable file workflow. Use the iterator APIs for large files or when translation is one stage in a larger Python pipeline.

## API credentials

Do not publish API keys in source code or screenshots. Loading keys from environment variables is safer:

```python
import os

api_keys = [
    os.environ["TRANSLATION_API_KEY_1"],
    os.environ["TRANSLATION_API_KEY_2"],
]
```

Every example below assumes `api_keys` contains one or more valid credentials.

## Quick start: file to file

```python
from aicorpusx import trans

output_path = trans(
    "terms.xlsx",
    output="terms_translated.xlsx",  # optional
    source_column="source",
    targets={
        "ar": "Arabic",
        "en": "English",
    },
    apis=api_keys,
    model="deepseek-chat",
    base_url="https://api.deepseek.com",
    batch_size=500,
    memory_mode="auto",
)

print(output_path)
```

The keys in `targets` identify target languages. The values are output column names. CSV input produces CSV output and XLSX input produces XLSX output. If `output` is omitted, the default name is `<input_stem>_translated.<extension>`.

### Memory modes

`trans()` accepts `batch_size` and `memory_mode`:

- `memory_mode="auto"` (default): streams new CSV jobs, preserves XLSX
  structure, uses streaming for `data_only=True`, and recognizes an existing
  checkpoint so a resumed job keeps using its original mode.
- `memory_mode="preserve"`: loads the table normally, retains the existing XLSX workbook structure, and limits only the number of task objects created at once.
- `memory_mode="stream"`: reads and writes rows incrementally, limits rows and tasks to `batch_size`, and supports resumable SQLite checkpoints. Streamed XLSX output contains cell values but does not retain workbook formatting or other advanced features.

For example, explicitly enable bounded-memory, resumable translation:

```python
trans(
    "large_input.xlsx",
    output="large_output.xlsx",
    source_column="source",
    targets={"en": "English", "ar": "Arabic"},
    apis=api_keys,
    model="deepseek-chat",
    base_url="https://api.deepseek.com",
    memory_mode="stream",
    batch_size=500,
)
```

Smaller batches reduce peak memory use. Larger batches reduce scheduling overhead.

### Resume and overwrite behavior

`trans()` defaults to `checkpoint=True` and `overwrite=False`.

- Non-empty target cells in an existing output file are skipped.
- In preserve mode, an existing output is reused only when its source rows still
  match the current input; stale output data is rebuilt from the current input.
- Successful checkpoint results are restored without sending the same task to the provider again.
- Re-running a fully completed job exits immediately because there are no remaining tasks.
- `overwrite=True` translates all eligible source cells again.
- `checkpoint=False` prevents creation of either checkpoint type.

Structure-preserving mode stores a JSON checkpoint next to the output file:

```text
terms_translated.xlsx.aicorpusx.checkpoint.json
```

Streaming mode stores a disk-backed SQLite checkpoint:

```text
terms_translated.xlsx.aicorpusx.checkpoint.sqlite3
```

The SQLite checkpoint does not keep every completed task in Python memory and does not rewrite the complete checkpoint after each result. If a streaming job is interrupted, run the same `trans()` call again. It reads the source again, restores completed translations from SQLite, retries unfinished or failed tasks, and safely rebuilds the complete output through a temporary file.

Checkpoints contain source-file identity information, completed translations, and failed-task details. They never store API keys. A checkpoint may be deleted after a completed job, but doing so removes that resume record.

Checkpoint identity includes SHA-256 hashes of the source and file-based
glossary, plus the model, endpoint, language, mode, provider type, provider
options digest, glossary configuration, CSV settings, and formula policy. A
configuration change therefore starts a fresh checkpoint state instead of
silently reusing incompatible translations. For a custom provider whose hidden
configuration changes without changing its Python class, pass a different
`checkpoint_tag=` value.

## Translate text in Python

### One string

```python
from aicorpusx import translate_text

english = translate_text(
    "Hello, world!",
    target_language="de",
    apis=api_keys,
    model="deepseek-chat",
    base_url="https://api.deepseek.com",
)

print(english)
```

### Multiple strings and languages

```python
from aicorpusx import translate_texts

results = translate_texts(
    ["Hello", "Goodbye"],
    target_languages=["de", "ar"],
    apis=api_keys,
    model="deepseek-chat",
    base_url="https://api.deepseek.com",
)
```

The result keeps the input order:

```python
[
    {
        "source": "Hello",
        "translations": {
            "de": "Hallo",
            "ar": "...",
        },
    },
    # ...
]
```

## Translate structured Python data

`translate_rows()` accepts any iterable of mappings. It copies the input rows, adds target columns, and returns new dictionaries; it does not mutate the original objects.

```python
from aicorpusx import translate_rows

rows = [
    {"id": 1, "text": "Hello"},
    {"id": 2, "text": "Goodbye"},
]

translated = translate_rows(
    rows,
    source_column="text",
    targets={"de": "German", "ar": "Arabic"},
    apis=api_keys,
    model="deepseek-chat",
    base_url="https://api.deepseek.com",
)

for row in translated:
    print(row["id"], row["German"], row["Arabic"])
```

Existing non-empty target values are preserved by default. Pass `overwrite=True` to replace them.

## Read, process, translate, and write a file

Use this workflow when your program needs preprocessing or postprocessing:

```python
from aicorpusx import read_rows, translate_rows, write_rows

rows = read_rows("input.xlsx", sheet_name="Data")

for row in rows:
    row["source"] = str(row["source"]).strip().replace("\n", " ")

translated = translate_rows(
    rows,
    source_column="source",
    targets={"en": "English", "ar": "Arabic"},
    apis=api_keys,
    model="deepseek-chat",
    base_url="https://api.deepseek.com",
)

for row in translated:
    row["English"] = row["English"].strip()

write_rows(translated, "output.xlsx", sheet_name="Translations")
```

The same helpers support `.csv` and `.xlsx`. The output extension selects the output format, so a program may read XLSX and write CSV or the reverse.

Column names and row keys must be non-empty strings, including mappings passed
directly to the in-memory APIs. Duplicate or empty file headers are rejected
instead of being silently merged or discarded. File writes are atomic: data is
written to a temporary file in the destination directory and the destination is
replaced only after the complete write succeeds.

CSV encoding and delimiter can be selected explicitly:

```python
rows = read_rows(
    "input.csv",
    csv_encoding="gb18030",
    csv_delimiter=";",
)
write_rows(
    rows,
    "output.csv",
    csv_encoding="gb18030",
    csv_delimiter=";",
)
```

For XLSX input, `data_only=True` returns OpenPyXL's cached formula results
instead of formula expressions. A formula without a cached result is returned
as `None`. The option is also accepted by `trans()`; auto mode selects streaming
for data-only XLSX input, while an explicit `memory_mode="preserve"` is rejected
because saving a data-only workbook would discard its formulas.

### Spreadsheet formula safety

`write_rows()`, `write_rows_iter()`, and `trans()` default to
`formula_policy="escape"`. Strings beginning with `=`, `+`, `-`, or `@`,
including after leading whitespace, are written as literal XLSX text. CSV has
no cell type metadata, so an apostrophe is prepended to such values and headers
to prevent spreadsheet applications from evaluating them. Use
`formula_policy="preserve"` only for trusted content when intentional formulas
are required.

## Large-file streaming

For a large resumable file job, the simplest option is streaming `trans()`:

```python
from aicorpusx import trans

trans(
    "large_input.csv",
    output="large_output.csv",
    source_column="source",
    targets={"en": "English", "ar": "Arabic"},
    apis=api_keys,
    model="deepseek-chat",
    base_url="https://api.deepseek.com",
    memory_mode="stream",
    batch_size=500,
    checkpoint=True,
)
```

This path combines bounded memory with SQLite checkpoint recovery.

`read_rows()` and `translate_rows()` keep all supplied rows in memory. When custom preprocessing or postprocessing is required, compose the iterator APIs instead:

```python
from aicorpusx import read_rows_iter, translate_rows_iter, write_rows_iter

rows = read_rows_iter("large_input.csv")

cleaned_rows = (
    {**row, "source": str(row["source"]).strip()}
    for row in rows
)

translated_rows = translate_rows_iter(
    cleaned_rows,
    batch_size=500,
    source_column="source",
    targets={"en": "English", "ar": "Arabic"},
    apis=api_keys,
    model="deepseek-chat",
    base_url="https://api.deepseek.com",
)

final_rows = (
    {**row, "English": row["English"].strip()}
    for row in translated_rows
)

write_rows_iter(final_rows, "large_output.csv")
```

Only one input row stream and one translation batch are retained. A smaller `batch_size` lowers peak memory use; a larger batch reduces setup overhead and gives the scheduler more work to distribute.

Streaming output columns are taken from `columns=` when provided, otherwise from the first output row. Later rows must not introduce new columns.

Header-only input files are supported: streaming translation preserves every
input column and adds the requested output columns even when there are no data
rows.

```python
write_rows_iter(
    final_rows,
    "large_output.csv",
    columns=["id", "source", "English", "Arabic"],
)
```

`write_rows()` and `write_rows_iter()` have the same incremental memory
behavior. The iterator-named form is provided to make streaming pipelines more
explicit; it is otherwise an alias of `write_rows()`.

### XLSX streaming limitations

Streaming XLSX input uses OpenPyXL read-only mode and streaming output uses write-only mode. This keeps memory bounded but transfers cell values only; it does not preserve the original workbook's styling, charts, formulas, merged cells, or macros. Use `trans(memory_mode="preserve")` when retaining the original XLSX workbook structure is more important than minimizing memory.

The standalone iterator pipeline does not create a checkpoint because the library does not control how its yielded rows are consumed. Use `trans(memory_mode="stream")` when both bounded memory and built-in recovery are required.

## Languages

There is no fixed language whitelist. Target identifiers are passed to the selected model, so actual language coverage depends on that model or provider. ISO 639-1 codes are recommended because they are short and consistent; full names such as `"Japanese"` also work when understood by the model.

Common codes:

| Code | Language | Code | Language |
| --- | --- | --- | --- |
| `zh` | Chinese | `en` | English |
| `ja` | Japanese | `ko` | Korean |
| `ar` | Arabic | `de` | German |
| `fr` | French | `es` | Spanish |
| `ru` | Russian | `pt` | Portuguese |
| `it` | Italian | `tr` | Turkish |
| `vi` | Vietnamese | `th` | Thai |
| `id` | Indonesian | `ms` | Malay |
| `hi` | Hindi | `fa` | Persian |
| `nl` | Dutch | `pl` | Polish |

Use the same identifiers consistently in `targets`, glossary columns, and `glossary_target_columns`.

## Source language and content mode

The default source language is `"auto"`. Set it explicitly when useful:

```python
translated = translate_rows(
    rows,
    source_column="source",
    source_language="zh",
    targets={"en": "English"},
    mode="sentence",
    apis=api_keys,
    model="deepseek-chat",
    base_url="https://api.deepseek.com",
)
```

Supported content modes are:

- `auto`: infer `term`, `sentence`, or `text` from each source value.
- `term`: short names, labels, and terminology.
- `sentence`: individual sentences or short passages.
- `text`: longer, possibly multiline content.

## Glossaries

### Dictionary glossary

For one target language, a flat mapping is sufficient:

```python
translated = translate_rows(
    rows,
    source_column="source",
    targets={"fr": "French"},
    glossary={
        "artificial intelligence": "intelligence artificielle",
        "machine learning": "apprentissage automatique",
    },
    glossary_mode="strict",
    apis=api_keys,
    model="deepseek-chat",
    base_url="https://api.deepseek.com",
)
```

For multiple target languages, use language-first mappings:

```python
glossary = {
    "en": {"source term": "approved English term"},
    "de": {"source term": "approved German term"},
}
```

### Glossary file

A multilingual glossary file may have columns such as `source,en,de,ar`:

```python
trans(
    "corpus.xlsx",
    source_column="source",
    targets={"en": "English", "de": "German"},
    glossary="glossary.xlsx",
    apis=api_keys,
    model="deepseek-chat",
    base_url="https://api.deepseek.com",
)
```

Map nonstandard column names explicitly:

```python
glossary_source_column="original term"
glossary_target_columns={"de": "approved German term"}
```

Glossary modes:

- `prefer`: include matched terminology in the prompt.
- `strict`: also reject and retry translations missing required target terms.
- `off`: ignore the glossary.

Terms are matched inside source text, with longer overlapping terms taking priority.

## Multiple API keys and resilience

When passing a custom provider object together with multiple API keys, the same
object may be called concurrently by several worker threads. Custom providers
must therefore be thread-safe. Provider construction and unexpected internal
worker errors are propagated to the caller instead of being converted into
silent empty translations.

- `strategy="dynamic"` is the default. Every live API worker pulls from a shared queue, so faster keys may process more tasks.
- `strategy="balanced"` splits work evenly at the start. Work assigned to a disabled key is handed to remaining workers.
- One source row translated into two languages creates two tasks. For example, six rows and two languages produce twelve tasks total, regardless of the number of API keys.
- HTTP 429, HTTP 5xx, timeouts, and connection failures use exponential backoff with jitter.
- HTTP 401 and 403 disable the affected credential.
- API keys are used only in request headers and are never written to progress output or checkpoints.

Main resilience options and defaults:

```python
trans(
    "corpus.csv",
    source_column="source",
    targets={"en": "English"},
    apis=api_keys,
    model="deepseek-chat",
    base_url="https://api.deepseek.com",
    sleep=0.2,
    max_retries=5,
    backoff_base=1,
    max_backoff=60,
    api_failure_threshold=5,
    api_cooldown=30,
    max_api_cooldown=300,
)
```

Time-based options must be finite numbers within the platform timeout limit.
Retry counts, failure thresholds, and batch sizes must be integers.

## Endpoint handling

Set `base_url` to an API root such as:

```text
https://api.deepseek.com
https://api.openai.com/v1
```

The library appends `/chat/completions`. A URL already ending in `/chat/completions` is used unchanged. Extra request fields can be supplied through `provider_options`:

```python
provider_options={"temperature": 0.1}
```

For a service that does not implement the compatible endpoint, pass an object with a `translate(...)` method as `provider`.

## Public API summary

```python
from aicorpusx import (
    read_rows,
    read_rows_iter,
    trans,
    translate_rows,
    translate_rows_iter,
    translate_text,
    translate_texts,
    write_rows,
    write_rows_iter,
)
```

- `trans`: resumable CSV/XLSX translation to another file.
- `translate_text`: translate one value and return a string.
- `translate_texts`: translate multiple values and return structured results.
- `translate_rows`: translate mappings in memory and return copied mappings.
- `translate_rows_iter`: translate mappings in bounded batches and yield results.
- `read_rows`: load a CSV/XLSX file into a list of dictionaries.
- `read_rows_iter`: stream dictionaries from CSV/XLSX.
- `write_rows`: write dictionaries to CSV/XLSX.
- `write_rows_iter`: incrementally consume and write dictionaries.
