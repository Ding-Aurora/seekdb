# Local file SQL

Local CSV, newline-delimited JSON and flat Parquet files can be queried by the
native seekdb resolver, optimizer and SQL operators. Apache Parquet is used only
to decode the file format; no external SQL engine is used.

The feature is disabled by default. Set `enable_file_sql=ON` in the session and
configure `secure_file_priv` through a local Unix socket connection. Both reads
and exports must remain inside that directory. Empty or NULL roots deny access.
Canonical paths, opened file identities and symlinks are checked.

```sql
SET SESSION enable_file_sql=ON;
SELECT category, COUNT(*), AVG(score)
FROM read_csv('/allowed/data.csv', header=true, sample_size=-1) c
GROUP BY category HAVING COUNT(*) > 1 ORDER BY category;

SELECT c.id, j.note
FROM read_csv('/allowed/data.csv') c
LEFT JOIN read_json('/allowed/data.jsonl', format='newline_delimited') j
ON c.id=j.id;

SELECT SUM(c1) FROM read_parquet('/allowed/data.parquet');
DESCRIBE SELECT * FROM read_csv('/allowed/data.csv');
SELECT file FROM glob('/allowed/*.csv') ORDER BY file;

COPY (SELECT id, note FROM read_csv('/allowed/data.csv') ORDER BY id)
TO '/allowed/export.csv' (FORMAT CSV, HEADER, NULLSTR 'NULL');
COPY existing_table TO '/allowed/table.csv' (HEADER, DELIMITER ';');
```

`read_ndjson` aliases `read_json`. File paths and options are constant literals.
CSV accepts `header`, `delim` (aliases `delimiter`, `sep`), `quote`, `escape`, and
`nullstr`. CSV input assumes a header by default; COPY writes a header only when
requested. Quote and escape default to the double quote character. Doubled
quotes and a distinct escape character are supported. Quoted NULL text remains
a string; unquoted NULL text becomes SQL NULL. Duplicate CSV headers receive
case-insensitive `__2`, `__3`, etc. suffixes; empty names become `column1`, etc.

CSV and JSONL schema inference reads the complete file with bounded record
buffers, then rewinds the reader. `sample_size` supports only `-1`. Types are
BIGINT, DOUBLE, BOOLEAN, VARCHAR, DATE, DATETIME and NULL. Integers widen to
DOUBLE when mixed with floating values; DATE and DATETIME widen to DATETIME;
other conflicts become VARCHAR. Out-of-range integers remain text. JSONL uses
the union of all fields, supplies NULL for missing fields, and rejects nested
objects and arrays. Numeric JSON strings remain strings. Parquet schema comes
from the footer and projected columns are decoded across row groups.

Rows flow into native filter, sort, aggregate and join operators. File scans
support row execution, bounded batches in vectorized plans and rescan. File
queries bypass the plan cache because external schemas and identities can change.
Identity/size/modification checks reject files changed between inference and
execution or during a scan. Input files are opened read-only.

Limits: 1024 columns, 16 MiB per text record/value, 10000 discovered files, 8 MiB
Parquet footer and 128 MiB uncompressed per Parquet row group. Nested/repeated
Parquet structures and unsupported logical types are rejected. Remote files,
automatic multi-file unions and distributed file scans are outside this scope.

COPY supports CSV `format`, `header`, `delim`/`delimiter`, `quote`, `escape`, and
`nullstr`. It streams native query results to an exclusive temporary file in the
destination directory, then publishes with an atomic, non-replacing hard link.
Existing targets (including symlinks) fail. Failure or cancellation removes the
temporary file. The destination filesystem must support hard links. COPY uses
the normal FILE privilege check.

## Validation

Build with the repository's pinned Rust toolchain and normal release build.
Rust tests cover parsers, types, Snappy/dictionary Parquet, multiple row groups,
projection, rescan, file mutation, containment, COPY rollback and publication.

```sh
cd rust && cargo test -p local file_sql && cd ..
bash build.sh release
make -C build_release -j4 seekdb
python3 unittest/sql/engine/basic/test_file_sql_integration.py \
  --socket /path/to/base/run/sql.sock
python3 unittest/sql/engine/basic/run_file_sql_public_test.py \
  --socket /path/to/base/run/sql.sock --logdir /tmp/file-sql-public-test
```

The integration scripts run on the database host and require `pymysql`. The
public runner uses the original `file_sql_basic.test` SQL and expected result;
it replaces only the cluster-specific fixture deployment/cleanup includes with
local setup, restores `secure_file_priv`, and records source checksums.
