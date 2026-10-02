# Golden wire fixtures

Messages captured from DuckDB 2.0 (`v2.0.0-alpha43586`, quack extension
`974927a394`), protocol v3:

- `connect`, `q1`..`q6`, `disconnect`: a client session against `quack_serve`,
  after `setup.sql`. `q1` holds every common type with NULLs, `q4` is an
  ERROR_RESPONSE with an exception type and extra info, `q6` is 3000 rows in two
  chunks.
- `attach-*`: DuckDB's own `ATTACH` talking to `quack_serve` (catalog queries,
  `BEGIN TRANSACTION`, a typed result).

`tests/golden.rs` decodes every message and requires the encoder to write the same
bytes back.
