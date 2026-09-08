# quack_protocol

Rust client-side SDK for DuckDB's experimental Quack remote protocol.

The `0.3.0-alpha` line supports both Quack protocol v1 and protocol v3 from
DuckDB 2.0 alpha:

- DuckDB `BinarySerializer`-compatible primitive, object, logical type, vector, and `DataChunk` codecs.
- Quack connection leases and heartbeats, prepare/query, acknowledged fetch streaming,
  disconnect, success, and error messages.
- A session pool for running queries concurrently against one server.
- Async HTTP `POST /quack` transport using `application/duckdb`.
- URI parsing for `localhost:9494`, `quack:host:port`, bracketed IPv6, and direct HTTP(S) URLs.
- SQL literal formatting for positional and named parameters.

```rust
use futures_util::TryStreamExt;
use quack_protocol::{QuackClient, QuackClientOptions, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let client = QuackClient::connect(
        "localhost:9494",
        QuackClientOptions {
            auth_token: Some("super_secret".to_string()),
            ..Default::default()
        },
    )
    .await?;

    let (_columns, rows) = client
        .query("SELECT 42::INTEGER AS answer", None)
        .await?
        .into_rows();
    let rows: Vec<_> = rows.try_collect().await?;
    println!("{:?}", rows);

    client.disconnect().await?;
    Ok(())
}
```

## Connection model

A `QuackClient` is **one session** on the server: one connection id, for the
client's whole life and for every clone of it. Two things follow.

**Queries on one client are serialized.** The server keeps a single resumable
result cursor per connection id, and a PREPARE resets it - so a second query
would invalidate a FETCH still in flight. Queries therefore take a lock, and a
result stream holds it until the stream is drained or dropped. Cloning the
client does not change that; the clones share the session.

**Session state persists.** Temporary tables, `SET`, transactions, and attached
databases live on the connection, so consecutive calls on one client see each
other's effects.

For concurrency, use a `QuackPool`. It keeps several sessions and hands a free
one to each caller, so N queries run on the server at once - what a query engine
wants when it opens one scan per partition:

```rust
use quack_protocol::{QuackClientOptions, QuackPool, QuackPoolOptions, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let pool = QuackPool::connect(
        "localhost:9494",
        QuackClientOptions::default(),
        // Size this to the parallelism you want - DataFusion's
        // `target_partitions`, say. Every connection is a DuckDB connection on
        // the server.
        QuackPoolOptions { max_connections: 8 },
    )
    .await?;

    // Each concurrent query runs on its own session.
    let (_columns, chunks) = pool.query("SELECT 42::INTEGER AS answer", None).await?.into_chunks();
    let _ = chunks;
    Ok(())
}
```

`QuackPool` is cheap to clone and shares its connections, so one pool can back
every table in a federated catalog. Connections open on demand up to
`max_connections`; callers past the limit wait for one to come free.

The pool gives **no session affinity** between calls: two `pool.query()` calls
may land on different sessions, so a temp table created by one is invisible to
the other. Work that depends on session state takes a connection out of the pool
and holds it:

```rust,ignore
let connection = pool.acquire().await?;
connection.execute("CREATE TEMP TABLE staging AS SELECT 1 AS id", None).await?;
let ids = connection.values("SELECT id FROM staging").await?;
// `connection` goes back to the pool when it is dropped.
```

`execute()` returns the rows a DML statement touched, as DuckDB reports them,
and `None` for DDL and queries. `QuackClientOptions` redacts `auth_token` in its
`Debug` output, and the crate re-exports `HeaderMap`, `HeaderName`, and
`HeaderValue` for `QuackClientOptions::headers` so callers need not depend on
`reqwest`.

A stream returned by `pool.query()` holds its connection until it is drained or
dropped, so drop it when the results are no longer wanted rather than leaving it
parked - the connection is not free until then.

### Closing sessions

A session the client no longer holds is closed, so the server does not keep a
cursor for a connection id nobody can use again:

- `disconnect()` (on a client, `close()` on a pool) closes deterministically and
  reports failure.
- Dropping the last handle to a client - or the pool that holds it - sends the
  DISCONNECT from a background task. That is best-effort: it only goes out while
  the Tokio runtime is alive, so call `disconnect()`/`close()` when the program
  is about to exit or the outcome matters.

### When the server forgets a connection

After a server restart, a session opened against the old server is gone and the
server answers `Invalid connection id`. `QuackError::is_connection_lost()`
reports that case; it means the request was rejected before any SQL ran.

The pool handles this itself: it retires the stale connections and retries the
query once on a fresh one. Only PREPARE is retried, because once results are
streaming the statement has already run - errors from FETCH, and appends, are
passed through untouched rather than risking a repeated write. A plain
`QuackClient` does not reconnect; it is one session, and quietly opening another
would drop the session state that came with it.

## Arrow output

The optional `arrow` feature adds a read-only bridge that turns query results
into `arrow_array::RecordBatch` streams. The Arrow schema is built from the
column definitions returned at prepare time, so it is correct even for results
that carry no rows.

```toml
[dependencies]
quack_protocol = { version = "0.2", features = ["arrow"] }
```

```rust
use futures_util::TryStreamExt;
use quack_protocol::arrow::arrow_array::RecordBatch;
use quack_protocol::{QuackClient, QuackClientOptions, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let client = QuackClient::connect("localhost:9494", QuackClientOptions::default()).await?;

    let (schema, batches) = client
        .query("SELECT 42::INTEGER AS answer", None)
        .await?
        .into_record_batches()?;
    let batches: Vec<RecordBatch> = batches.try_collect().await?;
    println!("{schema}: {} rows", batches.iter().map(RecordBatch::num_rows).sum::<usize>());

    client.disconnect().await?;
    Ok(())
}
```

`into_record_batches()` is the whole entry point. `quack_protocol::arrow`
otherwise only re-exports the `arrow-array`, `arrow-buffer`, and `arrow-schema`
crates, so downstream code can match versions.

The batch stream holds the client's connection until it is drained or dropped,
as `into_chunks()` and `into_rows()` do: a `QuackClient` runs one query at a
time, and a second query on the same client waits for the first stream to
finish. `QuackPool::query` returns the same `QuackResultStream`, so
`into_record_batches()` works there too - and concurrent scans then run on
separate sessions. See [Connection model](#connection-model).

### Type mapping

| DuckDB | Arrow |
|---|---|
| `NULL` | `Null` |
| `BOOLEAN` | `Boolean` |
| `TINYINT` … `BIGINT` | `Int8` … `Int64` |
| `UTINYINT` … `UBIGINT` | `UInt8` … `UInt64` |
| `HUGEINT`, `UHUGEINT` | `Decimal256(39, 0)` |
| `FLOAT`, `DOUBLE` | `Float32`, `Float64` |
| `DECIMAL(w, s)` | `Decimal128(w, s)` |
| `VARCHAR`, `CHAR`, `ENUM`, `UUID` | `Utf8` |
| `BLOB`, `GEOMETRY`, `BIT` | `Binary` |
| `DATE` | `Date32` |
| `TIME`, `TIME_NS` | `Time64(Microsecond)`, `Time64(Nanosecond)` |
| `TIMESTAMP_S`, `TIMESTAMP_MS`, `TIMESTAMP`, `TIMESTAMP_NS` | `Timestamp(<unit>, None)` |
| `TIMESTAMPTZ` | `Timestamp(Microsecond, Some("UTC"))` |
| `INTERVAL` | `Interval(MonthDayNano)` |
| `LIST`, `MAP` | `List(child)` |
| `ARRAY(n)` | `FixedSizeList(child, n)` |
| `STRUCT` | `Struct(children)` |
| `VARIANT` | `Struct(keys, children, values, data)` |

Every field is nullable; the wire carries no non-null guarantee. A `NULL` list,
array, or struct is a null container, not an empty one. `TIME` values must lie
within one day, as Arrow requires, so DuckDB's `TIME '24:00:00'` is rejected.
`TIMETZ` and `UNION` have no Arrow mapping yet and produce
`QuackError::UnsupportedType`.

Three mappings are provisional and may change in a future release: `MAP` is
encoded as `List<Struct<key, value>>` rather than Arrow's native `Map`, `ENUM`
as plain `Utf8` rather than a `Dictionary`, and `VARIANT` as the struct DuckDB
shreds it into rather than Arrow's canonical `arrow.variant` extension, whose
binary encoding is not the one DuckDB sends. A `VARIANT` column therefore
arrives as its physical layout — `keys`, `children`, `values`, and a `data`
blob — the same shape the `Value` path already exposes, and reading a value
back out of it means interpreting that encoding yourself.

Quack is still experimental upstream and not yet covered by a stable official wire spec. This implementation follows DuckDB's `duckdb-quack` extension.

## Compatibility

The client prefers protocol v3 and retries with the separate v1 codec when an
older server rejects the v3 handshake. `QuackConnectionInfo::quack_version`
reports the selected version. Set `min_supported_quack_version` and
`max_supported_quack_version` to the same value to require one version.

Protocol v1 retains its legacy prepare, fetch, and append payloads. Protocol v3
uses query UUIDs, acknowledged fetch batches, raw chunk payloads, and connection
heartbeats. The v3 send-data write path is not implemented yet, so `append` and
`append_rows` return an explicit unsupported-protocol error on a v3 connection;
they continue to work on v1 connections.
