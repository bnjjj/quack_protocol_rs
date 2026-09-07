# quack_protocol

Rust client-side SDK for DuckDB's experimental Quack remote protocol.

The `0.3.0-alpha` line supports both Quack protocol v1 and protocol v3 from
DuckDB 2.0 alpha:

- DuckDB `BinarySerializer`-compatible primitive, object, logical type, vector, and `DataChunk` codecs.
- Quack connection leases and heartbeats, prepare/query, acknowledged fetch streaming,
  disconnect, success, and error messages.
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
finish.

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
