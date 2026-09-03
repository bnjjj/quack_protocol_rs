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
