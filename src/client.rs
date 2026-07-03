use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_stream::try_stream;
use futures_core::Stream;
use futures_util::TryStreamExt;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue};

use crate::LogicalType;
use crate::builders::{ColumnDefinition, data_chunk_from_rows};
use crate::constants::{DEFAULT_QUACK_PORT, DUCKDB_MIME_TYPE, QUACK_ENDPOINT, QUACK_VERSION};
use crate::errors::{QuackError, Result};
use crate::messages::{MessageHeader, MessageType, QuackMessage, decode_message, encode_message};
use crate::sql::{SqlParameters, format_sql};
use crate::vector::{DataChunk, Row, Value, chunks_to_rows};

const DEFAULT_QUACK_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_QUACK_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ParsedQuackUri {
    pub(crate) base_url: String,
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) ssl: bool,
}

#[derive(Clone, Debug)]
pub struct QuackClientOptions {
    pub auth_token: Option<String>,
    pub client_duckdb_version: Option<String>,
    pub client_platform: Option<String>,
    pub min_supported_quack_version: Option<u64>,
    pub max_supported_quack_version: Option<u64>,
    pub ssl: Option<bool>,
    pub timeout: Option<Duration>,
    pub headers: HeaderMap,
}

impl Default for QuackClientOptions {
    fn default() -> Self {
        Self {
            auth_token: None,
            client_duckdb_version: None,
            client_platform: None,
            min_supported_quack_version: None,
            max_supported_quack_version: None,
            ssl: None,
            timeout: Some(DEFAULT_QUACK_REQUEST_TIMEOUT),
            headers: HeaderMap::new(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QuackConnectionInfo {
    pub server_duckdb_version: Option<String>,
    pub server_platform: Option<String>,
    pub quack_version: Option<u64>,
}

/// Caller-supplied metadata
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryMetadata {
    pub query_id: Option<String>,
}

/// One column of a query result: its name zipped with its logical type.
#[derive(Clone, Debug, PartialEq)]
pub struct QuackResultColumn {
    pub name: String,
    pub logical_type: LogicalType,
}

/// Stream of query result chunks returned by [`QuackClient::query`].
///
/// Each item is one [`DataChunk`]. No network I/O happens until the stream
/// is first polled: the PREPARE round-trip (which executes the query
/// server-side) runs on the first poll, and further chunks are fetched
/// lazily, one FETCH round-trip at a time; all chunks delivered by the same
/// round-trip are yielded across consecutive polls without extra I/O.
///
/// The result schema is exposed via [`columns`](Self::columns) — populated
/// once PREPARE completes, so guaranteed available after the first chunk (or
/// end of stream) has been observed. An empty result yields no items; poll
/// the stream to completion and read `columns()` for the schema. Chunks
/// carry their column names, so [`rows_from_chunk`](crate::rows_from_chunk)
/// decodes them directly.
///
/// Errors — including SQL errors surfaced by PREPARE — are yielded once as
/// `Err`, after which the stream is terminated and yields `None` forever.
#[must_use = "QuackResultStream is lazy: the query does not execute until the stream is polled"]
pub struct QuackResultStream {
    /// Shared with the generator backing `inner`, which fills it exactly once
    /// when the PREPARE response arrives.
    columns: Arc<OnceLock<Vec<QuackResultColumn>>>,
    inner: Pin<Box<dyn Stream<Item = Result<DataChunk>> + Send>>,
}

impl QuackResultStream {
    /// Builds the stream for one query. Constructing the generator here —
    /// the only place `columns` can be paired with the future that fills it —
    /// keeps the two from ever being wired up inconsistently.
    fn new(client: QuackClient, sql: String, query_id: String) -> Self {
        let columns = Arc::new(OnceLock::new());
        let columns_cell = Arc::clone(&columns);
        let inner = try_stream! {
            let query_started = Instant::now();
            let prepare = client.prepare(&sql).await?;
            let (result_types, result_names, mut needs_more_fetch, mut chunks, result_uuid) =
                match prepare {
                    QuackMessage::PrepareResponse {
                        result_types,
                        result_names,
                        needs_more_fetch,
                        results,
                        result_uuid,
                        ..
                    } => (
                        result_types,
                        result_names,
                        needs_more_fetch,
                        results,
                        result_uuid,
                    ),
                    other => Err(QuackError::protocol(format!(
                        "expected PREPARE_RESPONSE, got {:?}",
                        other.message_type()
                    )))?,
                };

            let mut total_rows: usize = chunks.iter().map(|chunk| chunk.row_count).sum();
            tracing::debug!(
                query_id = %query_id,
                %result_uuid,
                rows = total_rows,
                elapsed_ms = query_started.elapsed().as_millis() as u64,
                "quack PREPARE completed"
            );

            attach_column_names(&mut chunks, &result_names);
            let _ = columns_cell.set(
                result_names
                    .iter()
                    .zip(result_types)
                    .map(|(name, logical_type)| QuackResultColumn {
                        name: name.clone(),
                        logical_type,
                    })
                    .collect(),
            );

            for chunk in chunks {
                yield chunk;
            }
            while needs_more_fetch {
                let fetch_started = Instant::now();
                match client.fetch_result(result_uuid).await? {
                    QuackMessage::FetchResponse { mut results, .. } => {
                        let rows: usize = results.iter().map(|chunk| chunk.row_count).sum();
                        total_rows += rows;
                        tracing::debug!(
                            query_id = %query_id,
                            %result_uuid,
                            rows,
                            elapsed_ms = fetch_started.elapsed().as_millis() as u64,
                            "quack FETCH completed"
                        );
                        if results.is_empty() {
                            needs_more_fetch = false;
                        } else {
                            attach_column_names(&mut results, &result_names);
                            for chunk in results {
                                yield chunk;
                            }
                        }
                    }
                    other => Err(QuackError::protocol(format!(
                        "expected FETCH_RESPONSE, got {:?}",
                        other.message_type()
                    )))?,
                }
            }
            tracing::debug!(
                query_id = %query_id,
                %result_uuid,
                rows = total_rows,
                elapsed_ms = query_started.elapsed().as_millis() as u64,
                "quack query completed"
            );
        };
        Self {
            columns,
            inner: Box::pin(inner),
        }
    }

    /// Result schema. Empty until the PREPARE round-trip has completed on
    /// first poll; guaranteed populated once the stream has yielded a chunk
    /// or ended without error.
    pub fn columns(&self) -> &[QuackResultColumn] {
        self.columns.get().map(Vec::as_slice).unwrap_or(&[])
    }
}

impl Stream for QuackResultStream {
    type Item = Result<DataChunk>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.as_mut().poll_next(cx)
    }
}

#[derive(Clone, Debug)]
pub struct QuackClient {
    pub(crate) base_url: String,
    pub info: Option<QuackConnectionInfo>,
    http: reqwest::Client,
    headers: HeaderMap,
    timeout: Duration,
    connection_id: Option<String>,
    closed: bool,
    query_counter: Arc<AtomicU64>,
}

impl QuackClient {
    pub async fn connect(uri: &str, options: QuackClientOptions) -> Result<Self> {
        let parsed = parse_quack_uri(uri, options.ssl)?;
        let timeout = options.timeout.unwrap_or(DEFAULT_QUACK_REQUEST_TIMEOUT);
        let http = reqwest::Client::builder()
            .connect_timeout(DEFAULT_QUACK_CONNECT_TIMEOUT.min(timeout))
            .timeout(timeout)
            .build()?;
        let mut client = Self {
            base_url: parsed.base_url.trim_end_matches('/').to_string(),
            info: None,
            http,
            headers: options.headers.clone(),
            timeout,
            connection_id: None,
            closed: false,
            query_counter: Arc::new(AtomicU64::new(1)),
        };
        let response = client
            .send(&QuackMessage::ConnectionRequest {
                header: MessageHeader::new(MessageType::ConnectionRequest),
                auth_string: options.auth_token,
                client_duckdb_version: options.client_duckdb_version,
                client_platform: Some(
                    options
                        .client_platform
                        .unwrap_or_else(|| "quack-rust".to_string()),
                ),
                min_supported_quack_version: options
                    .min_supported_quack_version
                    .unwrap_or(QUACK_VERSION),
                max_supported_quack_version: options
                    .max_supported_quack_version
                    .unwrap_or(QUACK_VERSION),
            })
            .await?;

        match response {
            QuackMessage::ConnectionResponse {
                header,
                server_duckdb_version,
                server_platform,
                quack_version,
            } => {
                let connection_id = header.connection_id.ok_or_else(|| {
                    QuackError::protocol("CONNECTION_RESPONSE did not include a connection id")
                })?;
                client.connection_id = Some(connection_id);
                client.info = Some(QuackConnectionInfo {
                    server_duckdb_version,
                    server_platform,
                    quack_version,
                });
                Ok(client)
            }
            other => Err(QuackError::protocol(format!(
                "expected CONNECTION_RESPONSE, got {:?}",
                other.message_type()
            ))),
        }
    }

    pub fn is_connected(&self) -> bool {
        self.connection_id.is_some() && !self.closed
    }

    /// Executes `sql` lazily, returning a stream of result chunks.
    ///
    /// No network I/O happens until the returned [`QuackResultStream`] is
    /// first polled: the PREPARE round-trip (which executes the query
    /// server-side and yields the first chunks) runs on the first poll, and
    /// remaining chunks are fetched lazily, one round-trip at a time. This
    /// also means errors — including SQL errors — surface as stream items,
    /// not from awaiting `query` itself, and that a stream dropped without
    /// being polled never executes the query.
    ///
    /// Dropping the stream at any point cancels the query client-side: any
    /// in-flight request is aborted, no further FETCH requests are issued,
    /// and the client remains fully usable. The protocol has no cancellation
    /// message, so once PREPARE has run the server retains the result set
    /// until the connection is closed via [`QuackClient::disconnect`].
    ///
    /// The configured request timeout applies to each round-trip
    /// individually; the lifetime of the stream as a whole is unbounded.
    pub async fn query(
        &self,
        sql: &str,
        metadata: Option<&QueryMetadata>,
    ) -> Result<QuackResultStream> {
        self.query_inner(sql, None, metadata)
    }

    pub async fn query_with_params(
        &self,
        sql: &str,
        params: Option<&SqlParameters>,
    ) -> Result<QuackResultStream> {
        self.query_inner(sql, params, None)
    }

    fn query_inner(
        &self,
        sql: &str,
        params: Option<&SqlParameters>,
        metadata: Option<&QueryMetadata>,
    ) -> Result<QuackResultStream> {
        self.ensure_open()?;
        let query_id = metadata
            .and_then(|metadata| metadata.query_id.as_deref())
            .unwrap_or("-")
            .to_string();
        let sql = format_sql(sql, params)?;
        Ok(QuackResultStream::new(self.clone(), sql, query_id))
    }

    pub async fn first(&self, sql: &str) -> Result<Option<Row>> {
        let (_, rows) = drain_rows(self.query(sql, None).await?).await?;
        Ok(rows.into_iter().next())
    }

    pub async fn one(&self, sql: &str) -> Result<Row> {
        let (_, rows) = drain_rows(self.query(sql, None).await?).await?;
        if rows.len() != 1 {
            return Err(QuackError::protocol(format!(
                "expected exactly one row, got {}",
                rows.len()
            )));
        }
        Ok(rows.into_iter().next().expect("one row"))
    }

    pub async fn values(&self, sql: &str) -> Result<Vec<Value>> {
        let (names, rows) = drain_rows(self.query(sql, None).await?).await?;
        let first_name = match names.first() {
            Some(name) => name.as_str(),
            None => return Ok(Vec::new()),
        };
        Ok(rows
            .into_iter()
            .map(|mut row| row.shift_remove(first_name).unwrap_or(Value::Null))
            .collect())
    }

    pub async fn append(
        &self,
        table_name: impl Into<String>,
        schema_name: Option<String>,
        chunk: DataChunk,
    ) -> Result<()> {
        self.ensure_open()?;
        let message = QuackMessage::AppendRequest {
            header: self.scoped_header(MessageType::AppendRequest)?,
            schema_name,
            table_name: table_name.into(),
            append_chunk: chunk,
        };
        let response = self.send(&message).await?;
        expect_success(response)
    }

    pub async fn append_rows(
        &self,
        table_name: impl Into<String>,
        schema_name: Option<String>,
        rows: &[Row],
        columns: Option<Vec<ColumnDefinition>>,
        batch_size: Option<usize>,
    ) -> Result<()> {
        let table_name = table_name.into();
        if rows.is_empty() {
            let chunk = data_chunk_from_rows(rows, columns)?;
            return self.append(table_name, schema_name, chunk).await;
        }
        let batch_size = batch_size.unwrap_or(rows.len());
        if batch_size == 0 {
            return Err(QuackError::protocol(
                "append_rows batch_size must be at least 1",
            ));
        }
        for batch in rows.chunks(batch_size) {
            let chunk = data_chunk_from_rows(batch, columns.clone())?;
            self.append(table_name.clone(), schema_name.clone(), chunk)
                .await?;
        }
        Ok(())
    }

    pub async fn disconnect(&mut self) -> Result<()> {
        if self.closed || self.connection_id.is_none() {
            self.closed = true;
            return Ok(());
        }
        let message = QuackMessage::Disconnect {
            header: self.scoped_header(MessageType::DisconnectMessage)?,
        };
        let response = self.send(&message).await?;
        expect_success(response)?;
        self.closed = true;
        self.connection_id = None;
        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        self.disconnect().await
    }

    pub(crate) async fn send(&self, message: &QuackMessage) -> Result<QuackMessage> {
        let bytes = encode_message(message)?;
        let mut request = self
            .http
            .post(format!("{}{}", self.base_url, QUACK_ENDPOINT))
            .header(ACCEPT, HeaderValue::from_static(DUCKDB_MIME_TYPE))
            .header(CONTENT_TYPE, HeaderValue::from_static(DUCKDB_MIME_TYPE))
            .body(bytes);
        if !self.headers.is_empty() {
            request = request.headers(self.headers.clone());
        }
        request = request.timeout(self.timeout);
        let response = request.send().await?;
        if !response.status().is_success() {
            return Err(QuackError::protocol(format!(
                "Quack HTTP request failed with {} {}",
                response.status().as_u16(),
                response.status().canonical_reason().unwrap_or("")
            )));
        }
        let bytes = response.bytes().await?;
        let decoded = decode_message(&bytes)?;
        if let QuackMessage::ErrorResponse { message, .. } = decoded {
            return Err(QuackError::server(message));
        }
        Ok(decoded)
    }

    async fn prepare(&self, sql: &str) -> Result<QuackMessage> {
        self.ensure_open()?;
        let message = QuackMessage::PrepareRequest {
            header: self.scoped_header(MessageType::PrepareRequest)?,
            sql: sql.to_string(),
        };
        self.send(&message).await
    }

    async fn fetch_result(&self, result_uuid: crate::binary::HugeIntParts) -> Result<QuackMessage> {
        self.ensure_open()?;
        let message = QuackMessage::FetchRequest {
            header: self.scoped_header(MessageType::FetchRequest)?,
            result_uuid,
        };
        self.send(&message).await
    }

    fn scoped_header(&self, message_type: MessageType) -> Result<MessageHeader> {
        let connection_id = self
            .connection_id
            .clone()
            .ok_or_else(|| QuackError::protocol("Quack client is not connected"))?;
        let query_id = self.query_counter.fetch_add(1, Ordering::Relaxed);
        Ok(MessageHeader::new(message_type)
            .with_connection(connection_id)
            .with_client_query_id(query_id))
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed || self.connection_id.is_none() {
            Err(QuackError::protocol("Quack client is not connected"))
        } else {
            Ok(())
        }
    }
}

pub(crate) fn parse_quack_uri(input: &str, ssl_override: Option<bool>) -> Result<ParsedQuackUri> {
    let uri = input.trim();
    if uri.is_empty() {
        return Err(QuackError::protocol("Quack URI is empty"));
    }
    if uri.starts_with("http://") || uri.starts_with("https://") {
        let url = url::Url::parse(uri)?;
        let ssl = url.scheme() == "https";
        let port = url
            .port_or_known_default()
            .unwrap_or(if ssl { 443 } else { 80 });
        let host = url
            .host_str()
            .ok_or_else(|| QuackError::protocol(format!("invalid Quack URI host {input}")))?
            .to_string();
        let host_for_base = if host.contains(':') {
            format!("[{host}]")
        } else {
            host.clone()
        };
        return Ok(ParsedQuackUri {
            base_url: format!("{}://{}:{port}", url.scheme(), host_for_base),
            host,
            port,
            ssl,
        });
    }

    let rest = uri
        .strip_prefix("quack://")
        .or_else(|| uri.strip_prefix("quack:"))
        .unwrap_or(uri);
    if rest.is_empty() {
        return Err(QuackError::protocol(format!("invalid Quack URI {input}")));
    }
    let (host, port) = parse_host_port(rest)?;
    let ssl = ssl_override.unwrap_or(false);
    let protocol = if ssl { "https" } else { "http" };
    let host_for_base = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.clone()
    };
    Ok(ParsedQuackUri {
        base_url: format!("{protocol}://{host_for_base}:{port}"),
        host,
        port,
        ssl,
    })
}

fn parse_host_port(value: &str) -> Result<(String, u16)> {
    if let Some(rest) = value.strip_prefix('[') {
        let end = rest
            .find(']')
            .ok_or_else(|| QuackError::protocol(format!("invalid IPv6 Quack URI host {value}")))?;
        let host = rest[..end].to_string();
        let suffix = &rest[end + 1..];
        let port = if let Some(port) = suffix.strip_prefix(':') {
            parse_port(port)?
        } else {
            DEFAULT_QUACK_PORT
        };
        return Ok((host, port));
    }
    let colon_count = value.chars().filter(|ch| *ch == ':').count();
    match colon_count {
        0 => Ok((value.to_string(), DEFAULT_QUACK_PORT)),
        1 => {
            let (host, port) = value
                .split_once(':')
                .ok_or_else(|| QuackError::protocol(format!("invalid Quack URI {value}")))?;
            if host.is_empty() {
                return Err(QuackError::protocol(format!(
                    "invalid Quack URI host {value}"
                )));
            }
            Ok((host.to_string(), parse_port(port)?))
        }
        _ => Err(QuackError::protocol(format!(
            "IPv6 Quack URI hosts must be enclosed in []: {value}"
        ))),
    }
}

fn parse_port(value: &str) -> Result<u16> {
    let port = value
        .parse::<u16>()
        .map_err(|_| QuackError::protocol(format!("invalid Quack URI port {value}")))?;
    if port == 0 {
        return Err(QuackError::protocol(format!(
            "invalid Quack URI port {value}"
        )));
    }
    Ok(port)
}

fn attach_column_names(chunks: &mut [DataChunk], names: &[String]) {
    for chunk in chunks {
        chunk.column_names = Some(names.to_vec());
    }
}

/// Drains a query stream into decoded rows plus the result column names.
async fn drain_rows(mut stream: QuackResultStream) -> Result<(Vec<String>, Vec<Row>)> {
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.try_next().await? {
        chunks.push(chunk);
    }
    let names: Vec<String> = stream
        .columns()
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let rows = chunks_to_rows(&chunks, Some(&names))?;
    Ok((names, rows))
}

fn expect_success(response: QuackMessage) -> Result<()> {
    match response {
        QuackMessage::SuccessResponse { .. } => Ok(()),
        other => Err(QuackError::protocol(format!(
            "expected SUCCESS_RESPONSE, got {:?}",
            other.message_type()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_have_request_timeout() {
        assert_eq!(
            QuackClientOptions::default().timeout,
            Some(DEFAULT_QUACK_REQUEST_TIMEOUT)
        );
    }
}
