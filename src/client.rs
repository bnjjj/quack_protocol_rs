use std::iter::zip;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_stream::try_stream;
use futures_util::stream::{self, BoxStream};
use futures_util::{StreamExt, TryStreamExt};
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue};
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::binary::HugeIntParts;
use crate::builders::{ColumnDefinition, data_chunk_from_rows};
use crate::constants::{DEFAULT_QUACK_PORT, DUCKDB_MIME_TYPE, QUACK_ENDPOINT, QUACK_VERSION};
use crate::errors::{QuackError, Result};
use crate::messages::{MessageHeader, MessageType, QuackMessage, decode_message, encode_message};
use crate::sql::{QuerySql, SqlParameters, format_sql};
use crate::vector::{DataChunk, Row, Value, rows_from_chunk_with_names};

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

pub struct QueryMetadata {
    pub query_id: Option<String>,
}

#[must_use = "call into_rows() or into_chunks() to consume the query results"]
pub struct QuackResultStream {
    columns: Vec<ColumnDefinition>,
    inner: BoxStream<'static, Result<DataChunk>>,
}

impl QuackResultStream {
    fn new(columns: Vec<ColumnDefinition>, chunks: BoxStream<'static, Result<DataChunk>>) -> Self {
        Self {
            columns,
            inner: chunks,
        }
    }

    pub fn into_chunks(self) -> (Vec<ColumnDefinition>, BoxStream<'static, Result<DataChunk>>) {
        (self.columns, self.inner)
    }

    pub fn into_rows(self) -> (Vec<ColumnDefinition>, BoxStream<'static, Result<Row>>) {
        let col_names: Vec<String> = self.columns.iter().map(|col| col.name.to_owned()).collect();
        let rows = self
            .inner
            .flat_map(move |chunk| {
                stream::iter(
                    match chunk.and_then(|chunk| rows_from_chunk_with_names(&chunk, &col_names)) {
                        Ok(rows) => rows.into_iter().map(Ok).collect(),
                        Err(err) => vec![Err(err)],
                    },
                )
            })
            .boxed();
        (self.columns, rows)
    }
}

struct FetchState {
    connection: OwnedMutexGuard<Connection>,
    sql: QuerySql,
    result_uuid: HugeIntParts,
    needs_more_fetch: bool,
    query_started: Instant,
    rows_delivered: usize,
}

#[derive(Clone, Debug)]
pub struct QuackClient {
    // Holds connection to Quack server.
    //
    // Server holds a resumable cursor for result-streaming per unique
    // connection_id, and a `QuackClient` (its clones included, since they
    // share this `Arc`) maps to exactly one connection_id for its whole
    // lifetime. A concurrent PREPARE (e.g. from another query on this
    // connection_id) resets the cursor, invalidating a FETCH still in
    // progress. Connection is wrapped in Mutex to ensure queries are
    // executed serially on server.
    //
    // TODO: support concurrent queries to quack server by introducing
    // a connection pool
    //
    // TODO: close message is not issued to Quack server when `Connection` is
    // dropped. Server retains the cursor for the dropped connection_id.
    connection: Arc<Mutex<Connection>>,
    pub info: Option<QuackConnectionInfo>,
}

impl QuackClient {
    pub async fn connect(uri: &str, options: QuackClientOptions) -> Result<Self> {
        let parsed = parse_quack_uri(uri, options.ssl)?;
        let timeout = options.timeout.unwrap_or(DEFAULT_QUACK_REQUEST_TIMEOUT);
        let http = reqwest::Client::builder()
            .connect_timeout(DEFAULT_QUACK_CONNECT_TIMEOUT.min(timeout))
            .pool_max_idle_per_host(0)
            .timeout(timeout)
            .build()?;
        let base_url = parsed.base_url.trim_end_matches('/').to_string();
        let (connection, info) = Connection::connect(base_url, http, timeout, options).await?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            info: Some(info),
        })
    }

    pub fn is_connected(&self) -> bool {
        match self.connection.try_lock() {
            Ok(guard) => !guard.closed.load(Ordering::Relaxed),
            // Something's actively using the connection, so it can't be closed yet -
            // closing requires this same lock.
            Err(_) => true,
        }
    }

    pub async fn execute(&self, sql: &str, metadata: Option<&QueryMetadata>) -> Result<()> {
        let (_, chunks) = self.query_inner(sql, None, metadata).await?.into_chunks();
        chunks.try_for_each(|_| async { Ok(()) }).await
    }

    pub async fn query(
        &self,
        sql: &str,
        metadata: Option<&QueryMetadata>,
    ) -> Result<QuackResultStream> {
        self.query_inner(sql, None, metadata).await
    }

    pub async fn query_with_params(
        &self,
        sql: &str,
        params: Option<&SqlParameters>,
    ) -> Result<QuackResultStream> {
        self.query_inner(sql, params, None).await
    }

    // Execute a SQL query on Quack server and stream results via repeated
    // FETCH calls to server.
    async fn query_inner(
        &self,
        sql: &str,
        params: Option<&SqlParameters>,
        metadata: Option<&QueryMetadata>,
    ) -> Result<QuackResultStream> {
        let query_id = metadata
            .and_then(|metadata| metadata.query_id.as_deref())
            .unwrap_or("-")
            .to_string();
        let sql = QuerySql::new(format_sql(sql, params)?);

        let (columns, chunks, fetch_state) = self.prepare(sql, query_id.clone()).await?;
        let fetch_stream = self.fetch(fetch_state, &columns, query_id);

        Ok(QuackResultStream::new(
            columns,
            stream::iter(chunks).map(Ok).chain(fetch_stream).boxed(),
        ))
    }

    async fn prepare(
        &self,
        sql: QuerySql,
        query_id: String,
    ) -> Result<(Vec<ColumnDefinition>, Vec<DataChunk>, FetchState)> {
        // Acquires the connection lock here and carries it forward via
        // `FetchState` so the same lock stays held through every FETCH - see
        // `client.connection` field docs.
        let connection = Arc::clone(&self.connection).lock_owned().await;
        let query_started = Instant::now();
        let (result_types, result_names, needs_more_fetch, mut chunks, result_uuid) =
            match connection.prepare(sql.as_str()).await? {
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
                other => {
                    return Err(QuackError::protocol(format!(
                        "expected PREPARE_RESPONSE, got {:?}",
                        other.message_type()
                    )));
                }
            };

        let rows: usize = chunks.iter().map(|chunk| chunk.row_count).sum();
        tracing::debug!(
            query_id,
            %sql,
            %result_uuid,
            rows,
            elapsed_ms = query_started.elapsed().as_millis() as u64,
            "quack PREPARE completed"
        );

        attach_column_names(&mut chunks, &result_names);
        let columns: Vec<ColumnDefinition> = zip(result_names, result_types)
            .map(|(name, logical_type)| ColumnDefinition { name, logical_type })
            .collect();

        let fetch_state = FetchState {
            connection,
            sql,
            result_uuid,
            needs_more_fetch,
            query_started,
            rows_delivered: rows,
        };
        Ok((columns, chunks, fetch_state))
    }

    fn fetch(
        &self,
        state: FetchState,
        columns: &[ColumnDefinition],
        query_id: String,
    ) -> BoxStream<'static, Result<DataChunk>> {
        let FetchState {
            connection,
            sql,
            result_uuid,
            mut needs_more_fetch,
            query_started,
            mut rows_delivered,
        } = state;
        let column_names = columns
            .iter()
            .map(|col| col.name.to_owned())
            .collect::<Vec<_>>();

        Box::pin(try_stream! {
            while needs_more_fetch {
                let fetch_started = Instant::now();
                let mut results = match connection.fetch(result_uuid).await? {
                    QuackMessage::FetchResponse { results, .. } => results,
                    other => Err(QuackError::protocol(format!(
                        "expected FETCH_RESPONSE, got {:?}",
                        other.message_type()
                    )))?,
                };

                let rows: usize = results.iter().map(|chunk| chunk.row_count).sum();
                rows_delivered += rows;
                tracing::debug!(
                    query_id,
                    %sql,
                    %result_uuid,
                    rows,
                    elapsed_ms = fetch_started.elapsed().as_millis() as u64,
                    "quack FETCH completed"
                );

                needs_more_fetch = !results.is_empty();
                attach_column_names(&mut results, &column_names);
                for chunk in results {
                    yield chunk;
                }
            }

            tracing::debug!(
                query_id,
                %sql,
                %result_uuid,
                rows = rows_delivered,
                elapsed_ms = query_started.elapsed().as_millis() as u64,
                "quack query completed"
            );

            // `connection` drops here and the lock is released for the next queued
            // operation - the session on the Quack server stays open until
            // `disconnect`/`close` is called
        })
    }

    pub async fn first(&self, sql: &str) -> Result<Option<Row>> {
        let (_, rows) = self.query(sql, None).await?.into_rows();
        let rows: Vec<_> = rows.try_collect().await?;

        Ok(rows.into_iter().next())
    }

    pub async fn one(&self, sql: &str) -> Result<Row> {
        let (_, rows) = self.query(sql, None).await?.into_rows();
        let rows: Vec<_> = rows.try_collect().await?;

        if rows.len() != 1 {
            return Err(QuackError::protocol(format!(
                "expected exactly one row, got {}",
                rows.len()
            )));
        }
        Ok(rows.into_iter().next().expect("one row"))
    }

    pub async fn values(&self, sql: &str) -> Result<Vec<Value>> {
        let (columns, rows) = self.query(sql, None).await?.into_rows();
        let rows: Vec<_> = rows.try_collect().await?;

        let first_name = match columns.first() {
            Some(col) => &col.name,
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
        let connection = self.connection.lock().await;
        connection
            .append(table_name.into(), schema_name, chunk)
            .await
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

    pub async fn disconnect(&self) -> Result<()> {
        self.connection.lock().await.disconnect().await
    }

    pub async fn close(&self) -> Result<()> {
        self.disconnect().await
    }
}

#[derive(Debug)]
struct Connection {
    base_url: String,
    http: reqwest::Client,
    headers: HeaderMap,
    timeout: Duration,
    connection_id: String,
    query_counter: AtomicU64,
    closed: AtomicBool,
}

impl Connection {
    async fn connect(
        base_url: String,
        http: reqwest::Client,
        timeout: Duration,
        options: QuackClientOptions,
    ) -> Result<(Self, QuackConnectionInfo)> {
        let connection = Self {
            base_url,
            http,
            headers: options.headers,
            timeout,
            connection_id: String::new(),
            query_counter: AtomicU64::new(1),
            closed: AtomicBool::new(false),
        };
        let response = connection
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
                let info = QuackConnectionInfo {
                    server_duckdb_version,
                    server_platform,
                    quack_version,
                };
                Ok((
                    Self {
                        connection_id,
                        ..connection
                    },
                    info,
                ))
            }
            other => Err(QuackError::protocol(format!(
                "expected CONNECTION_RESPONSE, got {:?}",
                other.message_type()
            ))),
        }
    }

    async fn send(&self, message: &QuackMessage) -> Result<QuackMessage> {
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
            header: self.scoped_header(MessageType::PrepareRequest),
            sql: sql.to_string(),
        };
        self.send(&message).await
    }

    async fn fetch(&self, result_uuid: HugeIntParts) -> Result<QuackMessage> {
        self.ensure_open()?;
        let message = QuackMessage::FetchRequest {
            header: self.scoped_header(MessageType::FetchRequest),
            result_uuid,
        };
        self.send(&message).await
    }

    async fn append(
        &self,
        table_name: String,
        schema_name: Option<String>,
        chunk: DataChunk,
    ) -> Result<()> {
        self.ensure_open()?;
        let message = QuackMessage::AppendRequest {
            header: self.scoped_header(MessageType::AppendRequest),
            schema_name,
            table_name,
            append_chunk: chunk,
        };
        let response = self.send(&message).await?;
        expect_success(response)
    }

    async fn disconnect(&self) -> Result<()> {
        if self.ensure_open().is_err() {
            return Ok(());
        }
        let message = QuackMessage::Disconnect {
            header: self.scoped_header(MessageType::DisconnectMessage),
        };
        let response = self.send(&message).await?;
        expect_success(response)?;
        self.closed.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn scoped_header(&self, message_type: MessageType) -> MessageHeader {
        let query_id = self.query_counter.fetch_add(1, Ordering::Relaxed);
        MessageHeader::new(message_type)
            .with_connection(self.connection_id.clone())
            .with_client_query_id(query_id)
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Relaxed) {
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
