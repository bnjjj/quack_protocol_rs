use std::iter::zip;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use async_stream::try_stream;
use futures_util::stream::{self, BoxStream};
use futures_util::{StreamExt, TryStreamExt};
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue};
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::binary::HugeIntParts;
use crate::builders::{ColumnDefinition, data_chunk_from_rows};
use crate::constants::{
    DEFAULT_HEARTBEAT_TIMEOUT_SECS, DEFAULT_QUACK_PORT, DUCKDB_MIME_TYPE,
    MAX_HEARTBEAT_TIMEOUT_SECS, MAX_QUACK_VERSION, MIN_QUACK_VERSION, QUACK_ENDPOINT, QUACK_V1,
    QUACK_V3,
};
use crate::errors::{QuackError, Result};
use crate::messages::{
    MessageHeader, MessageType, QuackMessage, decode_message_for_version,
    encode_message_for_version,
};
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
    pub client_id: Option<String>,
    pub heartbeat_timeout: Option<Duration>,
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
            client_id: None,
            heartbeat_timeout: Some(Duration::from_secs(DEFAULT_HEARTBEAT_TIMEOUT_SECS)),
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
    pub heartbeat_timeout: Option<Duration>,
}

pub struct QueryMetadata {
    pub query_id: Option<String>,
}

#[must_use = "query results are dropped unless the stream is consumed"]
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
    next_batch_index: u64,
    ack_index: u64,
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
    state: Arc<ConnectionState>,
    _heartbeat: Option<Arc<HeartbeatGuard>>,
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
        let state = Arc::clone(&connection.state);
        let heartbeat = info
            .heartbeat_timeout
            .map(|timeout| Arc::new(HeartbeatGuard::start(&connection, timeout)));

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            state,
            _heartbeat: heartbeat,
            info: Some(info),
        })
    }

    pub fn is_connected(&self) -> bool {
        !self.state.is_closed()
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
            next_batch_index: 1,
            ack_index: 0,
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
            mut next_batch_index,
            mut ack_index,
        } = state;
        let column_names = columns
            .iter()
            .map(|col| col.name.to_owned())
            .collect::<Vec<_>>();
        let quack_version = connection.quack_version;

        Box::pin(try_stream! {
            while needs_more_fetch {
                let fetch_started = Instant::now();
                let (mut results, total_batches, batch_index) = match connection
                    .fetch(result_uuid, next_batch_index, ack_index)
                    .await?
                {
                    QuackMessage::FetchResponse {
                        results,
                        total_batches,
                        batch_index,
                        ..
                    } => (results, total_batches, batch_index),
                    other => Err(QuackError::protocol(format!(
                        "expected FETCH_RESPONSE, got {:?}",
                        other.message_type()
                    )))?,
                };

                if quack_version == QUACK_V3 {
                    match batch_index {
                        Some(batch_index) if batch_index == next_batch_index => {
                            if results.is_empty() {
                                Err(QuackError::protocol(
                                    "FETCH_RESPONSE batch did not include any chunks",
                                ))?;
                            }
                            ack_index = batch_index;
                            next_batch_index = next_batch_index.checked_add(1).ok_or_else(|| {
                                QuackError::protocol("FETCH_RESPONSE batch index overflow")
                            })?;
                        }
                        // A v3 FETCH_REQUEST asks for one exact batch. The server may produce
                        // batches out of order internally, but it must answer with the requested
                        // index; accepting another index here would silently reorder the result.
                        Some(batch_index) => Err(QuackError::protocol(format!(
                            "expected FETCH_RESPONSE batch {next_batch_index}, got {batch_index}"
                        )))?,
                        None if results.is_empty() => {
                            if let Some(total_batches) = total_batches {
                                if total_batches != ack_index {
                                    Err(QuackError::protocol(format!(
                                        "FETCH_RESPONSE ended after {ack_index} batches but announced {total_batches}"
                                    )))?;
                                }
                            }
                        }
                        None => Err(QuackError::protocol(
                            "FETCH_RESPONSE with chunks did not include a batch index",
                        ))?,
                    }
                    needs_more_fetch = batch_index.is_some();
                } else {
                    needs_more_fetch = !results.is_empty();
                }

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
        self.connection
            .lock()
            .await
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
    transport: Transport,
    connection_id: String,
    quack_version: u64,
    query_counter: AtomicU64,
    query_uuid_counter: AtomicU64,
    state: Arc<ConnectionState>,
}

impl Connection {
    async fn connect(
        base_url: String,
        http: reqwest::Client,
        timeout: Duration,
        options: QuackClientOptions,
    ) -> Result<(Self, QuackConnectionInfo)> {
        let min_version = options
            .min_supported_quack_version
            .unwrap_or(MIN_QUACK_VERSION);
        let max_version = options
            .max_supported_quack_version
            .unwrap_or(MAX_QUACK_VERSION);
        if min_version > max_version {
            return Err(QuackError::protocol(format!(
                "minimum Quack protocol version {min_version} exceeds maximum {max_version}"
            )));
        }
        let versions = [QUACK_V3, QUACK_V1]
            .into_iter()
            .filter(|version| (min_version..=max_version).contains(version))
            .collect::<Vec<_>>();
        if versions.is_empty() {
            return Err(QuackError::protocol(format!(
                "supported Quack protocol versions are {QUACK_V1} and {QUACK_V3}, requested range was {min_version}..={max_version}"
            )));
        }

        let heartbeat_timeout_seconds = options
            .heartbeat_timeout
            .unwrap_or_else(|| Duration::from_secs(DEFAULT_HEARTBEAT_TIMEOUT_SECS))
            .as_secs();
        if versions.contains(&QUACK_V3) {
            validate_heartbeat_timeout(heartbeat_timeout_seconds)?;
        }
        let transport = Transport {
            base_url,
            http,
            headers: options.headers.clone(),
            timeout,
        };

        let mut previous_error = None;
        for (index, quack_version) in versions.iter().copied().enumerate() {
            let response = transport
                .send(
                    &QuackMessage::ConnectionRequest {
                        header: MessageHeader::new(MessageType::ConnectionRequest),
                        auth_string: options.auth_token.clone(),
                        client_duckdb_version: options.client_duckdb_version.clone(),
                        client_platform: Some(
                            options
                                .client_platform
                                .clone()
                                .unwrap_or_else(|| "quack-rust".to_string()),
                        ),
                        min_supported_quack_version: quack_version,
                        max_supported_quack_version: quack_version,
                        client_id: options.client_id.clone(),
                        heartbeat_timeout_seconds,
                    },
                    quack_version,
                )
                .await;

            let response = match response {
                Ok(response) => response,
                Err(error)
                    if index + 1 < versions.len()
                        && quack_version == QUACK_V3
                        && is_version_negotiation_error(&error) =>
                {
                    previous_error = Some(error);
                    continue;
                }
                Err(error) if previous_error.is_some() && is_version_negotiation_error(&error) => {
                    return Err(previous_error.expect("checked above"));
                }
                Err(error) => return Err(error),
            };

            match response {
                QuackMessage::ConnectionResponse {
                    header,
                    server_duckdb_version,
                    server_platform,
                    quack_version: selected_version,
                    heartbeat_timeout_seconds,
                } => {
                    let connection_id = header.connection_id.ok_or_else(|| {
                        QuackError::protocol("CONNECTION_RESPONSE did not include a connection id")
                    })?;
                    if selected_version != Some(quack_version) {
                        return Err(QuackError::protocol(format!(
                            "server selected Quack protocol version {selected_version:?}, expected {quack_version}"
                        )));
                    }
                    let heartbeat_timeout = if quack_version == QUACK_V3 {
                        let seconds = heartbeat_timeout_seconds.ok_or_else(|| {
                            QuackError::protocol(
                                "protocol v3 CONNECTION_RESPONSE did not include a heartbeat timeout",
                            )
                        })?;
                        validate_heartbeat_timeout(seconds)?;
                        Some(Duration::from_secs(seconds))
                    } else {
                        None
                    };
                    let info = QuackConnectionInfo {
                        server_duckdb_version,
                        server_platform,
                        quack_version: Some(quack_version),
                        heartbeat_timeout,
                    };
                    return Ok((
                        Self {
                            transport,
                            connection_id,
                            quack_version,
                            query_counter: AtomicU64::new(1),
                            query_uuid_counter: AtomicU64::new(1),
                            state: Arc::new(ConnectionState::new()),
                        },
                        info,
                    ));
                }
                other => {
                    return Err(QuackError::protocol(format!(
                        "expected CONNECTION_RESPONSE, got {:?}",
                        other.message_type()
                    )));
                }
            }
        }

        Err(previous_error.unwrap_or_else(|| {
            QuackError::protocol("no compatible Quack protocol version was attempted")
        }))
    }

    async fn send(&self, message: &QuackMessage) -> Result<QuackMessage> {
        let response = self.transport.send(message, self.quack_version).await?;
        self.state.record_success();
        Ok(response)
    }

    async fn prepare(&self, sql: &str) -> Result<QuackMessage> {
        self.ensure_open()?;
        let query_uuid = HugeIntParts {
            upper: 0,
            lower: self.query_uuid_counter.fetch_add(1, Ordering::Relaxed),
        };
        let message = QuackMessage::PrepareRequest {
            header: self.scoped_header(MessageType::PrepareRequest),
            sql: sql.to_string(),
            query_uuid: (self.quack_version == QUACK_V3).then_some(query_uuid),
            inline_rows: None,
        };
        self.send(&message).await
    }

    async fn fetch(
        &self,
        result_uuid: HugeIntParts,
        batch_index: u64,
        ack_index: u64,
    ) -> Result<QuackMessage> {
        self.ensure_open()?;
        let message = QuackMessage::FetchRequest {
            header: self.scoped_header(MessageType::FetchRequest),
            result_uuid,
            batch_index: (self.quack_version == QUACK_V3).then_some(batch_index),
            ack_index: (self.quack_version == QUACK_V3).then_some(ack_index),
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
        if self.quack_version != QUACK_V1 {
            return Err(QuackError::protocol(
                "append is only supported by Quack protocol v1; protocol v3 uses SEND_DATA",
            ));
        }
        let message = QuackMessage::AppendRequest {
            header: self.scoped_header(MessageType::SendDataRequest),
            schema_name,
            table_name,
            append_chunk: chunk,
        };
        expect_success(self.send(&message).await?)
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
        self.state.close();
        Ok(())
    }

    fn scoped_header(&self, message_type: MessageType) -> MessageHeader {
        let query_id = self.query_counter.fetch_add(1, Ordering::Relaxed);
        MessageHeader::new(message_type)
            .with_connection(self.connection_id.clone())
            .with_client_query_id(query_id)
    }

    fn ensure_open(&self) -> Result<()> {
        if self.state.is_closed() {
            Err(QuackError::protocol("Quack client is not connected"))
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug)]
struct Transport {
    base_url: String,
    http: reqwest::Client,
    headers: HeaderMap,
    timeout: Duration,
}

impl Transport {
    async fn send(&self, message: &QuackMessage, quack_version: u64) -> Result<QuackMessage> {
        let bytes = encode_message_for_version(message, quack_version)?;
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
        let decoded = decode_message_for_version(&bytes, quack_version)?;
        if let QuackMessage::ErrorResponse { message, .. } = decoded {
            return Err(QuackError::server(message));
        }
        Ok(decoded)
    }
}

#[derive(Debug)]
struct HeartbeatGuard {
    task: tokio::task::JoinHandle<()>,
}

impl HeartbeatGuard {
    fn start(connection: &Connection, heartbeat_timeout: Duration) -> Self {
        let transport = connection.transport.clone();
        let connection_id = connection.connection_id.clone();
        let state = Arc::clone(&connection.state);
        let interval = (heartbeat_timeout / 3).max(Duration::from_millis(100));
        let task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if state.is_closed() {
                    return;
                }
                let heartbeat = QuackMessage::HeartbeatRequest {
                    header: MessageHeader::new(MessageType::HeartbeatRequest)
                        .with_connection(connection_id.clone()),
                };
                match transport.send(&heartbeat, QUACK_V3).await {
                    Ok(QuackMessage::SuccessResponse { .. }) => state.record_success(),
                    Ok(other) => {
                        tracing::warn!(
                            response = ?other.message_type(),
                            "Quack heartbeat returned an unexpected response; closing connection"
                        );
                        state.close();
                        return;
                    }
                    Err(error) if state.close_if_expired(heartbeat_timeout) => {
                        tracing::warn!(
                            %error,
                            timeout_seconds = heartbeat_timeout.as_secs(),
                            "Quack heartbeat lease expired; closing connection"
                        );
                        return;
                    }
                    Err(error) => {
                        tracing::debug!(
                            %error,
                            "Quack heartbeat failed transiently; retrying before lease expiry"
                        );
                    }
                }
            }
        });
        Self { task }
    }
}

#[derive(Debug)]
struct ConnectionState {
    lease: StdMutex<ConnectionLease>,
}

#[derive(Debug)]
struct ConnectionLease {
    last_successful_activity: Instant,
    closed: bool,
}

impl ConnectionState {
    fn new() -> Self {
        Self::new_at(Instant::now())
    }

    fn new_at(now: Instant) -> Self {
        Self {
            lease: StdMutex::new(ConnectionLease {
                last_successful_activity: now,
                closed: false,
            }),
        }
    }

    fn record_success(&self) {
        self.record_success_at(Instant::now());
    }

    fn record_success_at(&self, now: Instant) {
        let mut lease = self.lease();
        if !lease.closed {
            lease.last_successful_activity = now;
        }
    }

    fn close_if_expired(&self, timeout: Duration) -> bool {
        self.close_if_expired_at(Instant::now(), timeout)
    }

    fn close_if_expired_at(&self, now: Instant, timeout: Duration) -> bool {
        let mut lease = self.lease();
        if lease.closed || now.saturating_duration_since(lease.last_successful_activity) >= timeout
        {
            lease.closed = true;
        }
        lease.closed
    }

    fn close(&self) {
        self.lease().closed = true;
    }

    fn is_closed(&self) -> bool {
        self.lease().closed
    }

    fn lease(&self) -> std::sync::MutexGuard<'_, ConnectionLease> {
        self.lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Drop for HeartbeatGuard {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn validate_heartbeat_timeout(seconds: u64) -> Result<()> {
    if !(1..=MAX_HEARTBEAT_TIMEOUT_SECS).contains(&seconds) {
        return Err(QuackError::protocol(format!(
            "heartbeat timeout must be between 1 and {MAX_HEARTBEAT_TIMEOUT_SECS} seconds"
        )));
    }
    Ok(())
}

pub(crate) fn is_version_negotiation_error(error: &QuackError) -> bool {
    let message = match error {
        QuackError::Protocol(message) | QuackError::Server(message) => message,
        _ => return false,
    };
    let message = message.to_ascii_lowercase();
    message.contains("quack version")
        || message.contains("protocol version")
        || message.contains("http request failed with 500")
        || message.contains("deserialize")
        || message.contains("end of object")
        || message.contains("unexpected field")
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

    #[test]
    fn heartbeat_timeout_is_bounded() {
        assert!(validate_heartbeat_timeout(1).is_ok());
        assert!(validate_heartbeat_timeout(MAX_HEARTBEAT_TIMEOUT_SECS).is_ok());
        assert!(validate_heartbeat_timeout(0).is_err());
        assert!(validate_heartbeat_timeout(MAX_HEARTBEAT_TIMEOUT_SECS + 1).is_err());
    }

    #[test]
    fn connection_state_expires_only_after_the_last_successful_activity() {
        let started = Instant::now();
        let timeout = Duration::from_secs(30);
        let state = ConnectionState::new_at(started);

        assert!(!state.close_if_expired_at(started + Duration::from_secs(29), timeout));

        state.record_success_at(started + Duration::from_secs(29));
        assert!(!state.close_if_expired_at(started + Duration::from_secs(58), timeout));
        assert!(state.close_if_expired_at(started + Duration::from_secs(59), timeout));
        assert!(state.is_closed());
    }
}
