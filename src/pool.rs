//! A pool of Quack sessions, for running queries concurrently.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use async_stream::try_stream;
use futures_util::StreamExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::builders::ColumnDefinition;
use crate::client::{
    QuackClient, QuackClientOptions, QuackConnectionInfo, QuackResultStream, QueryMetadata,
};
use crate::errors::{QuackError, Result};
use crate::sql::SqlParameters;
use crate::vector::{DataChunk, Row, Value};

/// Connections a [`QuackPool`] opens when no size is chosen.
pub const DEFAULT_MAX_CONNECTIONS: usize = 4;

#[derive(Clone, Debug)]
pub struct QuackPoolOptions {
    /// Upper bound on sessions open against the server at once. Callers past
    /// the limit wait for one to come free.
    ///
    /// A query engine should set this to the number of scans it wants running
    /// in parallel - DataFusion's `target_partitions`, say - bearing in mind
    /// that every connection is a DuckDB connection on the server.
    ///
    /// It must be at least as large as the number of result streams a caller
    /// holds open at the same time. A plan that reads two streams in step
    /// while the pool can only supply one would wait forever: the parked
    /// stream holds the connection the other one is waiting for.
    pub max_connections: usize,
}

impl Default for QuackPoolOptions {
    fn default() -> Self {
        Self {
            max_connections: DEFAULT_MAX_CONNECTIONS,
        }
    }
}

/// A set of Quack sessions shared by concurrent callers.
///
/// A [`QuackClient`] is one server-side session and runs one query at a time
/// (see its docs for why). A `QuackPool` keeps several of those sessions and
/// hands a free one to each caller, so N queries run on the server at once.
/// That is what a query engine wants: DataFusion opens one scan per partition
/// and expects them to proceed in parallel. Cloning the pool is cheap and
/// shares its connections, so one pool can back every table in a catalog.
///
/// The trade is session state. Temporary tables, `SET`, transactions, and
/// attached databases live on a single connection, and the pool gives no
/// affinity between calls - two [`query`](Self::query) calls may land on
/// different sessions. Work that depends on session state holds one connection
/// for as long as it needs it:
///
/// ```no_run
/// # use quack_protocol::{QuackPool, Result};
/// # async fn example(pool: &QuackPool) -> Result<()> {
/// let connection = pool.acquire().await?;
/// connection
///     .execute("CREATE TEMP TABLE staging AS SELECT 1 AS id", None)
///     .await?;
/// let ids = connection.values("SELECT id FROM staging").await?;
/// # let _ = ids;
/// # Ok(())
/// # }
/// ```
///
/// Connections open on demand up to
/// [`max_connections`](QuackPoolOptions::max_connections) and are reused
/// afterwards. A connection that saw a wire failure is retired rather than
/// handed on, and retiring it closes its session on the server. Nothing is
/// opened in its place until a later [`acquire`](Self::acquire) finds no idle
/// connection, and the retired session keeps its slot until its DISCONNECT
/// has been answered, so the pool never has more sessions open than its limit.
#[derive(Clone, Debug)]
pub struct QuackPool {
    inner: Arc<PoolInner>,
}

impl QuackPool {
    /// Connect to a Quack server and open the pool.
    ///
    /// One connection is made now, so a bad URI, token, or protocol version
    /// fails here rather than on the first query; the rest are opened as
    /// concurrent callers need them.
    pub async fn connect(
        uri: &str,
        options: QuackClientOptions,
        pool_options: QuackPoolOptions,
    ) -> Result<Self> {
        if pool_options.max_connections == 0 {
            return Err(QuackError::protocol(
                "QuackPoolOptions::max_connections must be at least 1",
            ));
        }
        let client = QuackClient::connect(uri, options.clone()).await?;
        let info = client.info.clone();

        Ok(Self {
            inner: Arc::new(PoolInner {
                uri: uri.to_string(),
                options,
                info,
                permits: Arc::new(Semaphore::new(pool_options.max_connections)),
                max_connections: pool_options.max_connections,
                idle: Mutex::new(vec![client]),
                closed: AtomicBool::new(false),
            }),
        })
    }

    /// What the server reported when the pool's first connection was made.
    pub fn info(&self) -> Option<&QuackConnectionInfo> {
        self.inner.info.as_ref()
    }

    pub fn max_connections(&self) -> usize {
        self.inner.max_connections
    }

    /// Take a connection out of the pool for exclusive use.
    ///
    /// Waits if every connection is busy. The connection returns to the pool
    /// when the returned lease is dropped, so hold the lease for exactly as
    /// long as the work that needs one session - and no longer.
    pub async fn acquire(&self) -> Result<PooledClient> {
        let permit = self.inner.permit().await?;
        let client = match self.inner.take_idle() {
            Some(client) => client,
            None => self.inner.connect_one().await?,
        };
        Ok(self.inner.lease(client, permit))
    }

    /// Run a query on any free connection.
    ///
    /// The returned stream holds its connection until it is drained or
    /// dropped, the same way [`QuackClient::query`] does - so drop it once the
    /// results are no longer wanted, rather than leaving it parked.
    pub async fn query(
        &self,
        sql: &str,
        metadata: Option<&QueryMetadata>,
    ) -> Result<QuackResultStream> {
        self.stream(sql, None, metadata).await
    }

    pub async fn query_with_params(
        &self,
        sql: &str,
        params: Option<&SqlParameters>,
    ) -> Result<QuackResultStream> {
        self.stream(sql, params, None).await
    }

    /// Run a statement on any free connection and discard its result.
    ///
    /// Returns the number of rows a single `INSERT`, `UPDATE`, `DELETE`, or
    /// `MERGE` touched, as DuckDB reports it. Returns `None` for DDL, queries,
    /// statements with `RETURNING`, and SQL batches.
    pub async fn execute(
        &self,
        sql: &str,
        metadata: Option<&QueryMetadata>,
    ) -> Result<Option<u64>> {
        self.query(sql, metadata).await?.affected_rows(sql).await
    }

    pub async fn first(&self, sql: &str) -> Result<Option<Row>> {
        self.query(sql, None).await?.first_row().await
    }

    pub async fn one(&self, sql: &str) -> Result<Row> {
        self.query(sql, None).await?.one_row().await
    }

    pub async fn values(&self, sql: &str) -> Result<Vec<Value>> {
        self.query(sql, None).await?.first_column().await
    }

    /// Append a chunk on any free connection.
    ///
    /// Unlike a query, a failed append is never retried: a request that failed
    /// after the server ran it is indistinguishable from one it never saw, and
    /// appending twice is worse than failing once.
    pub async fn append(
        &self,
        table_name: impl Into<String>,
        schema_name: Option<String>,
        chunk: DataChunk,
    ) -> Result<()> {
        self.acquire()
            .await?
            .append(table_name, schema_name, chunk)
            .await
    }

    /// Append rows on one connection, in batches. Not retried, as
    /// [`append`](Self::append) is not.
    pub async fn append_rows(
        &self,
        table_name: impl Into<String>,
        schema_name: Option<String>,
        rows: &[Row],
        columns: Option<Vec<ColumnDefinition>>,
        batch_size: Option<usize>,
    ) -> Result<()> {
        self.acquire()
            .await?
            .append_rows(table_name, schema_name, rows, columns, batch_size)
            .await
    }

    /// Close every session the pool holds and refuse further connections.
    ///
    /// Connections still leased are closed as their leases drop. Returns the
    /// first failure, after trying to close all of them.
    pub async fn close(&self) -> Result<()> {
        let idle = self.inner.close_and_drain();
        // Wakes anyone waiting on `acquire` with a "pool is closed" error.
        self.inner.permits.close();

        let mut first_error = None;
        for client in idle {
            if let Err(err) = client.disconnect().await {
                first_error.get_or_insert(err);
            }
        }
        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    async fn stream(
        &self,
        sql: &str,
        params: Option<&SqlParameters>,
        metadata: Option<&QueryMetadata>,
    ) -> Result<QuackResultStream> {
        let lease = self.acquire().await?;
        match lease.client().query_inner(sql, params, metadata).await {
            Ok(stream) => Ok(attach_lease(stream, lease)),
            Err(error) => {
                // Keep the permit while retiring a failed session. In the
                // ambiguous error-text case this closes a still-live session;
                // for a genuinely stale id the disconnect simply fails. A
                // later explicit call can then open a replacement without a
                // healthy old session temporarily exceeding the pool limit.
                if error.is_connection_fatal() {
                    let _ = lease.disconnect().await;
                }
                Err(error)
            }
        }
    }
}

/// A connection borrowed from a [`QuackPool`].
///
/// This handle may be cloned, and a query stream may outlive the handle that
/// created it. All such handles share ownership of the lease; the connection
/// returns to the pool only after the last handle or result stream is dropped.
///
/// The underlying [`QuackClient`] is intentionally not exposed. Giving out a
/// raw client clone would let it keep using the session after the pool had
/// assigned that session to another borrower.
///
/// ```compile_fail
/// # use quack_protocol::{QuackClient, QuackPool, Result};
/// # async fn cannot_extract_client(pool: &QuackPool) -> Result<()> {
/// let lease = pool.acquire().await?;
/// let raw_client = QuackClient::clone(&lease);
/// # let _ = raw_client;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct PooledClient {
    inner: Arc<PooledClientInner>,
    /// Connection metadata reported when this session was opened.
    pub info: Option<QuackConnectionInfo>,
}

#[derive(Debug)]
struct PooledClientInner {
    // Both `Some` until `Drop` hands them back.
    client: Option<QuackClient>,
    permit: Option<OwnedSemaphorePermit>,
    pool: Arc<PoolInner>,
}

impl PooledClient {
    fn client(&self) -> &QuackClient {
        self.inner
            .client
            .as_ref()
            .expect("pooled client is taken only by Drop")
    }

    pub fn is_connected(&self) -> bool {
        self.client().is_connected()
    }

    /// Run a statement on this leased session and discard its result.
    ///
    /// Returns affected rows under the same rules as [`QuackPool::execute`].
    pub async fn execute(
        &self,
        sql: &str,
        metadata: Option<&QueryMetadata>,
    ) -> Result<Option<u64>> {
        self.query(sql, metadata).await?.affected_rows(sql).await
    }

    /// Run a query on this leased session.
    ///
    /// The returned stream retains a clone of the lease, so dropping this
    /// handle does not return the session while the stream is still alive.
    pub async fn query(
        &self,
        sql: &str,
        metadata: Option<&QueryMetadata>,
    ) -> Result<QuackResultStream> {
        let stream = self.client().query_inner(sql, None, metadata).await?;
        Ok(attach_lease(stream, self.clone()))
    }

    pub async fn query_with_params(
        &self,
        sql: &str,
        params: Option<&SqlParameters>,
    ) -> Result<QuackResultStream> {
        let stream = self.client().query_inner(sql, params, None).await?;
        Ok(attach_lease(stream, self.clone()))
    }

    pub async fn first(&self, sql: &str) -> Result<Option<Row>> {
        self.query(sql, None).await?.first_row().await
    }

    pub async fn one(&self, sql: &str) -> Result<Row> {
        self.query(sql, None).await?.one_row().await
    }

    pub async fn values(&self, sql: &str) -> Result<Vec<Value>> {
        self.query(sql, None).await?.first_column().await
    }

    pub async fn append(
        &self,
        table_name: impl Into<String>,
        schema_name: Option<String>,
        chunk: DataChunk,
    ) -> Result<()> {
        self.client().append(table_name, schema_name, chunk).await
    }

    pub async fn append_rows(
        &self,
        table_name: impl Into<String>,
        schema_name: Option<String>,
        rows: &[Row],
        columns: Option<Vec<ColumnDefinition>>,
        batch_size: Option<usize>,
    ) -> Result<()> {
        self.client()
            .append_rows(table_name, schema_name, rows, columns, batch_size)
            .await
    }

    /// Close this leased session. The pool retires it after all lease handles
    /// and streams have been dropped.
    pub async fn disconnect(&self) -> Result<()> {
        self.client().disconnect().await
    }

    pub async fn close(&self) -> Result<()> {
        self.disconnect().await
    }
}

impl Drop for PooledClientInner {
    fn drop(&mut self) {
        let Some(client) = self.client.take() else {
            return;
        };
        let Some(retired) = self.pool.release(client) else {
            // The permit drops next, after the connection is back in the
            // pool, so a waiter that wakes on it finds the connection waiting.
            return;
        };
        // Retired: the permit stays with the session until its DISCONNECT is
        // answered, so the caller that wakes on it opens a replacement only
        // once the old session is gone. Without a runtime the client's own
        // `Drop` logs and gives up, and the permit is released here.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let permit = self.permit.take();
        runtime.spawn(async move {
            // Failures are logged by `Connection`; there is no caller to tell.
            let _ = retired.disconnect().await;
            drop(retired);
            drop(permit);
        });
    }
}

#[derive(Debug)]
struct PoolInner {
    uri: String,
    options: QuackClientOptions,
    info: Option<QuackConnectionInfo>,
    permits: Arc<Semaphore>,
    max_connections: usize,
    idle: Mutex<Vec<QuackClient>>,
    closed: AtomicBool,
}

impl PoolInner {
    async fn permit(&self) -> Result<OwnedSemaphorePermit> {
        Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| QuackError::protocol("Quack pool is closed"))
    }

    fn lease(self: &Arc<Self>, client: QuackClient, permit: OwnedSemaphorePermit) -> PooledClient {
        let info = client.info.clone();
        PooledClient {
            inner: Arc::new(PooledClientInner {
                client: Some(client),
                permit: Some(permit),
                pool: Arc::clone(self),
            }),
            info,
        }
    }

    async fn connect_one(&self) -> Result<QuackClient> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(QuackError::protocol("Quack pool is closed"));
        }
        QuackClient::connect(&self.uri, self.options.clone()).await
    }

    fn take_idle(&self) -> Option<QuackClient> {
        let mut idle = self.lock_idle();
        // Connections retired while idle are dropped here, which closes their
        // sessions on the server.
        while let Some(client) = idle.pop() {
            if client.is_reusable() {
                return Some(client);
            }
        }
        None
    }

    // Puts a connection back, or returns it when it must be retired instead:
    // the pool is closed or the session saw a wire failure. The closed check
    // happens under the idle lock so that `close_and_drain` cannot run between
    // the check and the push and leave a live session behind in a closed pool.
    fn release(&self, client: QuackClient) -> Option<QuackClient> {
        let mut idle = self.lock_idle();
        if self.closed.load(Ordering::Relaxed) || !client.is_reusable() {
            return Some(client);
        }
        idle.push(client);
        None
    }

    fn close_and_drain(&self) -> Vec<QuackClient> {
        let mut idle = self.lock_idle();
        self.closed.store(true, Ordering::Relaxed);
        std::mem::take(&mut *idle)
    }

    // The lock guards a `Vec` and nothing else, so a panic elsewhere in the
    // process must not take the pool down with it.
    fn lock_idle(&self) -> std::sync::MutexGuard<'_, Vec<QuackClient>> {
        self.idle.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

// Keeps the connection out of the pool until the caller is done with the
// results, the way `QuackClient` holds its own connection lock.
fn attach_lease(stream: QuackResultStream, lease: PooledClient) -> QuackResultStream {
    let (columns, chunks) = stream.into_chunks();
    let chunks = try_stream! {
        let _lease = lease;
        for await chunk in chunks {
            yield chunk?;
        }
    };
    QuackResultStream::new(columns, chunks.boxed())
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    use super::*;
    use crate::binary::HugeIntParts;
    use crate::constants::QUACK_V1;
    use crate::messages::{MessageHeader, MessageType, QuackMessage, encode_message_for_version};

    fn scripted_server(responses: Vec<QuackMessage>) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let responses = responses
            .into_iter()
            .map(|response| {
                encode_message_for_version(&response, QUACK_V1).expect("encode test response")
            })
            .collect::<Vec<_>>();
        let server = thread::spawn(move || {
            for response in responses {
                let (mut socket, _) = listener.accept().expect("accept test request");
                let mut request = [0; 4096];
                let bytes_read = socket.read(&mut request).expect("read test request");
                assert!(bytes_read > 0, "test request must not be empty");
                write!(
                    socket,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    response.len()
                )
                .expect("write test response headers");
                socket.write_all(&response).expect("write test response");
            }
        });
        (format!("http://{address}"), server)
    }

    fn v1_options() -> QuackClientOptions {
        QuackClientOptions {
            min_supported_quack_version: Some(QUACK_V1),
            max_supported_quack_version: Some(QUACK_V1),
            timeout: Some(Duration::from_secs(2)),
            ..QuackClientOptions::default()
        }
    }

    #[tokio::test]
    async fn rejects_an_empty_pool() {
        let err = QuackPool::connect(
            "localhost:9494",
            QuackClientOptions::default(),
            QuackPoolOptions { max_connections: 0 },
        )
        .await
        .expect_err("max_connections 0 is rejected");
        assert!(err.to_string().contains("max_connections"), "{err}");
    }

    #[test]
    fn default_pool_is_small() {
        assert_eq!(
            QuackPoolOptions::default().max_connections,
            DEFAULT_MAX_CONNECTIONS
        );
    }

    #[test]
    fn cloning_a_pooled_client_produces_another_lease() {
        let clone_lease: fn(&PooledClient) -> PooledClient = PooledClient::clone;
        let _ = clone_lease;
    }

    #[tokio::test]
    async fn clones_and_query_streams_keep_the_pool_lease() {
        let connection_response = QuackMessage::ConnectionResponse {
            header: MessageHeader::new(MessageType::ConnectionResponse)
                .with_connection("leased-session"),
            server_duckdb_version: None,
            server_platform: None,
            quack_version: Some(QUACK_V1),
            heartbeat_timeout_seconds: None,
        };
        let prepare_response = QuackMessage::PrepareResponse {
            header: MessageHeader::new(MessageType::PrepareResponse)
                .with_connection("leased-session"),
            result_types: Vec::new(),
            result_names: Vec::new(),
            needs_more_fetch: false,
            results: Vec::new(),
            result_uuid: HugeIntParts { upper: 0, lower: 1 },
        };
        let (uri, server) = scripted_server(vec![connection_response, prepare_response]);
        let pool = QuackPool::connect(&uri, v1_options(), QuackPoolOptions { max_connections: 1 })
            .await
            .expect("connect pool");

        let lease = pool.acquire().await.expect("acquire lease");
        let cloned_lease = lease.clone();
        drop(lease);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), pool.acquire())
                .await
                .is_err(),
            "a cloned handle must keep the permit"
        );

        let stream = cloned_lease
            .query("SELECT 1", None)
            .await
            .expect("create result stream");
        drop(cloned_lease);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), pool.acquire())
                .await
                .is_err(),
            "a result stream must keep the permit"
        );

        drop(stream);
        let returned = tokio::time::timeout(Duration::from_secs(1), pool.acquire())
            .await
            .expect("released stream should unblock acquire")
            .expect("reacquire returned session");
        drop(returned);
        server.join().expect("test server");
    }

    fn connection_response(connection_id: &str) -> QuackMessage {
        QuackMessage::ConnectionResponse {
            header: MessageHeader::new(MessageType::ConnectionResponse)
                .with_connection(connection_id),
            server_duckdb_version: None,
            server_platform: None,
            quack_version: Some(QUACK_V1),
            heartbeat_timeout_seconds: None,
        }
    }

    fn success_response() -> QuackMessage {
        QuackMessage::SuccessResponse {
            header: MessageHeader::new(MessageType::SuccessResponse),
        }
    }

    #[tokio::test]
    async fn a_malformed_response_retires_the_session() {
        // SUCCESS in answer to PREPARE decodes fine but is the wrong message.
        let (uri, server) = scripted_server(vec![
            connection_response("malformed-session"),
            success_response(),
            success_response(),
        ]);
        let client = QuackClient::connect(&uri, v1_options())
            .await
            .expect("connect client");
        assert!(client.is_reusable());

        let error = match client.query("SELECT 1", None).await {
            Ok(_) => panic!("wrong response type is a protocol error"),
            Err(error) => error,
        };
        assert!(
            matches!(error, QuackError::Protocol(ref message) if message.contains("PREPARE_RESPONSE")),
            "{error}"
        );
        assert!(
            !client.is_reusable(),
            "a session that answered out of protocol must not be handed on"
        );

        client.disconnect().await.expect("disconnect");
        server.join().expect("test server");
    }

    #[tokio::test]
    async fn a_retired_lease_keeps_its_slot_until_the_session_is_closed() {
        // Response order is the whole assertion: with one slot, the
        // replacement CONNECT must come after the retired session's
        // DISCONNECT. If the permit were released early, the replacement
        // would consume the DISCONNECT acknowledgement and fail to connect.
        let (uri, server) = scripted_server(vec![
            connection_response("retired-session"),
            success_response(), // wrong answer to PREPARE: retires the session
            success_response(), // acknowledges DISCONNECT
            connection_response("replacement-session"),
        ]);
        let pool = QuackPool::connect(&uri, v1_options(), QuackPoolOptions { max_connections: 1 })
            .await
            .expect("connect pool");

        let lease = pool.acquire().await.expect("acquire lease");
        assert!(
            lease.query("SELECT 1", None).await.is_err(),
            "wrong response type is a protocol error"
        );
        drop(lease);

        let replacement = tokio::time::timeout(Duration::from_secs(2), pool.acquire())
            .await
            .expect("slot frees once the retired session is closed")
            .expect("replacement connects after the DISCONNECT");
        assert!(replacement.is_connected());
        drop(replacement);
        server.join().expect("test server");
    }

    #[tokio::test]
    async fn ambiguous_connection_error_text_is_returned_without_replay() {
        let connection_response = QuackMessage::ConnectionResponse {
            header: MessageHeader::new(MessageType::ConnectionResponse)
                .with_connection("possibly-stale-session"),
            server_duckdb_version: None,
            server_platform: None,
            quack_version: Some(QUACK_V1),
            heartbeat_timeout_seconds: None,
        };
        let ambiguous_error = QuackMessage::ErrorResponse {
            header: MessageHeader::new(MessageType::ErrorResponse),
            message: "Invalid connection id".to_string(),
        };
        let disconnect_response = QuackMessage::SuccessResponse {
            header: MessageHeader::new(MessageType::SuccessResponse),
        };
        let (uri, server) = scripted_server(vec![
            connection_response,
            ambiguous_error,
            disconnect_response,
        ]);
        let pool = QuackPool::connect(&uri, v1_options(), QuackPoolOptions { max_connections: 1 })
            .await
            .expect("connect pool");

        let error = match pool.query("INSERT INTO items VALUES (1)", None).await {
            Ok(_) => panic!("ambiguous server text must be surfaced"),
            Err(error) => error,
        };
        assert!(
            matches!(error, QuackError::Server(ref message) if message == "Invalid connection id"),
            "the original server error must be returned: {error}"
        );

        // The third response acknowledges DISCONNECT. If the pool replayed the
        // statement, it would consume that response as another CONNECT and
        // return a protocol error instead of the original server error above.
        server.join().expect("test server");
    }
}
