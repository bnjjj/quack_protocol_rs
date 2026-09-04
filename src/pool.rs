//! A pool of Quack sessions, for running queries concurrently.

use std::ops::Deref;
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
/// handed on, and retiring it closes its session on the server.
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

    pub async fn execute(&self, sql: &str, metadata: Option<&QueryMetadata>) -> Result<()> {
        self.query(sql, metadata).await?.drain().await
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
        self.inner.closed.store(true, Ordering::Relaxed);
        // Wakes anyone waiting on `acquire` with a "pool is closed" error.
        self.inner.permits.close();

        let mut first_error = None;
        for client in self.inner.drain_idle() {
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
        match lease.query_inner(sql, params, metadata).await {
            Ok(stream) => Ok(attach_lease(stream, lease)),
            // The server does not know this session - it restarted, or the
            // session was closed from elsewhere. It rejects such a request
            // before running any SQL, so nothing has happened that a retry
            // could repeat. Every idle connection was opened against the same
            // server, so retire them all and start from a fresh one.
            //
            // Only PREPARE is retried. Once results are streaming the
            // statement has already run, and errors from FETCH are passed
            // through untouched.
            Err(err) if err.is_connection_lost() => {
                drop(lease);
                self.inner.retire_idle();
                let permit = self.inner.permit().await?;
                let lease = self.inner.lease(self.inner.connect_one().await?, permit);
                let stream = lease.query_inner(sql, params, metadata).await?;
                Ok(attach_lease(stream, lease))
            }
            Err(err) => Err(err),
        }
    }
}

/// A connection borrowed from a [`QuackPool`].
///
/// Derefs to the underlying [`QuackClient`], and returns the connection to the
/// pool when dropped.
#[derive(Debug)]
pub struct PooledClient {
    // `Some` until `Drop` hands the client back.
    client: Option<QuackClient>,
    pool: Arc<PoolInner>,
    _permit: OwnedSemaphorePermit,
}

impl Deref for PooledClient {
    type Target = QuackClient;

    fn deref(&self) -> &Self::Target {
        self.client
            .as_ref()
            .expect("pooled client is taken only by Drop")
    }
}

impl Drop for PooledClient {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            self.pool.release(client);
        }
        // The permit drops next, after the connection is back in the pool, so
        // a waiter that wakes on it finds the connection waiting.
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
        PooledClient {
            client: Some(client),
            pool: Arc::clone(self),
            _permit: permit,
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

    fn release(&self, client: QuackClient) {
        if self.closed.load(Ordering::Relaxed) || !client.is_reusable() {
            // Dropping the last handle closes the session on the server.
            return;
        }
        self.lock_idle().push(client);
    }

    // Closes every idle session: the clients are dropped here, and dropping
    // the last handle to one sends its DISCONNECT.
    fn retire_idle(&self) {
        drop(self.drain_idle());
    }

    fn drain_idle(&self) -> Vec<QuackClient> {
        std::mem::take(&mut *self.lock_idle())
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
    use super::*;

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
}
