pub type Result<T> = std::result::Result<T, QuackError>;

// Errors the Quack server reports when it no longer holds the session named by
// our connection id - after a server restart, or once the session was
// disconnected. The server rejects such a request before running any SQL, so a
// caller may safely retry it on a fresh connection.
//
// The protocol carries no error code, only the extension's message text, so
// these are matched as substrings and tracked against `duckdb-quack`.
const CONNECTION_LOST_MESSAGES: [&str; 2] = [
    "Invalid connection id",
    "Connection does not exist / already disconnected",
];

#[derive(Debug, thiserror::Error)]
pub enum QuackError {
    #[error("quack protocol error: {0}")]
    Protocol(String),
    #[error("quack server error: {0}")]
    Server(String),
    #[error("unsupported quack type: {0}")]
    UnsupportedType(String),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("url parse error: {0}")]
    Url(#[from] url::ParseError),
    #[error("utf-8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

impl QuackError {
    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol(message.into())
    }

    pub(crate) fn server(message: impl Into<String>) -> Self {
        Self::Server(message.into())
    }

    pub(crate) fn unsupported(message: impl Into<String>) -> Self {
        Self::UnsupportedType(message.into())
    }

    /// The server told us the connection is gone, before running any SQL.
    ///
    /// Retrying the request on a fresh connection cannot duplicate a write.
    pub fn is_connection_lost(&self) -> bool {
        match self {
            Self::Server(message) => CONNECTION_LOST_MESSAGES
                .iter()
                .any(|known| message.contains(known)),
            _ => false,
        }
    }

    /// The connection that produced this error should not be reused.
    ///
    /// Transport and framing failures leave no way to tell what the server did
    /// with the request, so a pool retires the connection rather than hand it
    /// to the next caller. Errors raised without touching the wire - bad
    /// arguments, unsupported types - are not counted.
    pub fn is_connection_fatal(&self) -> bool {
        match self {
            Self::Http(_) | Self::Protocol(_) => true,
            Self::Server(_) => self.is_connection_lost(),
            Self::UnsupportedType(_) | Self::Url(_) | Self::Utf8(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_connection_errors_are_lost_and_fatal() {
        for message in CONNECTION_LOST_MESSAGES {
            let err = QuackError::server(message);
            assert!(err.is_connection_lost(), "{message}");
            assert!(err.is_connection_fatal(), "{message}");
        }
    }

    #[test]
    fn sql_errors_leave_the_connection_usable() {
        let err = QuackError::server("Table with name t does not exist!");
        assert!(!err.is_connection_lost());
        assert!(!err.is_connection_fatal());
    }

    #[test]
    fn transport_errors_are_fatal_but_not_retryable() {
        let err = QuackError::protocol("expected PREPARE_RESPONSE, got FetchResponse");
        assert!(err.is_connection_fatal());
        assert!(!err.is_connection_lost());
    }
}
