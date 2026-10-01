//! Stable HTTP rejection categories for admission-aware Quack proxies.

/// A typed proxy rejection. It never retains an upstream response body.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProxyError {
    #[error("worker at capacity")]
    AtCapacity,
    #[error("worker assignment changed")]
    AssignmentMismatch,
    #[error("worker admission is closed")]
    GateClosed,
    #[error("connection is not admitted")]
    InvalidConnection,
    #[error("connection has an operation in progress")]
    ConnectionBusy,
    #[error("controller authentication required")]
    Unauthorized,
    #[error("worker is unsafe")]
    WorkerUnsafe,
    #[error("worker recycling required")]
    RecycleRequired,
    #[error("query failed")]
    QueryFailed,
    #[error("invalid request")]
    InvalidRequest,
    #[error("request too large")]
    RequestTooLarge,
}

impl ProxyError {
    /// Recognizes a bounded structured failure only when its HTTP status agrees.
    /// Unknown failures remain generic transport/protocol errors, never successes.
    pub fn decode(status: u16, body: &[u8]) -> Option<Self> {
        if body.len() > 16 * 1024 {
            return None;
        }
        let value: serde_json::Value = serde_json::from_slice(body).ok()?;
        let code = value.get("error")?.get("code")?.as_str()?;
        Some(match (status, code) {
            (429, "at_capacity") => Self::AtCapacity,
            (409, "assignment_mismatch") => Self::AssignmentMismatch,
            (503, "gate_closed") => Self::GateClosed,
            (409, "invalid_connection") => Self::InvalidConnection,
            (409, "connection_busy") => Self::ConnectionBusy,
            (401, "unauthorized") => Self::Unauthorized,
            (503, "worker_unsafe") => Self::WorkerUnsafe,
            (503, "recycle_required") => Self::RecycleRequired,
            (400, "query_failed") => Self::QueryFailed,
            (400, "invalid_request") => Self::InvalidRequest,
            (413, "request_too_large") => Self::RequestTooLarge,
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unknown_status_code_pairs_and_discards_message_text() {
        for (status, code, expected) in [
            (429, "at_capacity", ProxyError::AtCapacity),
            (409, "assignment_mismatch", ProxyError::AssignmentMismatch),
            (503, "gate_closed", ProxyError::GateClosed),
            (409, "invalid_connection", ProxyError::InvalidConnection),
            (401, "unauthorized", ProxyError::Unauthorized),
        ] {
            let bytes = serde_json::to_vec(&serde_json::json!({"error": {"code": code, "message": "private SQL and credentials"}})).unwrap();
            let actual = ProxyError::decode(status, &bytes).unwrap();
            assert_eq!(actual, expected);
            assert!(!actual.to_string().contains("private"));
            assert_eq!(ProxyError::decode(200, &bytes), None);
        }
        assert_eq!(ProxyError::decode(429, &[b'x'; 16385]), None);
    }
}
