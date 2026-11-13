/// A Quack protocol version this crate speaks: [`QUACK_V1`] or [`QUACK_V3`].
///
/// The number is private, so no other version can be named.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QuackVersion(u64);

/// Quack protocol version 1.
pub const QUACK_V1: QuackVersion = QuackVersion(1);

/// Quack protocol version 3. Unlike v1, it has CANCEL and heartbeats.
pub const QUACK_V3: QuackVersion = QuackVersion(3);

impl QuackVersion {
    /// The version's number on the wire.
    pub const fn number(self) -> u64 {
        self.0
    }

    /// The version whose number on the wire is `number`, if this crate speaks it.
    pub(crate) const fn from_number(number: u64) -> Option<Self> {
        match number {
            1 => Some(QUACK_V1),
            3 => Some(QUACK_V3),
            _ => None,
        }
    }
}

impl std::fmt::Display for QuackVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

pub(crate) const MIN_QUACK_VERSION: QuackVersion = QUACK_V1;
pub(crate) const MAX_QUACK_VERSION: QuackVersion = QUACK_V3;
pub(crate) const DEFAULT_QUACK_PORT: u16 = 9494;
pub(crate) const QUACK_ENDPOINT: &str = "/quack";
pub(crate) const DUCKDB_MIME_TYPE: &str = "application/duckdb";
pub(crate) const FIELD_END: u16 = 0xffff;
pub(crate) const OPTIONAL_INDEX_INVALID: u64 = u64::MAX;
pub(crate) const DEFAULT_HEARTBEAT_TIMEOUT_SECS: u64 = 60;
pub(crate) const MAX_HEARTBEAT_TIMEOUT_SECS: u64 = 300;
