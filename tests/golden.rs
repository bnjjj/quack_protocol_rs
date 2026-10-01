//! Messages captured from DuckDB 2.0 re-encode to the same bytes.
#![cfg(feature = "server")]

use quack_protocol::server::{decode_request, encode_response};

#[test]
fn golden_messages_round_trip_byte_for_byte() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/golden");
    let mut checked = 0;
    for entry in std::fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        if !path.extension().is_some_and(|e| e == "req" || e == "resp") {
            continue;
        }
        let bytes = std::fs::read(&path).unwrap();
        let message = decode_request(&bytes).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        assert_eq!(
            encode_response(&message).unwrap(),
            bytes,
            "{} ({:?})",
            path.display(),
            message.message_type()
        );
        checked += 1;
    }
    assert!(checked > 20, "only {checked} fixtures");
}
