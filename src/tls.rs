use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls13_signature_with_raw_key};
use rustls::pki_types::{CertificateDer, ServerName, SubjectPublicKeyInfoDer, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};

use crate::errors::{QuackError, Result};

const SHA256_LEN: usize = 32;

/// SHA-256 fingerprint of the one server certificate a client trusts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CertificatePin([u8; SHA256_LEN]);

impl CertificatePin {
    /// Parses a fingerprint the way DuckDB's `ssl_fingerprint` does: hex
    /// digits in any case, optionally separated by colons or spaces, with an
    /// optional `sha256:` prefix.
    pub(crate) fn parse(fingerprint: &str) -> Result<Self> {
        let trimmed = fingerprint.trim();
        let digits = match trimmed.get(..7) {
            Some(prefix) if prefix.eq_ignore_ascii_case("sha256:") => &trimmed[7..],
            _ => trimmed,
        };
        let mut nibbles = Vec::with_capacity(SHA256_LEN * 2);
        for c in digits.chars().filter(|c| *c != ':' && *c != ' ') {
            let nibble = c.to_digit(16).ok_or_else(|| {
                QuackError::protocol(format!(
                    "invalid ssl_fingerprint {fingerprint:?}: expected the SHA-256 fingerprint \
                     of the server certificate as hex digits, optionally separated by colons"
                ))
            })?;
            nibbles.push(nibble as u8);
        }
        if nibbles.len() != SHA256_LEN * 2 {
            return Err(QuackError::protocol(format!(
                "invalid ssl_fingerprint {fingerprint:?}: a SHA-256 fingerprint has 64 hex digits, got {}",
                nibbles.len()
            )));
        }
        let mut bytes = [0; SHA256_LEN];
        for (byte, pair) in bytes.iter_mut().zip(nibbles.chunks_exact(2)) {
            *byte = (pair[0] << 4) | pair[1];
        }
        Ok(Self(bytes))
    }

    fn matches(&self, certificate: &CertificateDer<'_>) -> bool {
        ring::digest::digest(&ring::digest::SHA256, certificate.as_ref()).as_ref() == self.0
    }
}

/// A rustls client configuration that trusts exactly the certificate with
/// `pin`'s fingerprint, which is how DuckDB clients authenticate a Quack
/// server's self-signed certificate. CA and hostname checks are skipped; the
/// handshake signature is still verified against the pinned certificate's key.
///
/// Pinned connections use TLS 1.3 only: the signature is checked against the
/// certificate's raw public key, which rustls supports for TLS 1.3, so that
/// the X.509 v1 certificates `quack_generate_keys()` produces are accepted
/// (webpki parses v3 certificates only).
pub(crate) fn pinned_client_config(pin: CertificatePin) -> Result<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = PinnedCertVerifier {
        pin,
        provider: Arc::clone(&provider),
    };
    let mut config = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|err| QuackError::protocol(format!("failed to configure TLS: {err}")))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();
    // The ALPN list reqwest sets on the configurations it builds itself.
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(config)
}

#[derive(Debug)]
struct PinnedCertVerifier {
    pin: CertificatePin,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        if self.pin.matches(end_entity) {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(
                "server certificate does not match the pinned ssl_fingerprint".to_string(),
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::General(
            "pinned connections use TLS 1.3 only".to_string(),
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        let spki = subject_public_key_info(cert.as_ref()).ok_or_else(|| {
            rustls::Error::General(
                "server certificate has no readable subject public key".to_string(),
            )
        })?;
        verify_tls13_signature_with_raw_key(
            message,
            &SubjectPublicKeyInfoDer::from(spki),
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// The DER-encoded `subjectPublicKeyInfo` of an X.509 certificate of any
/// version, or `None` if `certificate` is not well-formed DER of that shape.
fn subject_public_key_info(certificate: &[u8]) -> Option<&[u8]> {
    const SEQUENCE: u8 = 0x30;
    const VERSION: u8 = 0xa0; // [0] EXPLICIT, present from v2 on

    let (certificate, _) = der_element(certificate, SEQUENCE)?;
    let (mut tbs, _) = der_element(certificate.contents, SEQUENCE)?;
    if tbs.contents.first() == Some(&VERSION) {
        tbs.contents = der_element(tbs.contents, VERSION)?.1;
    }
    // serialNumber, signature, issuer, validity, subject
    let mut rest = tbs.contents;
    for _ in 0..5 {
        rest = der_any(rest)?.1;
    }
    let (spki, _) = der_element(rest, SEQUENCE)?;
    Some(spki.encoded)
}

struct DerElement<'a> {
    encoded: &'a [u8],
    contents: &'a [u8],
}

fn der_element(input: &[u8], tag: u8) -> Option<(DerElement<'_>, &[u8])> {
    match der_any(input)? {
        (element, rest) if input[0] == tag => Some((element, rest)),
        _ => None,
    }
}

/// Splits the first DER element (single-byte tag, definite length) off `input`.
fn der_any(input: &[u8]) -> Option<(DerElement<'_>, &[u8])> {
    let first_length_byte = *input.get(1)?;
    let (length, header) = if first_length_byte < 0x80 {
        (usize::from(first_length_byte), 2)
    } else {
        let count = usize::from(first_length_byte & 0x7f);
        if count == 0 || count > std::mem::size_of::<usize>() {
            return None;
        }
        let bytes = input.get(2..2 + count)?;
        let length = bytes
            .iter()
            .fold(0usize, |length, byte| (length << 8) | usize::from(*byte));
        (length, 2 + count)
    };
    let end = header.checked_add(length)?;
    let encoded = input.get(..end)?;
    Some((
        DerElement {
            encoded,
            contents: &encoded[header..],
        },
        &input[end..],
    ))
}

#[cfg(test)]
mod tests {
    use base64::Engine;

    use super::*;

    const FINGERPRINT: &str = "2B:31:F0:0A:9C:44:D1:7E:65:B8:03:FE:12:34:56:78:9A:BC:DE:F0:01:23:45:67:89:AB:CD:EF:10:32:54:EA";

    // Two self-signed certificates for one P-256 key: an X.509 v1 certificate,
    // the version `quack_generate_keys()` produces, and a v3 one from
    // `openssl req -x509`. The fingerprint is what
    // `openssl x509 -noout -fingerprint -sha256` prints for the v1 certificate.
    const V1_CERT: &str = "MIIBDzCBtgIBATAKBggqhkjOPQQDAjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwHhcNMjYwOTI4MjAxNjA1WhcNMzYwOTI1MjAxNjA1WjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAASyjlZ2yGx5cVoj57+B6Ei3GFnNwkPfcD08AMj1fhzLENbu2BZstH9IfD9/LCHvSWgyYW7FcQEKkoARSLQyejMUMAoGCCqGSM49BAMCA0gAMEUCIQD5Zq/uMnCQSCAQ6OZcWh69Csmse3Pt6QjdY8OuiOlYFAIgfJ3tLNVcM0wragKjUsj46LsBM0BIs5QupUdyBb36XhU=";
    const V1_CERT_FINGERPRINT: &str = "19:4A:13:43:0E:E4:94:23:FB:22:C0:58:60:46:47:BA:26:9D:D7:8B:E4:32:FA:9C:CB:AE:B0:07:12:99:B0:69";
    const V3_CERT: &str = "MIIBfzCCASWgAwIBAgIUe5A7LGojUu+ZqCF+pUCEUTg6xjUwCgYIKoZIzj0EAwIwFTETMBEGA1UEAwwKcXVhY2stdGVzdDAeFw0yNjA5MjgyMDA5MzRaFw0zNjA5MjUyMDA5MzRaMBUxEzARBgNVBAMMCnF1YWNrLXRlc3QwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAASyjlZ2yGx5cVoj57+B6Ei3GFnNwkPfcD08AMj1fhzLENbu2BZstH9IfD9/LCHvSWgyYW7FcQEKkoARSLQyejMUo1MwUTAdBgNVHQ4EFgQUXDIXK/wxf8Lp7wFQgq2CrEyAUhUwHwYDVR0jBBgwFoAUXDIXK/wxf8Lp7wFQgq2CrEyAUhUwDwYDVR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNIADBFAiEA/heK1yWmVEeN+nBZ7k1HHDcBFg8/GdbLNTEC61Lmw5UCIDZKUeneEPpnSR2hO/sUFo+OkBTHaf5Kxa91dyNWmIVV";
    // `openssl pkey -pubout -outform der` for the key both certificates share.
    const SPKI: &str = "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEso5WdshseXFaI+e/gehItxhZzcJD33A9PADI9X4cyxDW7tgWbLR/SHw/fywh70loMmFuxXEBCpKAEUi0MnozFA==";

    fn der(base64: &str) -> Vec<u8> {
        base64::engine::general_purpose::STANDARD
            .decode(base64)
            .unwrap()
    }

    #[test]
    fn parses_duckdb_fingerprint_spellings() {
        let expected = CertificatePin::parse(FINGERPRINT).unwrap();
        let bare = FINGERPRINT.replace(':', "");
        for spelling in [
            bare.clone(),
            bare.to_lowercase(),
            format!("sha256:{FINGERPRINT}"),
            format!("SHA256:{bare}"),
            format!("  {} ", FINGERPRINT.replace(':', " ")),
        ] {
            assert_eq!(
                CertificatePin::parse(&spelling).unwrap(),
                expected,
                "{spelling}"
            );
        }
        assert_eq!(expected.0[0], 0x2B);
        assert_eq!(expected.0[31], 0xEA);
    }

    #[test]
    fn rejects_malformed_fingerprints() {
        for bad in [
            "",
            "2B:31",
            &FINGERPRINT.replace("EA", "EAFF"),
            &FINGERPRINT.replace("2B", "ZZ"),
            "sha1:2B31F00A9C44D17E65B803FE123456789ABCDEF00123456789ABCDEF103254EA",
        ] {
            let err = CertificatePin::parse(bad).unwrap_err();
            assert!(
                err.to_string().contains("invalid ssl_fingerprint"),
                "{bad}: {err}"
            );
        }
    }

    #[test]
    fn verifier_accepts_only_the_pinned_certificate() {
        let cert = CertificateDer::from(der(V1_CERT));
        let server_name = ServerName::try_from("quack.example").unwrap();
        let verify = |fingerprint: &str| {
            PinnedCertVerifier {
                pin: CertificatePin::parse(fingerprint).unwrap(),
                provider: Arc::new(rustls::crypto::ring::default_provider()),
            }
            .verify_server_cert(&cert, &[], &server_name, &[], UnixTime::now())
        };

        assert!(verify(V1_CERT_FINGERPRINT).is_ok());
        let err = verify(FINGERPRINT).unwrap_err();
        assert!(err.to_string().contains("ssl_fingerprint"), "{err}");
    }

    #[test]
    fn reads_the_public_key_of_v1_and_v3_certificates() {
        let spki = der(SPKI);
        for cert in [V1_CERT, V3_CERT] {
            assert_eq!(
                subject_public_key_info(&der(cert)),
                Some(spki.as_slice()),
                "{cert}"
            );
        }
    }

    #[test]
    fn malformed_certificates_have_no_public_key() {
        let cert = der(V1_CERT);
        assert_eq!(subject_public_key_info(&[]), None);
        assert_eq!(subject_public_key_info(&cert[..cert.len() / 2]), None);
        assert_eq!(subject_public_key_info(&der(SPKI)), None);
    }
}
