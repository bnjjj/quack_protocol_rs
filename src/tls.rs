use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls13_signature_with_raw_key};
use rustls::pki_types::{CertificateDer, ServerName, SubjectPublicKeyInfoDer, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use x509_parser::prelude::{FromDer, X509Certificate};

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
        let digits: String = digits.chars().filter(|c| !matches!(c, ':' | ' ')).collect();
        let mut bytes = [0; SHA256_LEN];
        hex::decode_to_slice(&digits, &mut bytes).map_err(|err| {
            QuackError::protocol(format!(
                "invalid ssl_fingerprint {fingerprint:?}: expected the 64 hex digits of the \
                 server certificate's SHA-256 fingerprint, optionally separated by colons ({err})"
            ))
        })?;
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
/// version, or `None` if `certificate` is not exactly one well-formed
/// certificate.
fn subject_public_key_info(certificate: &[u8]) -> Option<&[u8]> {
    match X509Certificate::from_der(certificate) {
        Ok(([], certificate)) => Some(certificate.tbs_certificate.subject_pki.raw),
        _ => None,
    }
}

/// A local HTTPS server for handshake tests, and certificates to serve.
#[cfg(test)]
pub(crate) mod test_server {
    use std::sync::Arc;

    use rustls::ServerConfig;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::server::{ClientHello, ResolvesServerCert};
    use rustls::sign::CertifiedKey;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;

    // Two self-signed certificates for one P-256 key: an X.509 v1 certificate,
    // the version `quack_generate_keys()` produces, and a v3 one from
    // `openssl req -x509`. The fingerprints are what
    // `openssl x509 -noout -fingerprint -sha256` prints for them.
    pub(crate) const V1_CERT: &[u8] = include_bytes!("../testdata/tls/v1-cert.der");
    pub(crate) const V1_CERT_FINGERPRINT: &str = "01:50:96:0A:D8:17:B9:61:A1:A9:08:AB:4A:96:D0:A0:10:C3:02:87:B1:DB:AC:EE:33:92:4F:15:C1:2B:53:B1";
    pub(crate) const V3_CERT: &[u8] = include_bytes!("../testdata/tls/v3-cert.der");
    pub(crate) const V3_CERT_FINGERPRINT: &str = "A5:B7:18:96:27:21:76:41:25:8E:6E:68:0E:1E:DF:F5:31:23:F0:45:CA:44:69:82:9A:86:42:EE:C6:5E:05:33";
    // The PKCS#8 private key both certificates are issued for.
    const KEY: &[u8] = include_bytes!("../testdata/tls/key.der");

    /// Serves `response`, a raw HTTP/1.1 response, over TLS 1.3 with
    /// `certificate` to every connection on a local port, and returns the port.
    pub(crate) async fn serve(certificate: &'static [u8], response: String) -> u16 {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        // Not `with_single_cert`: it checks the key against the certificate
        // through webpki, which rejects v1 certificates.
        let key = provider
            .key_provider
            .load_private_key(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(KEY)))
            .unwrap();
        let certified = CertifiedKey::new(vec![CertificateDer::from(certificate)], key);
        let config = ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(SingleCert(Arc::new(certified))));
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let acceptor = acceptor.clone();
                let response = response.clone();
                tokio::spawn(async move {
                    // Handshakes a test expects to fail end here.
                    let Ok(mut stream) = acceptor.accept(stream).await else {
                        return;
                    };
                    let mut request = Vec::new();
                    let mut buf = [0; 1024];
                    // Headers are enough: no test needs the body.
                    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                        match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => request.extend_from_slice(&buf[..n]),
                        }
                    }
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        port
    }

    #[derive(Debug)]
    struct SingleCert(Arc<CertifiedKey>);

    impl ResolvesServerCert for SingleCert {
        fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
            Some(Arc::clone(&self.0))
        }
    }

    pub(crate) fn ok_response() -> String {
        "HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok".to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::test_server::{self, V1_CERT, V1_CERT_FINGERPRINT, V3_CERT, V3_CERT_FINGERPRINT};
    use super::*;

    const FINGERPRINT: &str = "2B:31:F0:0A:9C:44:D1:7E:65:B8:03:FE:12:34:56:78:9A:BC:DE:F0:01:23:45:67:89:AB:CD:EF:10:32:54:EA";
    // `openssl pkey -pubout -outform der` for the key both test certificates share.
    const SPKI: &[u8] = include_bytes!("../testdata/tls/spki.der");

    async fn get_pinned(port: u16, fingerprint: &str) -> reqwest::Result<String> {
        let config = pinned_client_config(CertificatePin::parse(fingerprint).unwrap()).unwrap();
        reqwest::Client::builder()
            .use_preconfigured_tls(config)
            .build()?
            .get(format!("https://127.0.0.1:{port}/"))
            .send()
            .await?
            .text()
            .await
    }

    // The error and its sources, which is where reqwest keeps the TLS error.
    fn error_chain(err: &reqwest::Error) -> String {
        let mut chain = err.to_string();
        let mut source = err.source();
        while let Some(err) = source {
            chain.push_str(&format!(": {err}"));
            source = err.source();
        }
        chain
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
        let cert = CertificateDer::from(V1_CERT);
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
        for cert in [V1_CERT, V3_CERT] {
            assert_eq!(subject_public_key_info(cert), Some(SPKI));
        }
    }

    #[test]
    fn malformed_certificates_have_no_public_key() {
        assert_eq!(subject_public_key_info(&[]), None);
        assert_eq!(subject_public_key_info(&V1_CERT[..V1_CERT.len() / 2]), None);
        assert_eq!(subject_public_key_info(&[V1_CERT, &[0]].concat()), None);
        assert_eq!(subject_public_key_info(SPKI), None);
    }

    #[tokio::test]
    async fn pinned_handshake_succeeds_with_v1_and_v3_certificates() {
        for (cert, fingerprint) in [
            (V1_CERT, V1_CERT_FINGERPRINT),
            (V3_CERT, V3_CERT_FINGERPRINT),
        ] {
            let port = test_server::serve(cert, test_server::ok_response()).await;
            assert_eq!(
                get_pinned(port, fingerprint).await.unwrap(),
                "ok",
                "{fingerprint}"
            );
        }
    }

    #[tokio::test]
    async fn pinned_handshake_fails_for_another_certificate() {
        let port = test_server::serve(V1_CERT, test_server::ok_response()).await;
        for fingerprint in [FINGERPRINT, V3_CERT_FINGERPRINT] {
            let err = get_pinned(port, fingerprint).await.unwrap_err();
            let chain = error_chain(&err);
            assert!(
                chain.contains("does not match the pinned ssl_fingerprint"),
                "{chain}"
            );
        }
    }
}
