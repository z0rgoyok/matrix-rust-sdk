// Copyright 2026 Tich Connect
// SPDX-License-Identifier: Apache-2.0

//! Native implementation of connect-transport v1. Bodies and credentials only
//! travel in HTTPS headers, never URLs. Matrix encryption remains in the SDK.

use std::{
    collections::BTreeMap,
    sync::OnceLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use eyeball::SharedObservable;
use futures_util::{StreamExt, TryStreamExt, stream};
use http::{Method, Request, Response};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::TransmissionProgress;
use crate::error::HttpError;

const ORIGIN: &str = "https://connect.tich.app";
const MAX_BODY: usize = 50 * 1024 * 1024;
const MAX_ENVELOPE: usize = 12 * 1024 * 1024;
const CHUNK_SIZE: usize = 3072;
type Result<T> = std::result::Result<T, HttpError>;

fn failure(message: &'static str) -> HttpError {
    HttpError::ConnectTransport(message)
}

pub(super) fn handles(request: &Request<Bytes>) -> bool {
    let uri = request.uri();
    uri.scheme_str() == Some("https")
        && uri.authority().is_some_and(|a| a.as_str() == "connect.tich.app")
        && [Method::POST, Method::PUT, Method::PATCH, Method::DELETE].contains(request.method())
        && (uri.path().starts_with("/_matrix/client/") || uri.path().starts_with("/_matrix/media/"))
}

fn nonce() -> Result<String> {
    let mut bytes = [0; 32];
    getrandom::fill(&mut bytes).map_err(|_| failure("random source unavailable"))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

fn client() -> Result<&'static reqwest::Client> {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    if let Some(client) = CLIENT.get() {
        return Ok(client);
    }
    let built = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|_| failure("HTTP client unavailable"))?;
    let _ = CLIENT.set(built);
    CLIENT.get().ok_or_else(|| failure("HTTP client unavailable"))
}

// The same operation ID is reused on packet retries. A timeout or dropped
// response can never turn into a second non-idempotent Matrix write here.
async fn packet(
    origin: &str,
    id: &str,
    action: &str,
    headers: &[(&str, String)],
) -> Result<(u16, Bytes)> {
    for attempt in 0..3 {
        let mut request = client()?
            .get(format!("{origin}/_connect/v1/{action}?n={}", nonce()?))
            .header("X-Connect-Version", "1")
            .header("X-Connect-Id", id)
            .header("Cache-Control", "no-store");
        for (name, value) in headers {
            request = request.header(*name, value);
        }
        let response = match request.send().await {
            Ok(response) => response,
            Err(_) if attempt < 2 => continue,
            Err(_) => return Err(failure("packet unavailable; outcome may be unknown")),
        };
        if response.headers().get("x-connect-id").and_then(|v| v.to_str().ok()) != Some(id) {
            return Err(failure("response identity mismatch"));
        }
        let status = response.status().as_u16();
        let mut body = Vec::new();
        let mut chunks = response.bytes_stream();
        let mut interrupted = false;
        while let Some(chunk) = chunks.next().await {
            match chunk {
                Ok(chunk) if body.len() + chunk.len() <= MAX_ENVELOPE => {
                    body.extend_from_slice(&chunk)
                }
                Ok(_) => return Err(failure("response exceeds limit")),
                Err(_) => {
                    interrupted = true;
                    break;
                }
            }
        }
        if !interrupted {
            return Ok((status, body.into()));
        }
    }
    Err(failure("response interrupted; outcome may be unknown"))
}

#[derive(Deserialize)]
struct Ack {
    id: String,
    index: Option<usize>,
}

fn acknowledge(id: &str, expected_index: Option<usize>, status: u16, body: &[u8]) -> Result<()> {
    let ack: Ack = serde_json::from_slice(body).map_err(|_| failure("invalid acknowledgement"))?;
    if !(200..300).contains(&status) || ack.id != id || ack.index != expected_index {
        return Err(failure("acknowledgement mismatch"));
    }
    Ok(())
}

#[derive(Deserialize)]
struct Envelope {
    id: String,
    status: u16,
    body: String,
    headers: BTreeMap<String, String>,
}

fn decode(id: &str, body: &[u8]) -> Result<Response<Bytes>> {
    let envelope: Envelope =
        serde_json::from_slice(body).map_err(|_| failure("invalid envelope"))?;
    if envelope.id != id || !(200..600).contains(&envelope.status) {
        return Err(failure("invalid Matrix response"));
    }
    let bytes =
        URL_SAFE_NO_PAD.decode(envelope.body).map_err(|_| failure("invalid response body"))?;
    if [204, 205, 304].contains(&envelope.status) && !bytes.is_empty() {
        return Err(failure("unexpected response body"));
    }
    let mut response =
        Response::builder().status(envelope.status).header("cache-control", "no-store");
    for (name, value) in envelope.headers {
        if ["content-type", "retry-after", "www-authenticate"]
            .contains(&name.to_ascii_lowercase().as_str())
        {
            if value.len() > 4096 {
                return Err(failure("response header exceeds limit"));
            }
            response = response.header(name, value);
        }
    }
    response.body(bytes.into()).map_err(|_| failure("invalid response headers"))
}

// Cleanup also runs when the SDK cancels sync or an upload. The gateway TTL is
// the fallback when the process is terminated (including an expired NSE).
struct Cleanup {
    origin: String,
    id: String,
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        let origin = self.origin.clone();
        let id = self.id.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = tokio::time::timeout(
                    Duration::from_secs(3),
                    packet(&origin, &id, "cancel", &[]),
                )
                .await;
            });
        }
    }
}

pub(super) async fn send(
    request: &Request<Bytes>,
    timeout: Option<Duration>,
    progress: SharedObservable<TransmissionProgress>,
) -> Result<Response<Bytes>> {
    send_to(ORIGIN, request, timeout, progress).await
}

async fn send_to(
    origin: &str,
    request: &Request<Bytes>,
    timeout: Option<Duration>,
    progress: SharedObservable<TransmissionProgress>,
) -> Result<Response<Bytes>> {
    if request.body().len() > MAX_BODY {
        return Err(failure("file exceeds 50 MiB"));
    }
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| failure("invalid clock"))?
        .as_millis();
    let id = format!("{millis:016x}-{}", nonce()?);
    let mut headers = BTreeMap::new();
    for name in ["authorization", "content-type"] {
        if let Some(value) = request.headers().get(name) {
            headers.insert(name, value.to_str().map_err(|_| failure("invalid request header"))?);
        }
    }
    let meta = serde_json::to_vec(&serde_json::json!({
        "method": request.method().as_str(),
        "path": request.uri().path_and_query().map_or("/", |path| path.as_str()),
        "headers": headers, "length": request.body().len(),
        "sha256": Sha256::digest(request.body()).iter().map(|b| format!("{b:02x}")).collect::<String>(),
    }))
    .map_err(|_| failure("invalid request metadata"))?;
    if meta.len() > 4096 {
        return Err(failure("request metadata exceeds limit"));
    }
    let _cleanup = Cleanup { origin: origin.to_owned(), id: id.clone() };
    let deadline = if request.body().len() > 1024 * 1024 {
        Duration::from_secs(240)
    } else {
        timeout.unwrap_or(Duration::from_secs(30)).min(Duration::from_secs(240))
    };
    tokio::time::timeout(deadline, async {
        let (status, body) =
            packet(origin, &id, "begin", &[("X-Connect-Meta", URL_SAFE_NO_PAD.encode(meta))])
                .await?;
        acknowledge(&id, None, status, &body)?;
        progress.update(|p| p.total += request.body().len());
        let uploads: Vec<_> = (0..request.body().len())
            .step_by(CHUNK_SIZE)
            .enumerate()
            .map(|(index, offset)| {
                upload_chunk(
                    origin.to_owned(),
                    id.clone(),
                    index,
                    request.body().slice(offset..(offset + CHUNK_SIZE).min(request.body().len())),
                    progress.clone(),
                )
            })
            .collect();
        stream::iter(uploads).buffer_unordered(4).try_collect::<Vec<_>>().await?;
        loop {
            let (status, body) = packet(origin, &id, "execute", &[]).await?;
            match status {
                202 => tokio::time::sleep(Duration::from_millis(250)).await,
                429 => tokio::time::sleep(Duration::from_millis(500)).await,
                200..=299 => return decode(&id, &body),
                _ => return Err(failure("execution failed; outcome may be unknown")),
            }
        }
    })
    .await
    .map_err(|_| failure("deadline exceeded; outcome may be unknown"))?
}

async fn upload_chunk(
    origin: String,
    id: String,
    index: usize,
    chunk: Bytes,
    progress: SharedObservable<TransmissionProgress>,
) -> Result<()> {
    let (status, body) = packet(
        &origin,
        &id,
        "chunk",
        &[
            ("X-Connect-Index", index.to_string()),
            ("X-Connect-Chunk", URL_SAFE_NO_PAD.encode(&chunk)),
        ],
    )
    .await?;
    acknowledge(&id, Some(index), status, &body)?;
    progress.update(|p| p.current += chunk.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    #[test]
    fn routing_is_scoped_to_connect_matrix_mutations() {
        for (method, url, expected) in [
            ("POST", "https://connect.tich.app/_matrix/client/v3/login", true),
            ("PUT", "https://connect.tich.app/_matrix/media/v3/upload", true),
            ("GET", "https://connect.tich.app/_matrix/client/versions", false),
            ("POST", "https://other.example/_matrix/client/v3/login", false),
            ("POST", "http://connect.tich.app/_matrix/client/v3/login", false),
            ("POST", "https://connect.tich.app/_synapse/admin/v1/users", false),
        ] {
            let request = Request::builder().method(method).uri(url).body(Bytes::new()).unwrap();
            assert_eq!(handles(&request), expected);
        }
    }

    #[test]
    fn invalid_envelopes_cannot_masquerade_as_success() {
        for value in [
            serde_json::json!({"id":"wrong","status":200,"body":"","headers":{}}),
            serde_json::json!({"id":"test","status":99,"body":"","headers":{}}),
            serde_json::json!({"id":"test","status":200,"body":"!","headers":{}}),
            serde_json::json!({"id":"test","status":204,"body":"YQ","headers":{}}),
        ] {
            assert!(decode("test", &serde_json::to_vec(&value).unwrap()).is_err());
        }
        assert!(matches!(
            failure("unknown outcome").retry_kind(),
            crate::error::RetryKind::Permanent
        ));
    }

    fn reply(request: &wiremock::Request) -> ResponseTemplate {
        let id = request.headers["x-connect-id"].to_str().unwrap();
        let payload = match request.url.path() {
            "/_connect/v1/begin" => serde_json::json!({"id":id}),
            "/_connect/v1/chunk" => serde_json::json!({"id":id,
                "index":request.headers["x-connect-index"].to_str().unwrap().parse::<usize>().unwrap()}),
            "/_connect/v1/execute" => serde_json::json!({"id":id,"status":401,
                "body": URL_SAFE_NO_PAD.encode(br#"{"errcode":"M_UNAUTHORIZED"}"#),
                "headers":{"content-type":"application/json", "set-cookie":"never-forward"}}),
            _ => serde_json::json!({"id":id}),
        };
        ResponseTemplate::new(200).insert_header("X-Connect-Id", id).set_body_json(payload)
    }

    #[tokio::test]
    async fn upload_reassembles_body_and_preserves_matrix_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET")).respond_with(reply).mount(&server).await;
        let body = Bytes::from(vec![0x9f; CHUNK_SIZE * 5 + 17]);
        let request = Request::builder()
            .method("POST")
            .uri("https://connect.tich.app/_matrix/media/v3/upload?filename=test.bin")
            .header("authorization", "Bearer test-only")
            .header("content-type", "application/octet-stream")
            .body(body.clone())
            .unwrap();
        let response = send_to(
            &server.uri(),
            &request,
            None,
            SharedObservable::new(TransmissionProgress::default()),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), 401);
        assert_eq!(response.headers()["content-type"], "application/json");
        assert!(!response.headers().contains_key("set-cookie"));
        let requests = server.received_requests().await.unwrap();
        let begin = requests.iter().find(|r| r.url.path().ends_with("/begin")).unwrap();
        let meta: serde_json::Value = serde_json::from_slice(
            &URL_SAFE_NO_PAD.decode(begin.headers["x-connect-meta"].as_bytes()).unwrap(),
        )
        .unwrap();
        assert_eq!(meta["method"], "POST");
        assert_eq!(meta["length"], body.len());
        assert_eq!(meta["headers"]["authorization"], "Bearer test-only");
        assert_eq!(meta["path"], "/_matrix/media/v3/upload?filename=test.bin");
        let chunks: BTreeMap<usize, Vec<u8>> = requests
            .iter()
            .filter(|r| r.url.path().ends_with("/chunk"))
            .map(|r| {
                (
                    r.headers["x-connect-index"].to_str().unwrap().parse().unwrap(),
                    URL_SAFE_NO_PAD.decode(r.headers["x-connect-chunk"].as_bytes()).unwrap(),
                )
            })
            .collect();
        assert_eq!(chunks.into_values().flatten().collect::<Vec<_>>(), body);
        for packet in &requests {
            assert_eq!(packet.method.as_str(), "GET");
            assert_eq!(packet.headers["x-connect-id"], begin.headers["x-connect-id"]);
            assert!(!packet.url.as_str().contains("test-only"));
            assert!(packet.body.is_empty());
        }
    }

    #[tokio::test]
    async fn uncertain_execution_is_not_replayed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(|request: &wiremock::Request| {
                if request.url.path().ends_with("/execute") {
                    ResponseTemplate::new(502).insert_header(
                        "X-Connect-Id",
                        request.headers["x-connect-id"].to_str().unwrap(),
                    )
                } else {
                    reply(request)
                }
            })
            .mount(&server)
            .await;
        let request = Request::builder()
            .method("POST")
            .uri("https://connect.tich.app/_matrix/client/v3/createRoom")
            .body(Bytes::from("{}"))
            .unwrap();
        let error = send_to(
            &server.uri(),
            &request,
            None,
            SharedObservable::new(TransmissionProgress::default()),
        )
        .await
        .unwrap_err();
        assert!(matches!(error.retry_kind(), crate::error::RetryKind::Permanent));
        assert_eq!(
            server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .filter(|r| r.url.path().ends_with("/execute"))
                .count(),
            1
        );
    }

    #[tokio::test]
    #[ignore = "requires the public Tich Connect CDN; no account or credentials"]
    async fn live_cdn_preserves_login_validation_error() {
        let request = Request::builder()
            .method("POST")
            .uri("https://connect.tich.app/_matrix/client/v3/login")
            .header("content-type", "application/json")
            .body(Bytes::from("{}"))
            .unwrap();
        let response = send(&request, None, SharedObservable::new(TransmissionProgress::default()))
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
        let body: serde_json::Value = serde_json::from_slice(response.body()).unwrap();
        assert!(body["errcode"].as_str().unwrap().starts_with("M_"));
    }

    #[tokio::test]
    async fn wrong_identity_stops_before_execution() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("X-Connect-Id", "wrong")
                    .set_body_json(serde_json::json!({"id":"wrong"})),
            )
            .mount(&server)
            .await;
        let request = Request::builder()
            .method("POST")
            .uri("https://connect.tich.app/_matrix/client/v3/createRoom")
            .body(Bytes::from("{}"))
            .unwrap();
        assert!(
            send_to(
                &server.uri(),
                &request,
                None,
                SharedObservable::new(TransmissionProgress::default())
            )
            .await
            .is_err()
        );
        assert!(
            !server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .any(|r| r.url.path().ends_with("/execute"))
        );
    }
}
