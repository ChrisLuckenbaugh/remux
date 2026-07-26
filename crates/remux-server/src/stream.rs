use crate::ResultExt;
use async_trait::async_trait;
use axum::{body::Body, http::HeaderMap, response::Response};
use axum_anyhow::ApiResult as Result;
use futures_util::TryStreamExt;
use std::{io, path::PathBuf, sync::OnceLock};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::AppState;

/// Shared client for proxying direct-play range requests to HTTP/torrent
/// upstreams. Reused across requests so a seek doesn't pay a fresh TCP+TLS
/// handshake every time — a single Infuse playback session can issue dozens
/// of range requests as it scrubs.
fn http_source_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .build()
            .expect("failed to build HTTP client")
    })
}

/// Typed representation of how a stream is accessed (transport mechanism).
///
/// Each variant maps to a [`StreamSource`] implementation via [`into_source`],
/// or for addon-owned streams, to the addon's [`AddonKind::serve_stream`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum StreamDescriptor {
    Http {
        url: String,
        /// HTTP request headers to send when fetching this stream.
        #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
        request_headers: std::collections::HashMap<String, String>,
        /// HTTP response headers to forward to the client.
        #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
        response_headers: std::collections::HashMap<String, String>,
    },
    Local(PathBuf),
    Rtsp {
        url: String,
    },
    Torrent {
        info_hash: String,
        /// Filename hint for multi-file torrents (matched by name).
        file_hint: Option<String>,
        /// Direct file index within the torrent (takes precedence over file_hint).
        file_idx: Option<usize>,
        /// Tracker announce URLs (populated from the stream's `sources`).
        #[serde(default)]
        trackers: Vec<String>,
    },
    Opendal {
        addon_id: Uuid,
        path: String,
    },
}

impl Default for StreamDescriptor {
    fn default() -> Self {
        Self::Http {
            url: String::new(),
            request_headers: Default::default(),
            response_headers: Default::default(),
        }
    }
}

impl StreamDescriptor {
    pub fn http(url: impl Into<String>) -> Self {
        Self::Http {
            url: url.into(),
            request_headers: Default::default(),
            response_headers: Default::default(),
        }
    }

    pub fn rtsp(url: impl Into<String>) -> Self {
        Self::Rtsp { url: url.into() }
    }

    /// Input URL/path for ffprobe and ffmpeg (server-side tools).
    /// `Local` → raw filesystem path. `Http` → URL as-is.
    /// `Torrent`/`Opendal` → our stream proxy, which resolves them on demand.
    pub fn server_input(&self, media_id: Uuid, port: u16) -> String {
        match self {
            Self::Http { url, .. } | Self::Rtsp { url } => url.clone(),
            Self::Local(path) => path
                .to_string_lossy()
                .into_owned(),
            Self::Torrent { .. } | Self::Opendal { .. } => {
                format!("http://127.0.0.1:{}/stream/{}", port, media_id)
            }
        }
    }

    /// URL to hand to the Jellyfin client for direct play.
    /// `Http` streams play directly. Everything else routes through our stream proxy
    /// (client can't access local FS; Torrent/Opendal need server-side resolution).
    pub fn client_url(&self, media_id: Uuid, server_base: &str) -> String {
        match self {
            Self::Http { url, .. } => url.clone(),
            _ => format!("{}/stream/{}", server_base.trim_end_matches('/'), media_id),
        }
    }

    /// The raw HTTP URL for `Http` variants, or `None` for everything else.
    pub fn as_http_url(&self) -> Option<&str> {
        match self {
            Self::Http { url, .. } => Some(url),
            _ => None,
        }
    }

    /// If this descriptor is owned by an addon (needs its credentials/config to
    /// serve), return the addon's ID so the endpoint can dispatch to
    /// `AddonKind::serve_stream` instead of `into_source`.
    pub fn addon_id(&self) -> Option<Uuid> {
        match self {
            Self::Opendal { addon_id, .. } => Some(*addon_id),
            _ => None,
        }
    }

    /// Instantiate the runtime service for self-contained variants.
    ///
    /// Returns an error for `Rtsp` (must go through the transcode path) and
    /// `Opendal` (must go through the owning addon) rather than panicking —
    /// callers reach this from request handlers, so a descriptor mismatch
    /// should surface as a normal error response, not take down the request.
    pub fn into_source(self) -> anyhow::Result<Box<dyn StreamSource>> {
        match self {
            Self::Http {
                url,
                request_headers,
                response_headers,
            } => Ok(Box::new(HttpSource {
                url,
                request_headers,
                response_headers,
            })),
            Self::Local(path) => Ok(Box::new(LocalSource { path })),
            Self::Torrent {
                info_hash,
                file_hint,
                file_idx,
                trackers,
            } => Ok(Box::new(TorrentSource {
                info_hash,
                file_hint,
                file_idx,
                trackers,
            })),
            Self::Rtsp { .. } => Err(anyhow::anyhow!(
                "Rtsp descriptors must be served through the transcode path"
            )),
            Self::Opendal { .. } => Err(anyhow::anyhow!(
                "Opendal descriptors must be served through their addon"
            )),
        }
    }
}

/// Combined stream descriptor and provider metadata stored in `db::Media.stream_info`.
///
/// Replaces the old split between `db::Media.url` (transport) and
/// `db::Media.provider_info` (Stremio metadata). All addons populate whichever
/// fields they have; the rest are `None` / empty.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct StreamInfo {
    pub descriptor: StreamDescriptor,
    /// Filename from the provider (e.g. "Movie.2021.1080p.BluRay.mkv").
    /// Used for resolution matching during probe fallback.
    pub filename: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    /// Addon that produced this stream (stamped by the service layer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// UUID of the addon that produced this stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub addon_id: Option<Uuid>,
    pub seeders: Option<i64>,
    pub size: Option<i64>,
    pub duration: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtitles: Vec<crate::sdks::stremio::Subtitle>,
    /// Catchup URL template from M3U `catchup-source` attribute.
    /// `{utc}` / `{utcend}` placeholders are substituted at playback time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catchup_source: Option<String>,
    /// Number of days of catchup available (`catchup-days` attribute).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catchup_days: Option<i64>,
    /// Usenet NZB GUID (the `id` query param from the nzb_url). Used for RemuxDB matching.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usenet_guid: Option<String>,
    /// Usenet indexer name (e.g. "NZBgeek"). Used for RemuxDB matching.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usenet_indexer: Option<String>,
    /// Raw NZB URL (from AIOStreams streamData). Used for RemuxDB matching via indexer_guid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nzb_url: Option<String>,
    /// Torrent info-hash for the source release (from AIOStreams streamData).
    /// Stored independently of the descriptor so debrid Http streams can match by hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub torrent_info_hash: Option<String>,
    /// File index within the torrent (from AIOStreams streamData).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub torrent_file_idx: Option<i32>,
    /// Pre-probed codec/bitrate metadata from the addon.
    /// Extracted into `db::Media.probe_data` on conversion; not persisted here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_data: Option<crate::api::MediaSourceInfo>,
}

impl StreamInfo {
    pub fn is_p2p(&self) -> bool {
        matches!(self.descriptor, StreamDescriptor::Torrent { .. })
    }

    pub fn resolution_tag(&self) -> Option<String> {
        let src = self
            .filename
            .as_deref()
            .or(self
                .name
                .as_deref())?;
        crate::db::min_screen_size(&hunch::hunch(src)).map(|s| s.to_owned())
    }
}

/// A runtime service that can serve stream bytes as an HTTP response.
///
/// Implemented by self-contained variants (`Http`, `Local`, `Torrent`).
/// Addon-owned variants (`Opendal`) are served through `AddonKind::serve_stream`.
#[async_trait]
pub trait StreamSource: Send + Sync {
    async fn serve(&self, state: &AppState, headers: &HeaderMap) -> Result<Response>;
}

pub struct HttpSource {
    pub url: String,
    pub request_headers: std::collections::HashMap<String, String>,
    pub response_headers: std::collections::HashMap<String, String>,
}

pub struct LocalSource {
    pub path: PathBuf,
}

/// Public trackers used as fallback when a torrent stream provides none.
/// Sourced from https://github.com/ngosang/trackerslist (trackers_best).
const DEFAULT_TRACKERS: &[&str] = &[
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://tracker.qu.ax:6969/announce",
    "udp://wepzone.net:6969/announce",
    "udp://tracker.srv00.com:6969/announce",
];

pub struct TorrentSource {
    pub info_hash: String,
    pub file_hint: Option<String>,
    pub file_idx: Option<usize>,
    pub trackers: Vec<String>,
}

impl TorrentSource {
    fn to_magnet(&self) -> String {
        let mut m = format!("magnet:?xt=urn:btih:{}", self.info_hash);
        let trackers: &[String] = &self.trackers;
        if trackers.is_empty() {
            for t in DEFAULT_TRACKERS {
                m.push_str(&format!("&tr={}", urlencoding::encode(t)));
            }
        } else {
            for t in trackers {
                m.push_str(&format!("&tr={}", urlencoding::encode(t)));
            }
        }
        if let Some(idx) = self.file_idx {
            m.push_str(&format!("&file_idx={}", idx));
        }
        if let Some(hint) = &self.file_hint {
            m.push_str(&format!("&file={}", urlencoding::encode(hint)));
        }
        m
    }
}

#[async_trait]
impl StreamSource for HttpSource {
    async fn serve(&self, _state: &AppState, headers: &HeaderMap) -> Result<Response> {
        let had_range = headers.contains_key(http::header::RANGE);
        let mut req = http_source_client().get(&self.url);
        if let Some(v) = headers.get(http::header::RANGE) {
            req = req.header(http::header::RANGE, v.clone());
        }
        for (k, v) in &self.request_headers {
            req = req.header(k.as_str(), v.as_str());
        }

        let upstream = req
            .send()
            .await
            .context_bad_request("upstream request failed")?;

        let status = upstream.status();
        let upstream_headers = upstream
            .headers()
            .clone();
        let body = Body::from_stream(
            upstream
                .bytes_stream()
                .map_err(io::Error::other),
        );

        let mut resp = Response::builder()
            .status(status)
            .body(body)
            .unwrap();
        let out = resp.headers_mut();
        for (k, v) in &upstream_headers {
            match k.as_str() {
                "content-length" | "content-type" | "accept-ranges"
                | "content-range" | "last-modified" => {
                    out.insert(k, v.clone());
                }
                _ => {}
            }
        }
        if !out.contains_key(http::header::CONTENT_TYPE) {
            out.insert(
                http::header::CONTENT_TYPE,
                http::HeaderValue::from_static("application/octet-stream"),
            );
        }
        // The upstream may honor Range without advertising Accept-Ranges (or
        // may not send Range-related headers at all on a 200). If it actually
        // answered a range request with 206, or we know it's byte-addressable
        // via Content-Range, tell the client it can seek.
        if !out.contains_key(http::header::ACCEPT_RANGES)
            && (status == http::StatusCode::PARTIAL_CONTENT
                || (had_range && out.contains_key(http::header::CONTENT_RANGE)))
        {
            out.insert(
                http::header::ACCEPT_RANGES,
                http::HeaderValue::from_static("bytes"),
            );
        }

        Ok(resp)
    }
}

#[async_trait]
impl StreamSource for LocalSource {
    async fn serve(&self, _state: &AppState, headers: &HeaderMap) -> Result<Response> {
        let file = tokio::fs::File::open(&self.path)
            .await
            .context_not_found("file not found")?;
        let metadata = file
            .metadata()
            .await
            .context_bad_request("failed to read file metadata")?;
        let file_size = metadata.len();
        let content_type = mime_from_path(&self.path);

        let spec = headers
            .get(http::header::RANGE)
            .and_then(|v| {
                v.to_str()
                    .ok()
            })
            .map(|range| parse_range(range, file_size))
            .unwrap_or(RangeSpec::Ignore);

        match spec {
            RangeSpec::Unsatisfiable => Ok(range_not_satisfiable(file_size)),
            RangeSpec::Satisfiable { start, end } => {
                let length = end - start + 1;

                let mut file = file;
                file.seek(std::io::SeekFrom::Start(start))
                    .await
                    .context_bad_request("seek failed")?;

                let body = Body::from_stream(ReaderStream::new(file.take(length)));

                Ok(Response::builder()
                    .status(http::StatusCode::PARTIAL_CONTENT)
                    .header(http::header::CONTENT_TYPE, content_type)
                    .header(http::header::CONTENT_LENGTH, length)
                    .header(http::header::ACCEPT_RANGES, "bytes")
                    .header(
                        http::header::CONTENT_RANGE,
                        format!("bytes {}-{}/{}", start, end, file_size),
                    )
                    .body(body)
                    .unwrap())
            }
            RangeSpec::Ignore => {
                let body = Body::from_stream(ReaderStream::new(file));

                Ok(Response::builder()
                    .status(http::StatusCode::OK)
                    .header(http::header::CONTENT_TYPE, content_type)
                    .header(http::header::CONTENT_LENGTH, file_size)
                    .header(http::header::ACCEPT_RANGES, "bytes")
                    .body(body)
                    .unwrap())
            }
        }
    }
}

#[async_trait]
impl StreamSource for TorrentSource {
    async fn serve(&self, state: &AppState, headers: &HeaderMap) -> Result<Response> {
        let resolved = state
            .ctx
            .torrent
            .resolve_url(&self.to_magnet())
            .await
            .context_bad_request("failed to resolve torrent")?;

        HttpSource {
            url: resolved,
            request_headers: Default::default(),
            response_headers: Default::default(),
        }
        .serve(state, headers)
        .await
    }
}

/// The outcome of interpreting a `Range` header against a known content length.
///
/// Parsing resolves the header all the way to a decision so callers cannot
/// accidentally build a partial response from bounds that do not hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeSpec {
    /// A satisfiable single range with inclusive bounds (`start <= end < size`).
    Satisfiable { start: u64, end: u64 },
    /// Syntactically valid but not satisfiable for this content length; the
    /// caller must answer 416 rather than clamping into a bogus range.
    Unsatisfiable,
    /// No usable range: the whole representation should be sent with 200.
    Ignore,
}

impl RangeSpec {
    /// Byte count covered by a satisfiable range.
    pub fn length(&self) -> Option<u64> {
        match *self {
            Self::Satisfiable { start, end } => Some(end - start + 1),
            _ => None,
        }
    }
}

/// Interprets a `Range` header against `file_size`.
///
/// Deviations from a naive parse, all required by RFC 9110 §14.1.2 and relied on
/// by AVFoundation clients (Infuse) that probe with edge and suffix ranges:
///
/// - A malformed or non-`bytes` range is *ignored* (full 200), never an error.
/// - A start at or past the end is unsatisfiable (416), never a clamped range.
/// - A zero-length representation cannot satisfy any range.
/// - Multi-range requests are answered with their first range only; a multipart
///   body buys nothing for players that seek linearly.
pub fn parse_range(range: &str, file_size: u64) -> RangeSpec {
    let Some(spec) = range
        .trim()
        .strip_prefix("bytes=")
    else {
        return RangeSpec::Ignore;
    };

    let first = spec
        .split(',')
        .next()
        .unwrap_or("")
        .trim();
    let Some((start_str, end_str)) = first.split_once('-') else {
        return RangeSpec::Ignore;
    };
    let (start_str, end_str) = (start_str.trim(), end_str.trim());

    // Nothing can be served from an empty representation.
    if file_size == 0 {
        return RangeSpec::Unsatisfiable;
    }
    let last = file_size - 1;

    // Suffix form `bytes=-N`: the final N bytes. `bytes=-0` requests nothing.
    if start_str.is_empty() {
        return match end_str.parse::<u64>() {
            Ok(0) => RangeSpec::Unsatisfiable,
            Ok(suffix) => RangeSpec::Satisfiable {
                start: file_size.saturating_sub(suffix),
                end: last,
            },
            Err(_) => RangeSpec::Ignore,
        };
    }

    let Ok(start) = start_str.parse::<u64>() else {
        return RangeSpec::Ignore;
    };
    let end = if end_str.is_empty() {
        last
    } else {
        match end_str.parse::<u64>() {
            Ok(end) => end.min(last),
            Err(_) => return RangeSpec::Ignore,
        }
    };

    if start > last || start > end {
        return RangeSpec::Unsatisfiable;
    }

    RangeSpec::Satisfiable { start, end }
}

/// 416 response carrying the content length so the client can retry correctly.
pub fn range_not_satisfiable(file_size: u64) -> Response {
    Response::builder()
        .status(http::StatusCode::RANGE_NOT_SATISFIABLE)
        .header(http::header::ACCEPT_RANGES, "bytes")
        .header(
            http::header::CONTENT_RANGE,
            format!("bytes */{}", file_size),
        )
        .body(Body::empty())
        .unwrap()
}

pub fn mime_from_path(path: &std::path::Path) -> &'static str {
    match path
        .extension()
        .and_then(|e| e.to_str())
    {
        Some("mp4") | Some("m4v") => "video/mp4",
        Some("mkv") => "video/x-matroska",
        Some("avi") => "video/x-msvideo",
        Some("mov") => "video/quicktime",
        Some("webm") => "video/webm",
        Some("ts") | Some("m2ts") | Some("mts") | Some("m2t") => "video/mp2t",
        Some("mpg") | Some("mpeg") | Some("m2v") | Some("vob") => "video/mpeg",
        Some("wmv") | Some("asf") => "video/x-ms-wmv",
        Some("flv") => "video/x-flv",
        Some("3gp") => "video/3gpp",
        Some("ogv") => "video/ogg",
        Some("mp3") => "audio/mpeg",
        Some("flac") => "audio/flac",
        Some("aac") => "audio/aac",
        Some("ogg") => "audio/ogg",
        Some("opus") => "audio/opus",
        Some("m4a") => "audio/mp4",
        Some("wav") => "audio/wav",
        _ => "application/octet-stream",
    }
}

/// Extract the `urn:btih:` info-hash from a magnet URI.
fn extract_btih(magnet: &str) -> Option<String> {
    url::Url::parse(magnet)
        .ok()?
        .query_pairs()
        .find(|(k, _)| k == "xt")
        .and_then(|(_, v)| {
            v.strip_prefix("urn:btih:")
                .map(|h| h.to_ascii_lowercase())
        })
}

fn extract_query_param(url: &str, param: &str) -> Option<String> {
    url::Url::parse(url)
        .ok()?
        .query_pairs()
        .find(|(k, _)| k == param)
        .map(|(_, v)| v.into_owned())
}

#[cfg(test)]
mod range_tests {
    use super::*;

    const SIZE: u64 = 5_000_000;

    #[test]
    fn full_range() {
        assert_eq!(
            parse_range("bytes=0-", SIZE),
            RangeSpec::Satisfiable {
                start: 0,
                end: SIZE - 1
            }
        );
    }

    #[test]
    fn bounded_range() {
        assert_eq!(
            parse_range("bytes=0-1", SIZE),
            RangeSpec::Satisfiable { start: 0, end: 1 }
        );
    }

    #[test]
    fn end_beyond_size_is_clamped() {
        assert_eq!(
            parse_range("bytes=0-99999999999", SIZE),
            RangeSpec::Satisfiable {
                start: 0,
                end: SIZE - 1
            }
        );
    }

    #[test]
    fn suffix_range() {
        assert_eq!(
            parse_range("bytes=-1000", SIZE),
            RangeSpec::Satisfiable {
                start: SIZE - 1000,
                end: SIZE - 1
            }
        );
    }

    #[test]
    fn suffix_larger_than_file_returns_whole_file() {
        assert_eq!(
            parse_range("bytes=-99999999999", SIZE),
            RangeSpec::Satisfiable {
                start: 0,
                end: SIZE - 1
            }
        );
    }

    #[test]
    fn suffix_zero_is_unsatisfiable() {
        assert_eq!(parse_range("bytes=-0", SIZE), RangeSpec::Unsatisfiable);
    }

    /// The bug this whole module exists to prevent: a start at or past EOF
    /// used to underflow `end - start + 1` into a ~2^64 Content-Length
    /// instead of failing cleanly.
    #[test]
    fn start_at_file_size_is_unsatisfiable_not_underflow() {
        assert_eq!(
            parse_range(&format!("bytes={}-", SIZE), SIZE),
            RangeSpec::Unsatisfiable
        );
    }

    #[test]
    fn start_past_file_size_is_unsatisfiable() {
        assert_eq!(
            parse_range(&format!("bytes={}-", SIZE + 1000), SIZE),
            RangeSpec::Unsatisfiable
        );
    }

    #[test]
    fn start_after_end_is_unsatisfiable() {
        assert_eq!(parse_range("bytes=100-50", SIZE), RangeSpec::Unsatisfiable);
    }

    #[test]
    fn zero_byte_file_is_always_unsatisfiable() {
        assert_eq!(parse_range("bytes=0-", 0), RangeSpec::Unsatisfiable);
        assert_eq!(parse_range("bytes=0-0", 0), RangeSpec::Unsatisfiable);
    }

    /// RFC 9110 §14.1.2: a malformed Range must be ignored, not rejected —
    /// the server falls back to serving the full 200 response.
    #[test]
    fn malformed_range_is_ignored() {
        assert_eq!(parse_range("nonsense", SIZE), RangeSpec::Ignore);
        assert_eq!(parse_range("bytes=", SIZE), RangeSpec::Ignore);
        assert_eq!(parse_range("bytes=abc-def", SIZE), RangeSpec::Ignore);
        assert_eq!(parse_range("items=0-1", SIZE), RangeSpec::Ignore);
    }

    /// A multi-range request is answered with its first range rather than
    /// rejected outright.
    #[test]
    fn multi_range_uses_first_range() {
        assert_eq!(
            parse_range("bytes=0-1,4096-8191", SIZE),
            RangeSpec::Satisfiable { start: 0, end: 1 }
        );
    }

    #[test]
    fn length_helper() {
        assert_eq!(
            RangeSpec::Satisfiable { start: 0, end: 99 }.length(),
            Some(100)
        );
        assert_eq!(RangeSpec::Unsatisfiable.length(), None);
        assert_eq!(RangeSpec::Ignore.length(), None);
    }
}
