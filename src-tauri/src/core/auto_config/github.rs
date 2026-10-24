//! GitHub search and fetch for candidate Clash/Mihomo configs.
//!
//! Content is returned as text, never as a URL to subscribe to. The caller must store it as a
//! frozen `itype: "local"` profile: a `remote` profile would let the subscription timer in
//! `core::timer` re-pull the file later and silently replace whatever this module validated.
//!
//! Every request goes direct instead of through the core's mixed port. The agent only runs after
//! the connectivity probe proved the local link is up and the proxy path is down, so the proxy is
//! the one route already known to be broken.

use super::AgentFailure;
use crate::config::Config;
use crate::utils::network::{NetworkManager, ProxyType};
use anyhow::{Context as _, Result};
use backon::{ExponentialBuilder, Retryable as _};
use clash_verge_logging::{Type, logging};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderMap, HeaderValue};
use serde::Deserialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const API_BASE: &str = "https://api.github.com";
const RAW_BASE: &str = "https://raw.githubusercontent.com";
const TOKEN_ENV: &str = "GITHUB_TOKEN";
const REQUEST_TIMEOUT_SECS: u64 = 20;
const MAX_SEARCH_RETRIES: usize = 3;
/// Candidates are text configs; a larger body is not something worth applying.
const MAX_CONFIG_BYTES: usize = 2 * 1024 * 1024;
/// `code_search` allows 10 requests per minute even when authenticated, so a window can be almost
/// a minute from reopening. A cap below that would give up before the retry could succeed.
const MAX_RATE_LIMIT_WAIT: Duration = Duration::from_secs(90);

const GITHUB_JSON: &str = "application/vnd.github+json";
const GITHUB_RAW: &str = "application/vnd.github.raw";

const MAX_CANDIDATES: usize = 8;
const REPOS_PER_QUERY: usize = 3;

const SEARCH_QUERIES: &[&str] = &["clash config yaml", "mihomo config", "clash subscription"];

/// Probed in order on the unauthenticated path, which has no code search to point at real files.
const CANDIDATE_FILENAMES: &[&str] = &[
    "config.yaml",
    "clash.yaml",
    "mihomo.yaml",
    "config.yml",
    "clash.yml",
    "mihomo.yml",
];

/// A config fetched from GitHub, frozen at fetch time.
#[derive(Debug, Clone)]
pub struct ConfigCandidate {
    /// Where it came from, for logs: `owner/repo/path`.
    pub source: String,
    pub content: String,
}

/// A GitHub answer that names the cause, so the panel can show one of [`AgentFailure`] instead of
/// this text. Everything here keeps the English wording for the log; only the code crosses the IPC.
#[derive(Debug)]
struct Blocked {
    failure: AgentFailure,
    message: std::string::String,
}

impl std::fmt::Display for Blocked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Blocked {}

fn blocked(failure: AgentFailure, message: impl Into<std::string::String>) -> anyhow::Error {
    anyhow::Error::new(Blocked {
        failure,
        message: message.into(),
    })
}

/// The cause a failed search should show the user. A plain transport error says nothing more than
/// that GitHub could not be reached, which is also the honest reading of a body we could not parse.
pub fn failure_of(error: &anyhow::Error) -> AgentFailure {
    error
        .downcast_ref::<Blocked>()
        .map_or(AgentFailure::NetworkBlocked, |blocked| blocked.failure)
}

impl AgentFailure {
    /// Only an exhausted quota is worth asking about again; GitHub repeats every other answer until
    /// the user changes something, and retrying those just burns quota.
    const fn is_retryable(self) -> bool {
        matches!(self, Self::RateLimited)
    }
}

#[derive(Debug, Deserialize)]
struct CodeSearchPage {
    #[serde(default)]
    items: Vec<CodeSearchItem>,
}

#[derive(Debug, Deserialize)]
struct CodeSearchItem {
    path: String,
    /// Contents API URL with the matching commit already filled in as `ref`.
    url: String,
    repository: CodeSearchRepo,
}

#[derive(Debug, Deserialize)]
struct CodeSearchRepo {
    full_name: String,
}

#[derive(Debug, Deserialize)]
struct RepoSearchPage {
    #[serde(default)]
    items: Vec<RepoSearchItem>,
}

/// Unlike the repository object inside a code search hit, this one carries `default_branch`.
#[derive(Debug, Deserialize)]
struct RepoSearchItem {
    full_name: String,
    default_branch: String,
}

/// Searches GitHub for candidate configs and returns their contents.
///
/// Code search answers with the exact files that matched, but GitHub rejects it without
/// credentials, so an absent token degrades to repository search plus filename probing.
pub async fn search_candidates() -> Result<Vec<ConfigCandidate>> {
    match github_token().await {
        Some(token) => search_by_code(token).await,
        None => {
            logging!(
                warn,
                Type::Network,
                "no GitHub token configured; code search requires authentication, degrading to repository search"
            );
            search_by_repository().await
        }
    }
}

/// Reads the token from the `github_token` setting first, then the `GITHUB_TOKEN` environment
/// variable, and otherwise returns `None` so the caller degrades. Never logged, and never
/// defaulted to something else.
async fn github_token() -> Option<std::string::String> {
    let configured = Config::verge().await.latest_arc().github_token.clone();
    configured
        .as_deref()
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            std::env::var(TOKEN_ENV)
                .ok()
                .map(|token| token.trim().to_owned())
                .filter(|token| !token.is_empty())
        })
}

/// `token` is owned rather than borrowed on purpose: an `async fn` with a reference parameter
/// captures that lifetime in its return type, and the spawned agent task then needs `Send` to hold
/// for every lifetime, which rustc cannot prove.
async fn search_by_code(token: String) -> Result<Vec<ConfigCandidate>> {
    let mut candidates = Vec::new();
    let mut failure: Option<anyhow::Error> = None;

    // By value: a `&&str` held across these awaits makes the future's `Send` bound higher-ranked.
    for &query in SEARCH_QUERIES {
        if candidates.len() >= MAX_CANDIDATES {
            break;
        }
        let url = search_url("code", &format!("{query} extension:yaml"));
        let page: CodeSearchPage = match fetch_json(&url, Some(token.as_str())).await {
            Ok(page) => page,
            Err(error) => {
                logging!(warn, Type::Network, "code search for {query:?} failed: {error:#}");
                // A later query may still work; if none does, this is what the user has to fix.
                failure.get_or_insert(error);
                continue;
            }
        };

        for item in page.items {
            if candidates.len() >= MAX_CANDIDATES {
                break;
            }
            let source = format!("{}/{}", item.repository.full_name, item.path);
            match fetch_content(&item.url, Some(token.as_str())).await {
                Ok(content) => candidates.push(ConfigCandidate { source, content }),
                Err(error) => {
                    logging!(debug, Type::Network, "skipping {}: {error:#}", source);
                }
            }
        }
    }

    match (candidates.is_empty(), failure) {
        (true, Some(error)) => Err(error),
        _ => Ok(candidates),
    }
}

async fn search_by_repository() -> Result<Vec<ConfigCandidate>> {
    let mut candidates = Vec::new();
    let mut failure: Option<anyhow::Error> = None;

    // By value: a `&&str` held across these awaits makes the future's `Send` bound higher-ranked.
    for &query in SEARCH_QUERIES {
        if candidates.len() >= MAX_CANDIDATES {
            break;
        }
        let url = search_url("repositories", query);
        let page: RepoSearchPage = match fetch_json(&url, None).await {
            Ok(page) => page,
            Err(error) => {
                logging!(warn, Type::Network, "repository search for {query:?} failed: {error:#}");
                failure.get_or_insert(error);
                continue;
            }
        };

        for repo in page.items.into_iter().take(REPOS_PER_QUERY) {
            if candidates.len() >= MAX_CANDIDATES {
                break;
            }
            if let Some(candidate) = probe_repo_files(&repo).await {
                candidates.push(candidate);
            }
        }
    }

    match (candidates.is_empty(), failure) {
        (true, Some(error)) => Err(error),
        _ => Ok(candidates),
    }
}

/// Tries the conventional config filenames and keeps the first that answers. Most probes 404, so a
/// miss is the expected outcome rather than an error worth surfacing.
async fn probe_repo_files(repo: &RepoSearchItem) -> Option<ConfigCandidate> {
    for filename in CANDIDATE_FILENAMES {
        let url = format!("{RAW_BASE}/{}/{}/{}", repo.full_name, repo.default_branch, filename);
        // Routed through NetworkManager because raw content needs no credentials, and it already
        // enforces a byte cap and falls back to static webpki roots on TLS failures.
        match NetworkManager::new()
            .get_bytes(&url, ProxyType::None, Some(REQUEST_TIMEOUT_SECS), MAX_CONFIG_BYTES)
            .await
        {
            Ok(bytes) => {
                let source = format!("{}/{filename}", repo.full_name);
                return match std::string::String::from_utf8(bytes) {
                    Ok(content) => Some(ConfigCandidate { source, content }),
                    Err(error) => {
                        logging!(debug, Type::Network, "{source} is not valid UTF-8: {error}");
                        None
                    }
                };
            }
            Err(error) => {
                logging!(debug, Type::Network, "{}/{}: {}", repo.full_name, filename, error);
            }
        }
    }

    None
}

fn search_url(endpoint: &str, query: &str) -> std::string::String {
    let encoded = utf8_percent_encode(query, NON_ALPHANUMERIC);
    format!("{API_BASE}/search/{endpoint}?q={encoded}&per_page={MAX_CANDIDATES}")
}

async fn fetch_json<T: for<'de> Deserialize<'de>>(url: &str, token: Option<&str>) -> Result<T> {
    let body = api_get(url, token, GITHUB_JSON).await?;
    serde_json::from_str(&body).map_err(|error| {
        blocked(
            AgentFailure::NetworkBlocked,
            format!("{url} returned an unparsable body: {error}"),
        )
    })
}

/// Fetches file content through the contents API, which the code search hit already pins to a
/// commit via its `ref` query parameter.
async fn fetch_content(url: &str, token: Option<&str>) -> Result<std::string::String> {
    api_get(url, token, GITHUB_RAW).await
}

/// One GitHub API call, with the rate-limit wait applied before the retry layer sees the failure.
async fn api_get(url: &str, token: Option<&str>, accept: &str) -> Result<std::string::String> {
    let backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_secs(1))
        .with_max_delay(Duration::from_secs(60))
        .with_factor(2.0)
        .with_max_times(MAX_SEARCH_RETRIES);

    (|| async { request_once(url, token, accept).await })
        .retry(backoff)
        .when(|error: &anyhow::Error| match error.downcast_ref::<Blocked>() {
            Some(blocked) => blocked.failure.is_retryable(),
            None => true,
        })
        .await
}

async fn request_once(url: &str, token: Option<&str>, accept: &str) -> Result<std::string::String> {
    let client = build_client()?;
    let mut request = client.get(url).header(ACCEPT, HeaderValue::from_str(accept)?);
    if let Some(token) = token {
        let value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
            blocked(
                AgentFailure::TokenInvalid,
                "the GitHub token contains characters a header cannot carry",
            )
        })?;
        request = request.header(AUTHORIZATION, value);
    }

    let response = request.send().await.context("GitHub request failed")?;
    let status = response.status();
    let headers = response.headers().clone();

    if status.is_success() {
        return read_capped(response).await;
    }
    if is_rate_limited(status.as_u16(), &headers) {
        // Waited here rather than left to the backoff, which cannot know when the window reopens.
        // The backoff still adds its own delay on top; over-waiting is harmless in a background
        // task, while under-waiting just burns the next quota.
        let wait = rate_limit_wait(&headers).unwrap_or(Duration::from_secs(1));
        logging!(
            warn,
            Type::Network,
            "GitHub rate limit hit on {url} (status {status}); waiting {}s before retrying",
            wait.as_secs()
        );
        tokio::time::sleep(wait).await;
        return Err(blocked(
            AgentFailure::RateLimited,
            format!("GitHub rate limit hit on {url}"),
        ));
    }
    if matches!(status.as_u16(), 401 | 403 | 404 | 422) {
        // A rejected credential is the one answer the user can fix by hand, and GitHub sends it as
        // 403 as often as 401, so it is only distinguishable from "nothing here" by whether we
        // authenticated at all.
        let failure = match status.as_u16() {
            401 => AgentFailure::TokenInvalid,
            403 if token.is_some() => AgentFailure::TokenInvalid,
            _ => AgentFailure::NoUsableConfig,
        };
        return Err(blocked(failure, format!("{url} answered {status}")));
    }

    anyhow::bail!("{url} answered {status}")
}

fn build_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .tls_backend_rustls()
        .no_proxy()
        .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
        .connect_timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
        .user_agent(format!("clash-verge/v{}", env!("CARGO_PKG_VERSION")))
        .build()
        .context("failed to build the GitHub API client")
}

/// Streams the body so an oversized file is abandoned partway instead of buffered whole.
async fn read_capped(mut response: reqwest::Response) -> Result<std::string::String> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.context("failed to read the GitHub response")? {
        if body.len() + chunk.len() > MAX_CONFIG_BYTES {
            anyhow::bail!("response exceeded the {MAX_CONFIG_BYTES} byte limit");
        }
        body.extend_from_slice(&chunk);
    }

    std::string::String::from_utf8(body).context("GitHub returned a body that is not valid UTF-8")
}

/// GitHub reports an exhausted quota as 403 with the counters zeroed, and uses 429 for abuse
/// detection; both mean the same thing here.
fn is_rate_limited(status: u16, headers: &HeaderMap) -> bool {
    if status == 429 {
        return true;
    }
    if status != 403 {
        return false;
    }

    header_u64(headers, "x-ratelimit-remaining") == Some(0) || headers.contains_key("retry-after")
}

/// `Retry-After` counts seconds; `X-RateLimit-Reset` is the Unix time the window reopens. Both are
/// consulted because GitHub sends one or the other depending on which limit tripped.
fn rate_limit_wait(headers: &HeaderMap) -> Option<Duration> {
    let capped = |seconds: u64| Duration::from_secs(seconds.min(MAX_RATE_LIMIT_WAIT.as_secs()));

    if let Some(seconds) = header_u64(headers, "retry-after") {
        return Some(capped(seconds));
    }

    let reset = header_u64(headers, "x-ratelimit-reset")?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    let wait = reset.saturating_sub(now);
    (wait > 0).then(|| capped(wait))
}

fn header_u64(headers: &HeaderMap, name: &str) -> Option<u64> {
    headers.get(name)?.to_str().ok()?.trim().parse().ok()
}

#[cfg(test)]
#[allow(clippy::expect_used, reason = "tests assert by panicking")]
mod tests {
    use super::{MAX_RATE_LIMIT_WAIT, rate_limit_wait};
    use reqwest::header::HeaderMap;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                reqwest::header::HeaderName::from_bytes(name.as_bytes()).expect("valid header name"),
                reqwest::header::HeaderValue::from_str(value).expect("valid header value"),
            );
        }
        map
    }

    #[test]
    fn retry_after_wins_and_is_capped() {
        assert_eq!(
            rate_limit_wait(&headers(&[("retry-after", "7")])),
            Some(Duration::from_secs(7))
        );

        let huge = MAX_RATE_LIMIT_WAIT.as_secs() + 600;
        assert_eq!(
            rate_limit_wait(&headers(&[("retry-after", &huge.to_string())])),
            Some(MAX_RATE_LIMIT_WAIT)
        );
    }

    #[test]
    fn reset_timestamp_becomes_a_wait_from_now() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_secs();

        let future = headers(&[("x-ratelimit-reset", &(now + 30).to_string())]);
        let wait = rate_limit_wait(&future).expect("a future reset yields a wait");
        assert!((25..=30).contains(&wait.as_secs()), "wait was {:?}", wait);

        assert_eq!(rate_limit_wait(&headers(&[("x-ratelimit-reset", "1")])), None);
    }

    #[test]
    fn no_rate_limit_headers_means_no_wait_to_compute() {
        assert_eq!(rate_limit_wait(&headers(&[])), None);
        assert_eq!(rate_limit_wait(&headers(&[("x-ratelimit-remaining", "9")])), None);
    }
}
