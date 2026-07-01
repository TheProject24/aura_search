use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use base64::Engine;
use serde::{Deserialize, Serialize};

use zynsearch_core::engine::SearchEngineCore;
use zynsearch_core::index::DocumentSourceKind;
use zynsearch_core::multi_protocol::ZynQuery;
use zynsearch_core::query_pipeline::{display_filename, QueryCoordinator};
use zynsearch_core::storage::StorageManager;
use zynsearch_core::top_k::SearchResult as CoreSearchResult;

#[derive(Clone)]
pub struct HttpServerState {
    pub engine_core: Arc<SearchEngineCore>,
    pub coordinator: QueryCoordinator,
    pub db_path: String,
    pub auth: Option<HttpAuth>,
}

#[derive(Clone, Debug)]
pub struct HttpTlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

#[derive(Clone)]
pub struct HttpAuth {
    username: String,
    password: String,
}

impl HttpAuth {
    pub fn new(username: String, password: String) -> Self {
        Self { username, password }
    }
}

#[derive(Debug, Deserialize)]
pub struct IndexRequestBody {
    pub source_id: String,
    pub content: String,
    pub source_kind: Option<DocumentSourceKindRequest>,
    pub replace_existing: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct SearchRequestQuery {
    pub q: String,
    pub limit: Option<usize>,
    pub explain: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct IndexResponseBody {
    pub document_id: u64,
    pub source_id: String,
    pub status: &'static str,
}

#[derive(Debug, Serialize)]
pub struct DeleteResponseBody {
    pub document_id: u64,
    pub source_id: String,
    pub status: &'static str,
}

#[derive(Debug, Serialize)]
pub struct SearchResponseBody {
    pub query: String,
    pub results: Vec<SearchResultBody>,
    pub stats: SearchStatsBody,
}

#[derive(Debug, Serialize)]
pub struct SearchResultBody {
    pub rank: u32,
    pub document_id: u64,
    pub doc_id: u64,
    pub source_id: String,
    pub filename: String,
    pub score: f32,
    pub explanation: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SearchStatsBody {
    pub total_hits: u32,
    pub truncated: bool,
}

#[derive(Debug, Serialize)]
pub struct ApiErrorResponse {
    pub error: ApiErrorBody,
}

#[derive(Debug, Serialize)]
pub struct ApiErrorBody {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum DocumentSourceKindRequest {
    Opaque,
    Filesystem,
    S3Object,
}

impl From<DocumentSourceKindRequest> for DocumentSourceKind {
    fn from(value: DocumentSourceKindRequest) -> Self {
        match value {
            DocumentSourceKindRequest::Opaque => DocumentSourceKind::Opaque,
            DocumentSourceKindRequest::Filesystem => DocumentSourceKind::Filesystem,
            DocumentSourceKindRequest::S3Object => DocumentSourceKind::S3Object,
        }
    }
}

pub fn router(state: HttpServerState) -> Router {
    Router::new()
        .route("/index", post(index_document))
        .route("/search", get(search_documents))
        .route("/index/:id", delete(delete_document))
        .layer(axum::middleware::from_fn(ensure_json_accept_header))
        .layer(axum::middleware::from_fn_with_state(state.clone(), auth_middleware))
        .with_state(state)
}

pub async fn serve(
    state: HttpServerState,
    bind_addr: SocketAddr,
    tls: Option<HttpTlsConfig>,
) -> Result<(), Box<dyn std::error::Error>> {
    let app = router(state);

    match tls {
        Some(tls) => {
            let config = axum_server::tls_rustls::RustlsConfig::from_pem_file(&tls.cert_path, &tls.key_path).await?;
            println!("\n[NETWORK] Server listening on HTTPS {}", bind_addr);
            println!("[NETWORK] Ready for authenticated REST requests over TLS...\n");
            axum_server::bind_rustls(bind_addr, config)
                .serve(app.into_make_service())
                .await?;
        }
        None => {
            let listener = tokio::net::TcpListener::bind(bind_addr).await?;
            println!("\n[NETWORK] Server listening on HTTP {}", bind_addr);
            println!("[NETWORK] Ready for authenticated REST requests...\n");
            axum::serve(listener, app).await?;
        }
    }

    Ok(())
}

async fn auth_middleware(
    State(state): State<HttpServerState>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    if let Some(auth) = &state.auth {
        let Some(header_value) = request.headers().get(header::AUTHORIZATION) else {
            return unauthorized();
        };

        let Ok(header_value) = header_value.to_str() else {
            return unauthorized();
        };

        let Some(encoded) = header_value.strip_prefix("Basic ") else {
            return unauthorized();
        };

        let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
            return unauthorized();
        };

        let Ok(credentials) = String::from_utf8(decoded) else {
            return unauthorized();
        };

        let Some((username, password)) = credentials.split_once(':') else {
            return unauthorized();
        };

        if username != auth.username || password != auth.password {
            return unauthorized();
        }
    }

    next.run(request).await
}

async fn index_document(
    State(state): State<HttpServerState>,
    Json(payload): Json<IndexRequestBody>,
) -> Result<Json<IndexResponseBody>, ApiError> {
    if payload.source_id.trim().is_empty() {
        return Err(ApiError::bad_request("source_id is required"));
    }

    let source_kind = payload.source_kind.unwrap_or(DocumentSourceKindRequest::Opaque).into();
    let replace_existing = payload.replace_existing.unwrap_or(false);
    let tokens = state.engine_core.analyzer.analyze(&payload.content);

    let (doc_id, status) = {
        let mut index = state
            .engine_core
            .index
            .write()
            .map_err(|_| ApiError::internal("index lock poisoned"))?;
        let existing_doc_id = index
            .document_registry
            .iter()
            .find(|(_, stored_source)| stored_source.as_str() == payload.source_id.as_str())
            .map(|(&doc_id, _)| doc_id);

        if let Some(existing_doc_id) = existing_doc_id {
            if replace_existing {
                index.delete_document(existing_doc_id);
            } else {
                return Ok(Json(IndexResponseBody {
                    document_id: existing_doc_id as u64,
                    source_id: payload.source_id,
                    status: "skipped",
                }));
            }
        }

        let doc_id = index.register_document(&payload.source_id, source_kind);
        index.add_document(doc_id, tokens);
        let status = if existing_doc_id.is_some() { "replaced" } else { "created" };
        (doc_id, status)
    };

    persist_index(&state)?;

    Ok(Json(IndexResponseBody {
        document_id: doc_id as u64,
        source_id: payload.source_id,
        status,
    }))
}

async fn search_documents(
    State(state): State<HttpServerState>,
    Query(query): Query<SearchRequestQuery>,
) -> Result<Json<SearchResponseBody>, ApiError> {
    let raw_query = query.q.trim();
    if raw_query.is_empty() {
        return Err(ApiError::bad_request("q is required"));
    }

    let limit = query.limit.unwrap_or(10).max(1);
    let explain = query.explain.unwrap_or(false);
    let results = state.coordinator.execute(ZynQuery {
        query_string: raw_query.to_string(),
        limit: limit as u32,
    });

    let mut converted = Vec::new();
    for (rank, result) in results.into_iter().take(limit).enumerate() {
        let Some(result) = convert_search_result(&state, rank, result, explain) else {
            continue;
        };
        converted.push(result);
    }

    let stats = SearchStatsBody {
        total_hits: converted.len() as u32,
        truncated: converted.len() >= limit,
    };

    Ok(Json(SearchResponseBody {
        query: query.q,
        results: converted,
        stats,
    }))
}

async fn delete_document(
    State(state): State<HttpServerState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<DeleteResponseBody>, ApiError> {
    let (doc_id, source_id) = {
        let index = state
            .engine_core
            .index
            .read()
            .map_err(|_| ApiError::internal("index lock poisoned"))?;

        if let Ok(doc_id) = id.parse::<usize>() {
            let source_id = index.document_registry.get(&doc_id).cloned().unwrap_or_default();
            (Some(doc_id), source_id)
        } else {
            let doc_id = index
                .document_registry
                .iter()
                .find(|(_, stored_source)| stored_source.as_str() == id.as_str())
                .map(|(&doc_id, _)| doc_id);
            let source_id = doc_id
                .and_then(|doc_id| index.document_registry.get(&doc_id).cloned())
                .unwrap_or_else(|| id.clone());
            (doc_id, source_id)
        }
    };

    let Some(doc_id) = doc_id else {
        return Err(ApiError::not_found("document not found"));
    };

    state.engine_core.delete_document(doc_id);
    persist_index(&state)?;

    Ok(Json(DeleteResponseBody {
        document_id: doc_id as u64,
        source_id,
        status: "deleted",
    }))
}

fn convert_search_result(
    state: &HttpServerState,
    rank: usize,
    result: CoreSearchResult,
    explain: bool,
) -> Option<SearchResultBody> {
    let index = state.engine_core.index.read().ok()?;
    let doc_id = result.doc_id as usize;
    let metadata = index.document_metadata.get(&doc_id)?;

    let filename = {
        let display = display_filename(Some(&metadata.source_id));
        if display.is_empty() {
            metadata.source_id.clone()
        } else {
            display
        }
    };

    Some(SearchResultBody {
        rank: (rank + 1) as u32,
        document_id: doc_id as u64,
        doc_id: doc_id as u64,
        source_id: metadata.source_id.clone(),
        filename,
        score: result.score,
        explanation: explain.then(|| format!("bm25_score={}", result.score)),
    })
}

fn persist_index(state: &HttpServerState) -> Result<(), ApiError> {
    let index = state
        .engine_core
        .index
        .read()
        .map_err(|_| ApiError::internal("index lock poisoned"))?;
    StorageManager::serialize(&index, &state.db_path)
        .map_err(|err| ApiError::internal(format!("failed to persist index: {err}")))
}

async fn ensure_json_accept_header(
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let accepts_json = request
        .headers()
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.contains("application/json") || value == "*/*")
        .unwrap_or(true);

    if !accepts_json {
        return ApiError::unsupported_media_type("accept application/json").into_response();
    }

    next.run(request).await
}

fn unauthorized() -> Response {
    ApiError::unauthorized("invalid or missing credentials").into_response()
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized", message)
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", message)
    }

    fn unsupported_media_type(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_ACCEPTABLE, "not_acceptable", message)
    }

    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }
}

impl From<ApiError> for ApiErrorResponse {
    fn from(error: ApiError) -> Self {
        Self {
            error: ApiErrorBody {
                code: error.code.to_string(),
                message: error.message,
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status;
        let body: ApiErrorResponse = self.into();
        (status, Json(body)).into_response()
    }
}

impl IntoResponse for ApiErrorResponse {
    fn into_response(self) -> Response {
        Json(self).into_response()
    }
}
