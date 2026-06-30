use std::path::Path;
use std::pin::Pin;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tonic::transport::Server;

use zynsearch_core::index::DocumentSourceKind;
use zynsearch_core::engine::SearchEngineCore;
use zynsearch_core::storage::StorageManager;
use zynsearch_core::config::{load_app_config, AppConfig, IngestionMode, ProtocolMode};
use zynsearch_core::ingestion::{ingest_and_persist, LocalDirIngestionSource, S3IngestionSource};
use zynsearch_core::query_pipeline::{format_results, parse_query, QueryCoordinator};
use zynsearch_core::multi_protocol::ZynQuery;
use zynsearch_core::top_k::SearchResult as CoreSearchResult;
mod http;

pub mod zynsearch {
    pub mod v1 {
        tonic::include_proto!("zynsearch.v1");
    }
}

use zynsearch::v1::{
    BulkIndexItemResult, BulkIndexRequest, BulkIndexResponse, DeleteRequest, DeleteResponse,
    DeleteStatus, Document, DocumentSourceKind as ProtoDocumentSourceKind, Error, IndexOptions,
    IndexRequest, IndexResponse, IndexStatus, SearchRequest, SearchResponse, SearchResult,
    SearchStats, SearchStreamResponse,
};
use zynsearch::v1::zyn_search_service_server::{ZynSearchService, ZynSearchServiceServer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_app_config()?;

    println!("========================================");
    println!(
        "      {} v{}      ",
        config.manifest.name,
        config.manifest.version
    );
    println!("========================================");

    let engine_core = SearchEngineCore::new();
    let shared_engine = std::sync::Arc::new(engine_core);
    let coordinator = QueryCoordinator::new(shared_engine.clone(), 4);
    if config.cleanup.enable_periodic_cleanup {
        shared_engine.start_periodic_cleanup(Duration::from_secs(config.cleanup.period_seconds.max(1)));
    }

    if Path::new(&config.storage.db_path).exists() {
        println!("[BOOT] Hydrating database from: {}", config.storage.db_path);
        match StorageManager::deserialize(&config.storage.db_path) {
            Ok(loaded_index) => {
                *shared_engine.index.write().unwrap() = loaded_index;
                println!("[BOOT] Hydration successful.");
            }
            Err(e) => {
                eprintln!("[ERROR] Failed to load DB: {}", e);
                ingest_corpus(&shared_engine, &config)?;
            }
        }
    } else {
        println!("[BOOT] No database found. Initiating corpus crawl...");
        ingest_corpus(&shared_engine, &config)?;
    }

    if let Some(query) = config.runtime.query.clone() {
        let hits = coordinator.execute(ZynQuery { query_string: query.clone(), limit: 10 });
        println!("{}", String::from_utf8_lossy(&format_results(&hits, config.runtime.output_format)));
        return Ok(());
    }

    match config.runtime.protocol {
        ProtocolMode::Tcp => {
            run_tcp_server(&config, coordinator).await?;
        }
        ProtocolMode::Grpc => {
            run_grpc_server(&config, shared_engine).await?;
        }
        ProtocolMode::Http => {
            run_http_server(&config, shared_engine).await?;
        }
        ProtocolMode::Both => {
            println!("[BOOT] Starting HTTP transport.");
            run_http_server(&config, shared_engine.clone()).await?;
            run_grpc_server(&config, shared_engine).await?;
        }
    }

    Ok(())
}

async fn run_tcp_server(
    config: &AppConfig,
    coordinator: QueryCoordinator,
) -> Result<(), Box<dyn std::error::Error>> {
    let bind_addr = format!("{}:{}", config.runtime.host, config.runtime.port);
    let listener = TcpListener::bind(&bind_addr).await?;
    println!("\n[NETWORK] Server listening on TCP {}", bind_addr);
    println!("[NETWORK] Ready for incoming connections...\n");

    loop {
        let (mut socket, addr) = listener.accept().await?;
        println!("[TCP] Client connected from: {}", addr);

        let coordinator = coordinator.clone();
        let output_format = config.runtime.output_format;
        tokio::spawn(async move {
            let mut buffer = [0; 1024];

            let _ = socket.write_all(b"Connected to ZynSearch. Enter query:\n> ").await;

            loop {
                let bytes_read = match socket.read(&mut buffer).await {
                    Ok(n) if n == 0 => break,
                    Ok(n) => n,
                    Err(_) => break,
                };

                let payload = &buffer[..bytes_read];
                let parsed_query = match parse_query(payload) {
                    Ok(query) => query,
                    Err(err) => {
                        let _ = socket.write_all(format!("Query parse error: {}\n", err).as_bytes()).await;
                        continue;
                    }
                };

                if parsed_query.query_string.trim().is_empty() {
                    let _ = socket.write_all(b"> ").await;
                    continue;
                }

                if parsed_query.query_string == "exit" || parsed_query.query_string == "quit" {
                    let _ = socket.write_all(b"Goodbye!\n").await;
                    break;
                }

                let hits = coordinator.execute(parsed_query.clone());
                let wire_bytes = format_results(&hits, output_format);

                if socket.write_all(&wire_bytes).await.is_err() {
                    break;
                }
            }
            println!("[TCP] Client disconnected: {}", addr);
        });
    }
}

async fn run_grpc_server(
    config: &AppConfig,
    engine_core: Arc<SearchEngineCore>,
) -> Result<(), Box<dyn std::error::Error>> {
    let bind_addr = format!("{}:{}", config.runtime.host, config.runtime.port).parse()?;
    let service = ZynGrpcService::new(engine_core, config.storage.db_path.clone());

    println!("\n[NETWORK] Server listening on gRPC {}", bind_addr);
    println!("[NETWORK] Ready for incoming gRPC requests...\n");

    Server::builder()
        .add_service(ZynSearchServiceServer::new(service))
        .serve(bind_addr)
        .await?;

    Ok(())
}

async fn run_http_server(
    config: &AppConfig,
    engine_core: Arc<SearchEngineCore>,
) -> Result<(), Box<dyn std::error::Error>> {
    let bind_addr = format!("{}:{}", config.runtime.host, config.runtime.port).parse()?;
    let auth = match (&config.runtime.http_username, &config.runtime.http_password) {
        (Some(username), Some(password)) => Some(http::HttpAuth::new(username.clone(), password.clone())),
        _ => None,
    };
    let http_auth_enabled = auth.is_some();

    if auth.is_none() {
        println!("[BOOT] HTTP authentication is disabled. Set ZYN_HTTP_USERNAME and ZYN_HTTP_PASSWORD to secure the API.");
    }

    let state = http::HttpServerState {
        coordinator: QueryCoordinator::new(engine_core.clone(), 4),
        engine_core,
        db_path: config.storage.db_path.clone(),
        auth,
    };

    let tls = match (
        config.runtime.http_tls_cert_path.as_ref(),
        config.runtime.http_tls_key_path.as_ref(),
    ) {
        (Some(cert_path), Some(key_path)) => Some(http::HttpTlsConfig {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
        }),
        (None, None) => None,
        _ => {
            return Err("both ZYN_HTTP_TLS_CERT_PATH and ZYN_HTTP_TLS_KEY_PATH must be set to enable HTTPS".into());
        }
    };

    if tls.is_some() && !http_auth_enabled {
        println!("[BOOT] HTTPS is enabled. Consider setting Basic Auth too for SDK credential-based access.");
    }

    http::serve(state, bind_addr, tls).await
}

fn ingest_corpus(engine_core: &std::sync::Arc<SearchEngineCore>, config: &AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    match config.ingestion.mode {
        IngestionMode::LocalDir => {
            let source = LocalDirIngestionSource::new(&config.ingestion.corpus_dir);
            ingest_and_persist(engine_core, &config.storage.db_path, &source)?;
        }
        IngestionMode::S3 => {
            let bucket = config
                .ingestion
                .s3_bucket
                .as_ref()
                .ok_or("S3 ingestion selected but no s3_bucket was configured")?;
            let source = S3IngestionSource::new(bucket.clone(), config.ingestion.s3_prefix.clone());
            ingest_and_persist(engine_core, &config.storage.db_path, &source)?;
        }
    }
    Ok(())
}

#[derive(Clone)]
struct ZynGrpcService {
    engine_core: Arc<SearchEngineCore>,
    coordinator: QueryCoordinator,
    db_path: String,
}

impl ZynGrpcService {
    fn new(engine_core: Arc<SearchEngineCore>, db_path: String) -> Self {
        Self {
            coordinator: QueryCoordinator::new(engine_core.clone(), 4),
            engine_core,
            db_path,
        }
    }

    fn persist(&self) -> Result<(), Status> {
        let index = self.engine_core.index.read()
            .map_err(|_| Status::internal("index lock poisoned"))?;
        StorageManager::serialize(&index, &self.db_path)
            .map_err(|e| Status::internal(format!("failed to persist index: {e}")))
    }

    fn index_document(&self, document: Document, options: Option<IndexOptions>) -> Result<IndexResponse, Status> {
        let response = self.index_document_without_persist(document, options)?;
        if response.status == IndexStatus::Created as i32
            || response.status == IndexStatus::Replaced as i32
        {
            self.persist()?;
        }
        Ok(response)
    }

    fn index_document_without_persist(&self, document: Document, options: Option<IndexOptions>) -> Result<IndexResponse, Status> {
        if document.source_id.trim().is_empty() {
            return Err(Status::invalid_argument("document.source_id is required"));
        }

        let source_kind = proto_source_kind_to_core(document.source_kind())?;
        let replace_existing = options.map(|o| o.replace_existing).unwrap_or(false);
        let tokens = self.engine_core.analyzer.analyze(&document.content);

        let (doc_id, status) = {
            let mut index = self.engine_core.index.write()
                .map_err(|_| Status::internal("index lock poisoned"))?;

            let existing_doc_id = find_doc_id_by_source(&index, &document.source_id);
            if let Some(existing_doc_id) = existing_doc_id {
                if replace_existing {
                    index.delete_document(existing_doc_id);
                } else {
                    return Ok(IndexResponse {
                        document_id: existing_doc_id as u64,
                        source_id: document.source_id,
                        status: IndexStatus::Skipped as i32,
                    });
                }
            }

            let doc_id = index.register_document(&document.source_id, source_kind);
            index.add_document(doc_id, tokens);
            let status = if existing_doc_id.is_some() {
                IndexStatus::Replaced
            } else {
                IndexStatus::Created
            };
            (doc_id, status)
        };

        Ok(IndexResponse {
            document_id: doc_id as u64,
            source_id: document.source_id,
            status: status as i32,
        })
    }

    fn search_results(&self, request: SearchRequest) -> Result<(Vec<SearchResult>, SearchStats), Status> {
        if request.query.trim().is_empty() {
            return Err(Status::invalid_argument("query is required"));
        }

        let start = std::time::Instant::now();
        let requested_limit = request.limit.max(1);
        let results = self.coordinator.execute(ZynQuery {
            query_string: request.query,
            limit: requested_limit,
        });

        let converted = results
            .iter()
            .enumerate()
            .filter_map(|(rank, result)| self.convert_search_result(rank, *result, request.explain))
            .collect::<Vec<_>>();

        let stats = SearchStats {
            total_hits: converted.len() as u32,
            truncated: converted.len() as u32 >= requested_limit,
            elapsed_millis: start.elapsed().as_millis() as u64,
        };

        Ok((converted, stats))
    }

    fn convert_search_result(&self, rank: usize, result: CoreSearchResult, explain: bool) -> Option<SearchResult> {
        let index = self.engine_core.index.read().ok()?;
        let doc_id = result.doc_id as usize;
        let metadata = index.document_metadata.get(&doc_id)?;
        let source_kind = core_source_kind_to_proto(metadata.source_kind);
        let filename = Path::new(&metadata.source_id)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(&metadata.source_id)
            .to_string();

        Some(SearchResult {
            document_id: result.doc_id as u64,
            doc_id: result.doc_id as u64,
            source_id: metadata.source_id.clone(),
            source_kind: source_kind as i32,
            title: filename.clone(),
            filename,
            rank: (rank + 1) as u32,
            score: result.score,
            matched_terms: Vec::new(),
            metadata: std::collections::HashMap::new(),
            explanation: if explain {
                format!("bm25_score={}", result.score)
            } else {
                String::new()
            },
        })
    }
}

#[tonic::async_trait]
impl ZynSearchService for ZynGrpcService {
    async fn index(
        &self,
        request: Request<IndexRequest>,
    ) -> Result<Response<IndexResponse>, Status> {
        let request = request.into_inner();
        let document = request
            .document
            .ok_or_else(|| Status::invalid_argument("document is required"))?;
        let response = self.index_document(document, request.options)?;
        Ok(Response::new(response))
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let (results, stats) = self.search_results(request.into_inner())?;
        Ok(Response::new(SearchResponse {
            results,
            stats: Some(stats),
        }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let request = request.into_inner();
        let (doc_id, source_id, source_kind) = {
            let index = self.engine_core.index.read()
                .map_err(|_| Status::internal("index lock poisoned"))?;
            match request.target {
                Some(zynsearch::v1::delete_request::Target::DocumentId(id)) => {
                    let metadata = index.document_metadata.get(&(id as usize));
                    (
                        Some(id as usize),
                        metadata.map(|m| m.source_id.clone()).unwrap_or_default(),
                        metadata.map(|m| m.source_kind).unwrap_or(DocumentSourceKind::Opaque),
                    )
                }
                Some(zynsearch::v1::delete_request::Target::SourceId(ref source)) => {
                    let doc_id = find_doc_id_by_source(&index, source);
                    if let Some(id) = doc_id {
                        if let Some(metadata) = index.document_metadata.get(&id) {
                            (doc_id, source.clone(), metadata.source_kind)
                        } else {
                            (doc_id, source.clone(), DocumentSourceKind::Opaque)
                        }
                    } else {
                        (None, source.clone(), DocumentSourceKind::Opaque)
                    }
                }
                None => return Err(Status::invalid_argument("delete target is required")),
            }
        };

        let Some(doc_id) = doc_id else {
            return Ok(Response::new(DeleteResponse {
                status: DeleteStatus::NotFound as i32,
                document_id: 0,
                source_id,
            }));
        };

        if request.delete_from_storage && source_kind == DocumentSourceKind::Filesystem && !source_id.is_empty() {
            match fs::remove_file(&source_id) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(Status::failed_precondition(format!("failed to delete source file: {e}"))),
            }
        }

        self.engine_core.delete_document(doc_id);
        self.persist()?;

        Ok(Response::new(DeleteResponse {
            status: DeleteStatus::Deleted as i32,
            document_id: doc_id as u64,
            source_id,
        }))
    }

    async fn bulk_index(
        &self,
        request: Request<tonic::Streaming<BulkIndexRequest>>,
    ) -> Result<Response<BulkIndexResponse>, Status> {
        let mut stream = request.into_inner();
        let mut indexed_count = 0;
        let mut changed = false;
        let mut results = Vec::new();

        while let Some(item) = stream.message().await? {
            let Some(document) = item.document else {
                results.push(BulkIndexItemResult {
                    document_id: 0,
                    source_id: String::new(),
                    status: IndexStatus::Unspecified as i32,
                    error: Some(Error {
                        code: "INVALID_ARGUMENT".to_string(),
                        message: "document is required".to_string(),
                        details: std::collections::HashMap::new(),
                    }),
                });
                continue;
            };

            let source_id = document.source_id.clone();
            match self.index_document_without_persist(document, item.options) {
                Ok(response) => {
                    if response.status == IndexStatus::Created as i32
                        || response.status == IndexStatus::Replaced as i32
                    {
                        indexed_count += 1;
                        changed = true;
                    }
                    results.push(BulkIndexItemResult {
                        document_id: response.document_id,
                        source_id: response.source_id,
                        status: response.status,
                        error: None,
                    });
                }
                Err(status) => {
                    results.push(BulkIndexItemResult {
                        document_id: 0,
                        source_id,
                        status: IndexStatus::Unspecified as i32,
                        error: Some(Error {
                            code: format!("{:?}", status.code()),
                            message: status.message().to_string(),
                            details: std::collections::HashMap::new(),
                        }),
                    });
                }
            }
        }

        if changed {
            self.persist()?;
        }

        Ok(Response::new(BulkIndexResponse {
            indexed_count,
            results,
        }))
    }

    type SearchStreamStream = Pin<Box<dyn Stream<Item = Result<SearchStreamResponse, Status>> + Send + 'static>>;

    async fn search_stream(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<Self::SearchStreamStream>, Status> {
        let request = request.into_inner();
        if request.query.trim().is_empty() {
            return Err(Status::invalid_argument("query is required"));
        }

        let query = ZynQuery {
            query_string: request.query,
            limit: request.limit.max(1),
        };
        let explain = request.explain;
        let coordinator = self.coordinator.clone();
        let service = self.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        tokio::task::spawn_blocking(move || {
            let start = std::time::Instant::now();
            let requested_limit = query.limit;
            let mut streamed = 0u32;
            let result_rx = coordinator.execute_streaming(query);

            for core_result in result_rx {
                let Some(result) = service.convert_search_result(core_result, explain) else {
                    continue;
                };
                streamed += 1;

                if tx.blocking_send(Ok(SearchStreamResponse {
                    payload: Some(zynsearch::v1::search_stream_response::Payload::Result(result)),
                })).is_err() {
                    return;
                }
            }

            let _ = tx.blocking_send(Ok(SearchStreamResponse {
                payload: Some(zynsearch::v1::search_stream_response::Payload::Stats(SearchStats {
                    total_hits: streamed,
                    truncated: streamed >= requested_limit,
                    elapsed_millis: start.elapsed().as_millis() as u64,
                })),
            }));
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

fn find_doc_id_by_source(index: &zynsearch_core::index::InvertedIndex, source_id: &str) -> Option<usize> {
    index
        .document_registry
        .iter()
        .find(|(_, stored_source)| stored_source.as_str() == source_id)
        .map(|(&doc_id, _)| doc_id)
}

fn proto_source_kind_to_core(kind: ProtoDocumentSourceKind) -> Result<DocumentSourceKind, Status> {
    match kind {
        ProtoDocumentSourceKind::Unspecified | ProtoDocumentSourceKind::Opaque => Ok(DocumentSourceKind::Opaque),
        ProtoDocumentSourceKind::Filesystem => Ok(DocumentSourceKind::Filesystem),
        ProtoDocumentSourceKind::S3Object => Ok(DocumentSourceKind::S3Object),
    }
}

fn core_source_kind_to_proto(kind: DocumentSourceKind) -> ProtoDocumentSourceKind {
    match kind {
        DocumentSourceKind::Opaque => ProtoDocumentSourceKind::Opaque,
        DocumentSourceKind::Filesystem => ProtoDocumentSourceKind::Filesystem,
        DocumentSourceKind::S3Object => ProtoDocumentSourceKind::S3Object,
    }
}
