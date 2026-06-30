use std::path::Path;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use zynsearch::engine::SearchEngineCore;
use zynsearch::storage::StorageManager;
use zynsearch::config::{load_app_config, AppConfig, IngestionMode, ProtocolMode};
use zynsearch::ingestion::{ingest_and_persist, LocalDirIngestionSource, S3IngestionSource};
use zynsearch::query_pipeline::{format_results, parse_query, QueryCoordinator};
use zynsearch::multi_protocol::ZynQuery;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_app_config()?;

    println!("========================================");
    println!("      ZynSearch Search Daemon v1.0     ");
    println!("========================================");

    let engine_core = SearchEngineCore::new();
    let shared_engine = std::sync::Arc::new(engine_core);
    let coordinator = QueryCoordinator::new(shared_engine.clone(), 4);

    if Path::new(&config.db_path).exists() {
        println!("[BOOT] Hydrating database from: {}", config.db_path);
        match StorageManager::deserialize(&config.db_path) {
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

    if let Some(query) = config.query.clone() {
        let hits = coordinator.execute(ZynQuery { query_string: query.clone(), limit: 10 });
        println!("{}", String::from_utf8_lossy(&format_results(&hits, config.output_format)));
        return Ok(());
    }

    match config.protocol {
        ProtocolMode::Tcp => {
            run_tcp_server(&config, coordinator).await?;
        }
        ProtocolMode::Http | ProtocolMode::Grpc | ProtocolMode::Both => {
            println!(
                "[BOOT] Selected protocol {:?}, but the transport layer is not built yet. Falling back to TCP.",
                config.protocol
            );
            run_tcp_server(&config, coordinator).await?;
        }
    }

    Ok(())
}

async fn run_tcp_server(
    config: &AppConfig,
    coordinator: QueryCoordinator,
) -> Result<(), Box<dyn std::error::Error>> {
    let bind_addr = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(&bind_addr).await?;
    println!("\n[NETWORK] Server listening on TCP {}", bind_addr);
    println!("[NETWORK] Ready for incoming connections...\n");

    loop {
        let (mut socket, addr) = listener.accept().await?;
        println!("[TCP] Client connected from: {}", addr);

        let coordinator = coordinator.clone();
        let output_format = config.output_format;
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

fn ingest_corpus(engine_core: &std::sync::Arc<SearchEngineCore>, config: &AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    match config.ingestion {
        IngestionMode::LocalDir => {
            let source = LocalDirIngestionSource::new(&config.corpus_dir);
            ingest_and_persist(engine_core, &config.db_path, &source)?;
        }
        IngestionMode::S3 => {
            let bucket = config
                .s3_bucket
                .as_ref()
                .ok_or("S3 ingestion selected but no s3_bucket was configured")?;
            let source = S3IngestionSource::new(bucket.clone(), config.s3_prefix.clone());
            ingest_and_persist(engine_core, &config.db_path, &source)?;
        }
    }
    Ok(())
}
