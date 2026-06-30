mod crawler;
mod parser;
mod analyzer;
mod index;
mod searcher;
mod engine;
mod storage;
mod memtable;
mod segment;
mod boolean_query;
mod collection_stats;
mod bm25;
mod top_k;
mod positional_queries;
mod wire_framing;
mod multi_protocol;
mod sharding;
mod sc_ga;
mod multi_reader;
mod dictionary;
mod merge_policy;
mod compression;
mod skip_posting;
mod bm_wand;
mod layout;
mod wal;
mod bitmap;
mod config; // Register your new config module!

use std::path::Path;
use clap::Parser;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde::Serialize;

use crawler::DirectoryCrawler;
use parser::{PlainTextParser, MarkdownParser, DocumentParser};
use engine::SearchEngineCore;
use storage::StorageManager;
use config::{Config, OutputFormat};

#[derive(Serialize)]
struct SearchResponse {
    query: String,
    status: String,
    match_count: usize,
    documents: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Parse configuration from CLI arguments or .env variables
    let config = Config::parse();

    println!("========================================");
    println!("      ZynSearch TCP Daemon v1.0        ");
    println!("========================================");

    let engine_core = SearchEngineCore::new();

    // 2. BOOT SEQUENCE
    if Path::new(&config.db_path).exists() {
        println!("[BOOT] Hydrating database from: {}", config.db_path);
        match StorageManager::deserialize(&config.db_path) {
            Ok(loaded_index) => {
                *engine_core.index.write().unwrap() = loaded_index;
                println!("[BOOT] Hydration successful.");
            }
            Err(e) => {
                eprintln!("[ERROR] Failed to load DB: {}", e);
                run_ingestion_pipeline(&engine_core, &config);
            }
        }
    } else {
        println!("[BOOT] No database found. Initiating corpus crawl...");
        run_ingestion_pipeline(&engine_core, &config);
    }

    // 3. START TCP SERVER
    let bind_addr = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(&bind_addr).await?;
    println!("\n[NETWORK] Server listening on TCP {}", bind_addr);
    println!("[NETWORK] Ready for incoming connections...\n");

    // Wrap the engine in an Arc so we can share it safely across thousands of TCP connections
    let shared_engine = std::sync::Arc::new(engine_core);

    // 4. ASYNC CONNECTION LOOP
    loop {
        // Wait for a new client to connect
        let (mut socket, addr) = listener.accept().await?;
        println!("[TCP] Client connected from: {}", addr);

        // Clone the Arc pointer for this specific client task
        let engine_clone = shared_engine.clone();

        // Spawn a concurrent Tokio task. This allows thousands of users 
        // to search simultaneously without blocking each other!
        tokio::spawn(async move {
            let mut buffer = [0; 1024];

            // Greet the client
            let _ = socket.write_all(b"Connected to ZynSearch. Enter query:\n> ").await;

            loop {
                // Read client input
                let bytes_read = match socket.read(&mut buffer).await {
                    Ok(n) if n == 0 => break, // Client disconnected cleanly
                    Ok(n) => n,
                    Err(_) => break, // Connection dropped
                };

                // Parse the raw bytes into a UTF-8 string query
                let query = String::from_utf8_lossy(&buffer[..bytes_read]);
                let query = query.trim();

                if query.is_empty() {
                    let _ = socket.write_all(b"> ").await;
                    continue;
                }

                if query == "exit" || query == "quit" {
                    let _ = socket.write_all(b"Goodbye!\n").await;
                    break;
                }

                // EXECUTE SEARCH ALGORITHM
                let hits = engine_clone.execute_search(query);

                // Build the standard response object
                let response_data = SearchResponse {
                    query: query.to_string(),
                    status: "success".to_string(),
                    match_count: hits.len(),
                    documents: hits,
                };

                // 3. Dynamically format based on runtime configuration
                let wire_bytes: Vec<u8> = match config.format {
                    OutputFormat::Text => {
                        // Human-readable terminal output
                        let mut text = format!("Found {} matches:\n", response_data.match_count);
                        for doc in &response_data.documents {
                            text.push_str(&format!(" -> {}\n", doc));
                        }
                        text.into_bytes()
                    }
                    OutputFormat::Json => {
                        // High-compatibility JSON string serialization
                        serde_json::to_vec(&response_data).unwrap_or_default()
                    }
                    OutputFormat::Binary => {
                        // Ultra-low latency, dense binary representation for other Rust backend nodes
                        bincode::serialize(&response_data).unwrap_or_default()
                    }
                };

                // Shoot the customized byte envelope out across the TCP wire!
                if socket.write_all(&wire_bytes).await.is_err() {
                    break;
                }
            }
            println!("[TCP] Client disconnected: {}", addr);
        });
    }
}

fn run_ingestion_pipeline(engine_core: &SearchEngineCore, config: &Config) {
    let target_dir = Path::new(&config.corpus_dir);
    let allowed_extensions = vec!["txt".to_string(), "md".to_string()];
    let crawler = DirectoryCrawler::new(target_dir, allowed_extensions);

    let discovered_files = crawler.run();
    for path_buf in discovered_files {
        let path_str = path_buf.to_string_lossy().into_owned();
        let raw_content = std::fs::read_to_string(&path_buf).unwrap_or_default();
        let clean_text = match path_buf.extension().and_then(|ext| ext.to_str()) {
            Some("md") => MarkdownParser.parse(&raw_content),
            _ => PlainTextParser.parse(&raw_content),
        };
        let tokens = engine_core.analyzer.analyze(&clean_text);
        engine_core.ingest_document(&path_str, tokens);
    }
    
    let current_state = engine_core.index.read().unwrap();
    let _ = StorageManager::serialize(&current_state, &config.db_path);
}
