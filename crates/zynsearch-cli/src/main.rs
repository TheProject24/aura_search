use std::path::Path;
use std::time::Duration;

use zynsearch_core::config::{load_app_config, AppConfig, IngestionMode};
use zynsearch_core::engine::SearchEngineCore;
use zynsearch_core::ingestion::{ingest_and_persist, LocalDirIngestionSource, S3IngestionSource};
use zynsearch_core::multi_protocol::ZynQuery;
use zynsearch_core::query_pipeline::format_results;
use zynsearch_core::storage::StorageManager;
use zynsearch_core::query_pipeline::QueryCoordinator;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_app_config()?;
    let engine_core = SearchEngineCore::new();
    let shared_engine = std::sync::Arc::new(engine_core);
    let coordinator = QueryCoordinator::new(shared_engine.clone(), 4);
    if config.cleanup.enable_periodic_cleanup {
        shared_engine.start_periodic_cleanup(Duration::from_secs(config.cleanup.period_seconds.max(1)));
    }

    if Path::new(&config.storage.db_path).exists() {
        if let Ok(loaded_index) = StorageManager::deserialize(&config.storage.db_path) {
            *shared_engine.index.write().unwrap() = loaded_index;
        } else {
            ingest_corpus(&shared_engine, &config)?;
        }
    } else {
        ingest_corpus(&shared_engine, &config)?;
    }

    if let Some(query) = config.runtime.query.clone() {
        let hits = coordinator.execute(ZynQuery { query_string: query.clone(), limit: 10 });
        println!("{}", String::from_utf8_lossy(&format_results(&hits, config.runtime.output_format)));
    }

    Ok(())
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
