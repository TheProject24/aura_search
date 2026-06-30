use std::path::{Path, PathBuf};

use crate::document_ingest;
use crate::crawler::DirectoryCrawler;
use crate::engine::SearchEngineCore;
use crate::index::InvertedIndex;
use crate::storage::StorageManager;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestionSourceKind {
    LocalDir,
    S3,
}

#[derive(Debug, Clone)]
pub struct IngestionDocument {
    pub source_id: String,
    pub body: String,
}

pub trait IngestionSource {
    fn kind(&self) -> IngestionSourceKind;
    fn collect_documents(&self) -> Result<Vec<IngestionDocument>, String>;
}

pub struct LocalDirIngestionSource {
    root_dir: PathBuf,
}

impl LocalDirIngestionSource {
    pub fn new(root_dir: impl Into<PathBuf>) -> Self {
        Self {
            root_dir: root_dir.into(),
        }
    }
}

impl IngestionSource for LocalDirIngestionSource {
    fn kind(&self) -> IngestionSourceKind {
        IngestionSourceKind::LocalDir
    }

    fn collect_documents(&self) -> Result<Vec<IngestionDocument>, String> {
        let crawler = DirectoryCrawler::new(Path::new(&self.root_dir), document_ingest::allowed_extensions());
        let mut documents = Vec::new();

        for path_buf in crawler.run() {
            let body = document_ingest::normalize_for_indexing(&path_buf)?;
            documents.push(IngestionDocument {
                source_id: path_buf.to_string_lossy().into_owned(),
                body,
            });
        }

        Ok(documents)
    }
}

pub struct S3IngestionSource {
    pub bucket: String,
    pub prefix: Option<String>,
}

impl S3IngestionSource {
    pub fn new(bucket: impl Into<String>, prefix: Option<String>) -> Self {
        Self {
            bucket: bucket.into(),
            prefix,
        }
    }
}

impl IngestionSource for S3IngestionSource {
    fn kind(&self) -> IngestionSourceKind {
        IngestionSourceKind::S3
    }

    fn collect_documents(&self) -> Result<Vec<IngestionDocument>, String> {
        Err(format!(
            "S3 ingestion is wired into the config and source abstraction, but the object-store client is not implemented yet for bucket '{}'",
            self.bucket
        ))
    }
}

pub fn ingest_source(engine: &SearchEngineCore, source: &dyn IngestionSource) -> Result<Vec<String>, String> {
    let documents = source.collect_documents()?;
    let mut indexed = Vec::new();

    for document in documents {
        engine.ingest_document_text(&document.source_id, &document.body);
        indexed.push(document.source_id);
    }

    Ok(indexed)
}

pub fn ingest_and_persist(
    engine: &std::sync::Arc<SearchEngineCore>,
    config_db_path: &str,
    source: &dyn IngestionSource,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let indexed = ingest_source(engine, source)?;
    println!("[BOOT] Indexed {} documents from corpus.", indexed.len());

    let current_state: std::sync::RwLockReadGuard<'_, InvertedIndex> = engine.index.read().unwrap();
    let _ = StorageManager::serialize(&current_state, config_db_path);
    Ok(indexed)
}
