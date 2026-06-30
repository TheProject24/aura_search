use std::path::{Path, PathBuf};

use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use futures::StreamExt;
use tokio::runtime::Runtime;

use crate::document_ingest;
use crate::crawler::DirectoryCrawler;
use crate::engine::SearchEngineCore;
use crate::index::{DocumentSourceKind, InvertedIndex};
use crate::storage::StorageManager;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestionSourceKind {
    LocalDir,
    S3,
}

#[derive(Debug, Clone)]
pub struct IngestionDocument {
    pub source_id: String,
    pub source_kind: DocumentSourceKind,
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
            match document_ingest::normalize_for_indexing(&path_buf) {
                Ok(body) => {
                    documents.push(IngestionDocument {
                        source_id: path_buf.to_string_lossy().into_owned(),
                        source_kind: DocumentSourceKind::Filesystem,
                        body,
                    });
                }
                Err(err) => {
                    eprintln!(
                        "Warning: Skipped unreadable document {}, Reason: {}",
                        path_buf.display(),
                        err
                    );
                }
            }
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
        let object_store = AmazonS3Builder::new()
            .with_bucket_name(&self.bucket)
            .build()
            .map_err(|e| format!("Failed to build S3 object store for bucket '{}': {e}", self.bucket))?;

        let prefix = self.prefix.clone().unwrap_or_default();
        let root = if prefix.is_empty() {
            ObjectPath::from("")
        } else {
            ObjectPath::from(prefix.as_str())
        };

        let runtime = Runtime::new().map_err(|e| format!("Failed to create async runtime for S3 ingestion: {e}"))?;
        runtime.block_on(async move {
            let mut stream = object_store.list(Some(&root));

            let mut documents = Vec::new();
            let allowed = document_ingest::allowed_extensions();

            while let Some(entry) = stream.next().await {
                let entry = entry.map_err(|e| format!("Failed to read S3 listing entry: {e}"))?;
                let location = entry.location;
                let key = location.to_string();
                let extension = Path::new(&key)
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .unwrap_or("")
                    .to_ascii_lowercase();

                if !allowed.iter().any(|candidate| candidate == &extension) {
                    continue;
                }

                let bytes = object_store
                    .get(&location)
                    .await
                    .map_err(|e| format!("Failed to fetch S3 object '{key}': {e}"))?;
                let content = bytes
                    .bytes()
                    .await
                    .map_err(|e| format!("Failed to read bytes for S3 object '{key}': {e}"))?;

                let body = match extension.as_str() {
                    "md" => document_ingest::normalize_for_text(String::from_utf8_lossy(&content).as_ref(), true),
                    "txt" => document_ingest::normalize_for_text(String::from_utf8_lossy(&content).as_ref(), false),
                    "csv" => document_ingest::normalize_for_csv_bytes(&content),
                    "docx" => document_ingest::normalize_for_docx_bytes(&content),
                    "xlsx" => document_ingest::normalize_for_xlsx_bytes(&content),
                    "pdf" => document_ingest::normalize_for_pdf_bytes(&content),
                    _ => Err(format!("Unsupported S3 object format: {key}")),
                };

                match body {
                    Ok(body) => {
                        documents.push(IngestionDocument {
                            source_id: key,
                            source_kind: DocumentSourceKind::S3Object,
                            body,
                        });
                    }
                    Err(err) => {
                        eprintln!("Warning: Skipped unreadable S3 object {}, Reason: {}", key, err);
                    }
                }
            }

            Ok::<_, String>(documents)
        })
    }
}

pub fn ingest_source(engine: &SearchEngineCore, source: &dyn IngestionSource) -> Result<Vec<String>, String> {
    let documents = source.collect_documents()?;
    let mut indexed = Vec::new();

    for document in documents {
        let tokens = engine.analyzer.analyze(&document.body);
        engine.ingest_document(&document.source_id, document.source_kind, tokens);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::SearchEngineCore;

    struct MockSource {
        kind: IngestionSourceKind,
        docs: Vec<IngestionDocument>,
    }

    impl IngestionSource for MockSource {
        fn kind(&self) -> IngestionSourceKind {
            self.kind
        }

        fn collect_documents(&self) -> Result<Vec<IngestionDocument>, String> {
            Ok(self.docs.clone())
        }
    }

    #[test]
    fn ingest_pipeline_accepts_any_source_implementation() {
        let engine = SearchEngineCore::new();
        let source = MockSource {
            kind: IngestionSourceKind::S3,
            docs: vec![
                IngestionDocument {
                    source_id: "s3://bucket/doc-1.txt".to_string(),
                    source_kind: DocumentSourceKind::S3Object,
                    body: "fast car on s3".to_string(),
                },
                IngestionDocument {
                    source_id: "s3://bucket/doc-2.txt".to_string(),
                    source_kind: DocumentSourceKind::S3Object,
                    body: "another fast vehicle".to_string(),
                },
            ],
        };

        let indexed = ingest_source(&engine, &source).unwrap();
        assert_eq!(indexed.len(), 2);
        assert!(indexed[0].starts_with("s3://"));
    }

    #[test]
    fn s3_source_can_be_configured_without_live_access() {
        let source = S3IngestionSource::new("demo-bucket", Some("corpus/".to_string()));
        assert_eq!(source.kind(), IngestionSourceKind::S3);
        assert_eq!(source.bucket, "demo-bucket");
        assert_eq!(source.prefix.as_deref(), Some("corpus/"));
    }
}
