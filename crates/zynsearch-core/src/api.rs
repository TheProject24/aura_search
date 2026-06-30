use std::path::Path;
use std::sync::Arc;

use crate::engine::SearchEngineCore;
use crate::ingestion::{self, IngestionSource};
use crate::query_pipeline::QueryCoordinator;
use crate::top_k::SearchResult;

/// Embeddable facade for the ZynSearch search engine.
pub struct ZynSearch {
    core: Arc<SearchEngineCore>,
    shard_count: usize,
}

impl ZynSearch {
    /// Create a new builder to configure ZynSearch programmatically.
    pub fn builder() -> ZynSearchBuilder {
        ZynSearchBuilder::default()
    }

    /// Index a document from raw text.
    pub fn index_text(&self, source_id: impl Into<String>, raw_text: impl AsRef<str>) {
        self.core.ingest_document_text(&source_id.into(), raw_text.as_ref());
    }

    /// Index a document from raw text.
    pub fn index_document(&self, source_id: impl Into<String>, raw_text: impl AsRef<str>) {
        self.core.ingest_document_text(&source_id.into(), raw_text.as_ref());
    }

    /// Index a document directly from pre-tokenized tokens.
    pub fn index_tokens(&self, source_id: impl Into<String>, tokens: Vec<String>) {
        self.core.ingest_document(&source_id.into(), tokens);
    }

    /// Index all candidate files inside a local directory matching default extension rules.
    pub fn index_directory(&self, path: impl AsRef<Path>) -> Result<Vec<String>, String> {
        self.core.ingest_corpus_dir(path.as_ref())
    }

    /// Index document data using any object conforming to `IngestionSource` (e.g. S3 source).
    pub fn index_source(&self, source: &dyn IngestionSource) -> Result<Vec<String>, String> {
        ingestion::ingest_source(&self.core, source)
    }

    /// Retrieve matching document paths using a raw search query.
    pub fn search(&self, query: impl AsRef<str>) -> Vec<String> {
        self.core.execute_search(query.as_ref())
    }

    /// Retrieve matching results scored via BM25 across all shards.
    pub fn search_scored(&self, query: impl AsRef<str>, limit: usize) -> Vec<SearchResult> {
        let coordinator = QueryCoordinator::new(self.core.clone(), self.shard_count);
        let zyn_query = crate::multi_protocol::ZynQuery {
            query_string: query.as_ref().to_string(),
            limit: limit as u32,
        };
        coordinator.execute(zyn_query)
    }

    /// Retrieve matching results scored via BM25 targeting a specific shard partition.
    pub fn search_scored_for_shard(&self, query: impl AsRef<str>, shard_id: usize, shard_count: usize, limit: usize) -> Vec<SearchResult> {
        self.core.execute_search_for_shard(query.as_ref(), shard_id, shard_count, limit)
    }

    /// Resolve a numerical Document ID to its corresponding path/name registry key.
    pub fn document_path(&self, doc_id: usize) -> Option<String> {
        let index = self.core.index.read().unwrap();
        index.document_registry.get(&doc_id).cloned()
    }

    /// Construct a scatter-gather query coordinator bound to the engine instance.
    pub fn coordinator(&self) -> QueryCoordinator {
        QueryCoordinator::new(self.core.clone(), self.shard_count)
    }

    /// Get a direct reference to the underlying core engine state.
    pub fn core(&self) -> Arc<SearchEngineCore> {
        self.core.clone()
    }

    /// Actively delete a document from the index registry and postings using its numerical ID.
    pub fn delete_document(&self, doc_id: usize) {
        self.core.delete_document(doc_id);
    }

    /// Actively delete a document from the index registry and postings using its path/key.
    pub fn delete_document_by_path(&self, path: impl AsRef<str>) {
        self.core.delete_document_by_path(path.as_ref());
    }

    /// Actively check and remove all document registries pointing to local files that no longer exist.
    pub fn cleanup_non_existent_documents(&self) {
        self.core.cleanup_non_existent_documents();
    }

    /// Explicitly start the periodic background cleanup thread for missing/deleted files.
    pub fn start_periodic_cleanup(&self, interval: std::time::Duration) {
        let core_clone = self.core.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                loop {
                    tokio::time::sleep(interval).await;
                    core_clone.cleanup_non_existent_documents();
                }
            });
        } else {
            std::thread::spawn(move || {
                loop {
                    std::thread::sleep(interval);
                    core_clone.cleanup_non_existent_documents();
                }
            });
        }
    }

    /// Load index database from a binary dump file path.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, String> {
        let path_str = path.as_ref().to_string_lossy().into_owned();
        let loaded_index = crate::storage::StorageManager::deserialize(&path_str)
            .map_err(|e| format!("Failed to load database: {e}"))?;
        let core = Arc::new(SearchEngineCore::new());
        *core.index.write().unwrap() = loaded_index;
        Ok(Self {
            core,
            shard_count: 1,
        })
    }

    /// Save current index database to a binary dump file path.
    pub fn save(&self, path: impl AsRef<Path>) -> Result<(), String> {
        let path_str = path.as_ref().to_string_lossy().into_owned();
        let index_guard = self.core.index.read().unwrap();
        crate::storage::StorageManager::serialize(&index_guard, &path_str)
            .map_err(|e| format!("Failed to save database: {e}"))
    }
}

/// A builder pattern helper for programmatically configuring `ZynSearch` instances.
pub struct ZynSearchBuilder {
    shard_count: usize,
    enable_periodic_cleanup: bool,
    cleanup_interval: std::time::Duration,
}

impl Default for ZynSearchBuilder {
    fn default() -> Self {
        Self {
            shard_count: 1,
            enable_periodic_cleanup: false,
            cleanup_interval: std::time::Duration::from_secs(60),
        }
    }
}

impl ZynSearchBuilder {
    /// Configure default partition shard count.
    pub fn shard_count(mut self, shard_count: usize) -> Self {
        self.shard_count = shard_count;
        self
    }

    /// Configure whether periodic background cleanup loop is enabled.
    pub fn enable_periodic_cleanup(mut self, enabled: bool) -> Self {
        self.enable_periodic_cleanup = enabled;
        self
    }

    /// Configure cleanup interval duration.
    pub fn cleanup_interval(mut self, interval: std::time::Duration) -> Self {
        self.cleanup_interval = interval;
        self
    }

    /// Finalize configuration and instantiate search engine.
    pub fn build(self) -> ZynSearch {
        let shard_count = self.shard_count.max(1);
        let core = Arc::new(SearchEngineCore::new());

        if self.enable_periodic_cleanup {
            let core_clone = core.clone();
            let interval = self.cleanup_interval;
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    loop {
                        tokio::time::sleep(interval).await;
                        core_clone.cleanup_non_existent_documents();
                    }
                });
            } else {
                std::thread::spawn(move || {
                    loop {
                        std::thread::sleep(interval);
                        core_clone.cleanup_non_existent_documents();
                    }
                });
            }
        }

        ZynSearch {
            core,
            shard_count,
        }
    }
}

/// Helper function to quickly instantiate a ZynSearch engine with default configuration.
pub fn create_engine() -> ZynSearch {
    ZynSearchBuilder::default().build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_creates_embeddable_engine() {
        let engine = ZynSearch::builder().shard_count(4).build();
        engine.index_text("doc-1", "fast car and red bike");
        engine.index_text("doc-2", "slow train and blue bus");

        let results = engine.search("fast car");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], "doc-1");
    }

    #[test]
    fn create_engine_helper_is_available() {
        let engine = create_engine();
        engine.index_text("doc-1", "fast car and red bike");
        let results = engine.search("fast car");
        assert_eq!(results, vec!["doc-1".to_string()]);
    }

    #[test]
    fn test_scored_search_and_document_path() {
        let engine = ZynSearch::builder().shard_count(2).build();
        engine.index_text("doc-1", "fast car and red bike");
        engine.index_text("doc-2", "slow train and blue bus");

        let scored = engine.search_scored("fast car", 10);
        assert!(!scored.is_empty());
        let best_doc_id = scored[0].doc_id;
        let resolved_path = engine.document_path(best_doc_id as usize).unwrap();
        assert_eq!(resolved_path, "doc-1");
    }

    #[test]
    fn test_save_and_load() {
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join("test_zynsearch_api.bin");

        let engine = create_engine();
        engine.index_text("doc-1", "fast car and red bike");
        engine.save(&db_path).unwrap();

        let loaded_engine = ZynSearch::load(&db_path).unwrap();
        let results = loaded_engine.search("fast car");
        assert_eq!(results, vec!["doc-1".to_string()]);

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn test_deletion_and_cleanup() {
        let temp_dir = std::env::temp_dir();

        let file1 = temp_dir.join("zyn_doc-1.txt");
        let file2 = temp_dir.join("zyn_doc-2.txt");

        std::fs::write(&file1, "fast red formula car").unwrap();
        std::fs::write(&file2, "speed blue formula car").unwrap();

        let path1 = file1.to_string_lossy().into_owned();
        let path2 = file2.to_string_lossy().into_owned();

        let engine = create_engine();
        engine.index_text(&path1, "fast red formula car");
        engine.index_text(&path2, "speed blue formula car");

        let res = engine.search("formula");
        assert_eq!(res.len(), 2);

        // Active deletion
        engine.delete_document_by_path(&path1);
        let res = engine.search("formula");
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], path2);

        // Lazy deletion: delete file2 from actual storage/disk
        let _ = std::fs::remove_file(&file2);

        let res = engine.search("formula");
        assert_eq!(res.len(), 0);

        // Verify registry is cleaned up
        let core_ref = engine.core();
        let read_index = core_ref.index.read().unwrap();
        assert!(read_index.document_registry.is_empty());

        let _ = std::fs::remove_file(file1);
    }
}
