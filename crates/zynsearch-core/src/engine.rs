// engine.rs

use std::sync::{Arc, RwLock};
use std::time::Duration;
use crate::index::InvertedIndex;
use crate::index::DocumentSourceKind;
use crate::analyzer::TextAnalyzer;
use crate::searcher::SearchEngine;
use crate::top_k::SearchResult;
use crate::document_ingest;
use crate::crawler::DirectoryCrawler;
use std::path::Path;

pub struct SearchEngineCore {
    pub index: Arc<RwLock<InvertedIndex>>,
    pub analyzer: Arc<TextAnalyzer>,
}

impl SearchEngineCore {
    pub fn new() -> Self {
        SearchEngineCore { 
            index: Arc::new(RwLock::new(InvertedIndex::new())), 
            analyzer: Arc::new(TextAnalyzer::new()) 
        }
    }

    pub fn ingest_document(&self, source_id: &str, source_kind: DocumentSourceKind, tokens: Vec<String>) {
        let mut write_guard = self.index.write().unwrap();

        let doc_id = write_guard.register_document(source_id, source_kind);
        write_guard.add_document(doc_id, tokens);
    }

    pub fn execute_search(&self, raw_query: &str) -> Vec<String> {
        let matching_paths = {
            let read_guard = self.index.read().unwrap();
            let searcher = SearchEngine::new(&read_guard, &*self.analyzer);
            searcher.search(raw_query)
        };

        let mut to_delete = Vec::new();
        let mut valid_paths = Vec::new();

        for path in matching_paths {
            let source_kind = {
                let read_guard = self.index.read().unwrap();
                read_guard
                    .document_registry
                    .iter()
                    .find(|(_, stored_path)| **stored_path == path)
                    .and_then(|(&doc_id, _)| read_guard.document_metadata.get(&doc_id).map(|m| m.source_kind))
                    .unwrap_or(DocumentSourceKind::Opaque)
            };

            if self.is_missing_live_document(source_kind, &path) {
                to_delete.push(path);
            } else {
                valid_paths.push(path);
            }
        }

        if !to_delete.is_empty() {
            let mut write_guard = self.index.write().unwrap();
            for path in to_delete {
                let doc_id_to_del = write_guard.document_registry.iter()
                    .find(|(_, p)| **p == path)
                    .map(|(&id, _)| id);
                if let Some(id) = doc_id_to_del {
                    write_guard.delete_document(id);
                }
            }
        }

        valid_paths
    }

    pub fn execute_search_for_shard(
        &self,
        raw_query: &str,
        shard_id: usize,
        shard_count: usize,
        limit: usize,
    ) -> Vec<SearchResult> {
        let results = {
            let read_guard = self.index.read().unwrap();
            let searcher = SearchEngine::new(&read_guard, &*self.analyzer);
            searcher.search_scored(raw_query, shard_id, shard_count, limit)
        };

        let mut to_delete = Vec::new();
        let mut valid_results = Vec::new();

        {
            let read_guard = self.index.read().unwrap();
            for result in &results {
                if let Some(path) = read_guard.document_registry.get(&(result.doc_id as usize)) {
                    let source_kind = read_guard
                        .document_metadata
                        .get(&(result.doc_id as usize))
                        .map(|m| m.source_kind)
                        .unwrap_or(DocumentSourceKind::Opaque);

                    if self.is_missing_live_document(source_kind, path) {
                        to_delete.push(result.doc_id as usize);
                    } else {
                        valid_results.push(*result);
                    }
                } else {
                    to_delete.push(result.doc_id as usize);
                }
            }
        }

        if !to_delete.is_empty() {
            let mut write_guard = self.index.write().unwrap();
            for id in to_delete {
                write_guard.delete_document(id);
            }
        }

        valid_results
    }

    pub fn delete_document(&self, doc_id: usize) {
        let mut write_guard = self.index.write().unwrap();
        write_guard.delete_document(doc_id);
    }

    pub fn delete_document_by_path(&self, path: &str) {
        let mut write_guard = self.index.write().unwrap();
        let doc_id_to_del = write_guard.document_registry.iter()
            .find(|(_, p)| **p == path)
            .map(|(&id, _)| id);
        if let Some(id) = doc_id_to_del {
            write_guard.delete_document(id);
        }
    }

    pub fn cleanup_non_existent_documents(&self) {
        let mut to_delete = Vec::new();
        {
            let read_guard = self.index.read().unwrap();
            for (&doc_id, metadata) in &read_guard.document_metadata {
                if self.is_missing_live_document(metadata.source_kind, &metadata.source_id) {
                    to_delete.push(doc_id);
                }
            }
        }

        if !to_delete.is_empty() {
            let mut write_guard = self.index.write().unwrap();
            for doc_id in to_delete {
                write_guard.delete_document(doc_id);
            }
        }
    }

    pub fn ingest_document_text(&self, path: &str, raw_text: &str) {
        let tokens = self.analyzer.analyze(raw_text);
        self.ingest_document(path, DocumentSourceKind::Opaque, tokens);
    }

    pub fn ingest_corpus_dir(&self, corpus_dir: &Path) -> Result<Vec<String>, String> {
        let crawler = DirectoryCrawler::new(corpus_dir, document_ingest::allowed_extensions());
        let mut indexed = Vec::new();

        for path_buf in crawler.run() {
            match document_ingest::normalize_for_indexing(&path_buf) {
                Ok(normalized) => {
                    let path_str = path_buf.to_string_lossy().into_owned();
                    self.ingest_document(&path_str, DocumentSourceKind::Filesystem, self.analyzer.analyze(&normalized));
                    indexed.push(path_str);
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

        Ok(indexed)
    }

    pub fn start_periodic_cleanup(self: &Arc<Self>, interval: Duration) {
        let core_clone = self.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(interval);
                core_clone.cleanup_non_existent_documents();
            }
        });
    }

    fn is_missing_live_document(&self, source_kind: DocumentSourceKind, source_id: &str) -> bool {
        match source_kind {
            DocumentSourceKind::Opaque => false,
            DocumentSourceKind::S3Object => false,
            DocumentSourceKind::Filesystem => !Path::new(source_id).exists(),
        }
    }
}
