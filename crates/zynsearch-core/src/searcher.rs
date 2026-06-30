use std::collections::HashSet;
use crate::index::InvertedIndex;
use crate::analyzer::{TextAnalyzer};
use crate::bm25::Bm25Scorer;
use crate::top_k::SearchResult;
use std::collections::HashMap;

pub struct SearchEngine<'a> {
    index: &'a InvertedIndex,
    analyzer: &'a TextAnalyzer,
}

impl <'a> SearchEngine<'a> {
    pub fn new(index: &'a InvertedIndex, analyzer: &'a TextAnalyzer) -> Self {
        SearchEngine { index, analyzer }
    }

    pub fn search(&self, raw_query: &str) -> Vec<String> {
        let query_tokens = self.analyzer.analyze(raw_query);

        if query_tokens.is_empty() {
            return Vec::new();
        }

        let mut matching_doc_ids: HashSet<usize> = Vec::new().into_iter().collect();
        let mut is_first_token = true;

        for token in query_tokens {
            if let Some(posting_list) = self.index.index.get(&token) {
                let current_token_docs: HashSet<usize> = posting_list
                    .iter()
                    .map(|p| p.document_id)
                    .filter(|id| !self.index.deleted_documents.contains(id))
                    .collect();

                if is_first_token {
                    matching_doc_ids = current_token_docs;
                    is_first_token = false;
                } else {
                    matching_doc_ids.retain(|id| current_token_docs.contains(id));
                }
                if matching_doc_ids.is_empty() {
                    return Vec::new();
                }
            } else {
                return Vec::new();
            }
        }

        let mut final_results = Vec::new();
        for doc_id in matching_doc_ids {
            if self.index.deleted_documents.contains(&doc_id) {
                continue;
            }
            if let Some(file_path) = self.index.document_registry.get(&doc_id) {
                final_results.push(file_path.clone());
            }
        }
        final_results
    }

    pub fn search_scored(&self, raw_query: &str, shard_id: usize, shard_count: usize, limit: usize) -> Vec<SearchResult> {
        let query_tokens = self.analyzer.analyze(raw_query);
        if query_tokens.is_empty() {
            return Vec::new();
        }

        let mut doc_scores: HashMap<usize, f32> = HashMap::new();
        let total_docs = self.index.document_registry.len() as u64;
        if total_docs == 0 {
            return Vec::new();
        }

        let bm25 = Bm25Scorer::new();
        let doc_lengths = self.doc_lengths();
        let avg_doc_length = if doc_lengths.is_empty() {
            0.0
        } else {
            doc_lengths.values().sum::<u32>() as f32 / doc_lengths.len() as f32
        };

        for token in query_tokens {
            let Some(posting_list) = self.index.index.get(&token) else { continue; };
            let idf = bm25.calculate_idf(total_docs, posting_list.len() as u64);

            for posting in posting_list {
                if self.index.deleted_documents.contains(&posting.document_id) {
                    continue;
                }
                if posting.document_id % shard_count != shard_id {
                    continue;
                }

                let doc_length = *doc_lengths.get(&posting.document_id).unwrap_or(&1);
                let score = bm25.score(idf, posting.frequency as u32, doc_length, avg_doc_length.max(1.0));
                *doc_scores.entry(posting.document_id).or_insert(0.0) += score;
            }
        }

        let mut results: Vec<SearchResult> = doc_scores
            .into_iter()
            .map(|(doc_id, score)| SearchResult { doc_id: doc_id as u32, score })
            .collect();

        results.sort_by(|a, b| b.score.total_cmp(&a.score));
        results.truncate(limit);
        results
    }

    fn doc_lengths(&self) -> HashMap<usize, u32> {
        let mut lengths = HashMap::new();
        for postings in self.index.index.values() {
            for posting in postings {
                *lengths.entry(posting.document_id).or_insert(0) += posting.frequency as u32;
            }
        }
        lengths
    }
}
