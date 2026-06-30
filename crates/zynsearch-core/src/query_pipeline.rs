use crate::config::OutputFormat;
use crate::multi_protocol::{ProtocolParser, ZynQuery};
use crate::top_k::{SearchResult, TopKCollector};
use crate::engine::SearchEngineCore;
use std::collections::HashMap;
use std::sync::{mpsc, Arc};
use std::thread;

#[derive(Clone)]
pub struct QueryCoordinator {
    engine: Arc<SearchEngineCore>,
    shard_count: usize,
}

impl QueryCoordinator {
    pub fn new(engine: Arc<SearchEngineCore>, shard_count: usize) -> Self {
        Self { engine, shard_count: shard_count.max(1) }
    }

    pub fn execute(&self, query: ZynQuery) -> Vec<SearchResult> {
        let (tx, rx) = mpsc::channel();

        for shard_id in 0..self.shard_count {
            let tx_clone = tx.clone();
            let engine_clone = Arc::clone(&self.engine);
            let query_clone = query.query_string.clone();
            let limit = query.limit as usize;
            let shard_count = self.shard_count;

            thread::spawn(move || {
                let shard_results = engine_clone.execute_search_for_shard(&query_clone, shard_id, shard_count, limit);
                let _ = tx_clone.send(shard_results);
            });
        }

        drop(tx);

        let mut collector = TopKCollector::new(query.limit as usize);
        for shard_results in rx {
            for result in shard_results {
                collector.collect(result.doc_id, result.score);
            }
        }

        collector.into_sorted_vec()
    }

    pub fn execute_streaming(&self, query: ZynQuery) -> mpsc::Receiver<SearchResult> {
        let (tx, rx) = mpsc::channel();

        for shard_id in 0..self.shard_count {
            let tx_clone = tx.clone();
            let engine_clone = Arc::clone(&self.engine);
            let query_clone = query.query_string.clone();
            let limit = query.limit as usize;
            let shard_count = self.shard_count;

            thread::spawn(move || {
                let shard_results = engine_clone.execute_search_for_shard(&query_clone, shard_id, shard_count, limit);
                for result in shard_results {
                    if tx_clone.send(result).is_err() {
                        break;
                    }
                }
            });
        }

        drop(tx);
        rx
    }
}

pub fn parse_query(payload: &[u8]) -> Result<ZynQuery, String> {
    ProtocolParser::parse_incoming_payload(payload)
}

pub fn format_results(results: &[SearchResult], format: OutputFormat) -> Vec<u8> {
    match format {
        OutputFormat::Text => {
            let mut text = format!("Found {} matches:\n", results.len());
            for result in results {
                text.push_str(&format!(" -> doc {} (score {:.3})\n", result.doc_id, result.score));
            }
            text.into_bytes()
        }
        OutputFormat::Json => {
            let payload = results
                .iter()
                .map(|result| serde_json::json!({ "doc_id": result.doc_id, "score": result.score }))
                .collect::<Vec<_>>();
            serde_json::to_vec(&payload).unwrap_or_default()
        }
        OutputFormat::Binary => {
            bincode::serialize(results).unwrap_or_default()
        }
    }
}

pub fn build_shard_counts_by_doc_id(doc_ids: &[usize], shard_count: usize) -> HashMap<usize, usize> {
    let mut shard_counts = HashMap::new();
    for &doc_id in doc_ids {
        *shard_counts.entry(doc_id % shard_count.max(1)).or_insert(0) += 1;
    }
    shard_counts
}
