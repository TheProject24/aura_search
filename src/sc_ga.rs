use std::sync::mpsc;
use std::thread;



#[derive(Debug, Clone)]
pub struct SearchResult {
    pub doc_id: u32,
    pub score: f32,
}

#[derive(Debug, Clone)]
pub struct ShardNode {
    pub id: usize,
}

impl ShardNode {
    pub fn local_search(&self, query: &str, limit: usize) -> Vec<SearchResult> {
        println!("Shard {} is searching for '{}' . . .", self.id, query);

        let mut results = vec![
            SearchResult { doc_id: (self.id * 100 + 1) as u32, score: 10.0 + self.id as f32 },
            SearchResult { doc_id: (self.id * 100 + 2) as u32, score: 5.0 + self.id as f32 },
        ];

        results.truncate(limit);
        results
    }
}

pub struct QueryCoordinator {
    pub shards: Vec<ShardNode>,
}

impl QueryCoordinator {
    pub fn execute_dist_search(&self, query: &str, limit: usize) -> Vec<SearchResult> {
        let (tx, rx) = mpsc::channel();

        for shard in &self.shards {
            let tx_clone = tx.clone();

            let shard_clone = shard.clone();
            let query_string = query.to_string();

            thread::spawn(move || {
                let local_results = shard_clone.local_search(&query_string, limit);
                let _ = tx_clone.send(local_results);
            });
        }

        drop(tx);

        let mut unified_results: Vec<SearchResult> = Vec::new();

        for mut shard_results in rx {
            unified_results.append(&mut shard_results);
        }

        unified_results.sort_by(|a, b| b.score.total_cmp(&a.score));

        unified_results.truncate(limit);

        unified_results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scatter_gather_execution() {
        let coordinator = QueryCoordinator {
            shards: vec![ShardNode { id: 1 }, ShardNode { id: 2 }, ShardNode { id: 3 }],
        };

        let top_k_limit = 3;
        let final_results = coordinator.execute_dist_search("fast car", top_k_limit);
        assert_eq!(final_results.len(), 3);

        println!("\n=== FINAL PAGE 1 ===");
        for (index, result) in final_results.iter().enumerate() {
            println!("Rank {} | Doc ID: {} | Score: {}", index + 1, result.doc_id, result.score);
        }

        assert_eq!(final_results[0].doc_id, 301);
    }
}
