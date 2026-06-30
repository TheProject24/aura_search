use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub score: f32,
    pub doc_id: u32,
    pub source_id: Option<String>,
}

impl PartialEq for SearchResult {
    fn eq(&self, other: &Self) -> bool {
        self.doc_id == other.doc_id
    }
}

impl Eq for SearchResult {
    
}

impl Ord for SearchResult {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score.total_cmp(&other.score).then_with(|| other.doc_id.cmp(&self.doc_id))
    }
}

impl PartialOrd for SearchResult {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct TopKCollector {
    capacity: usize,
    min_heap: BinaryHeap<Reverse<SearchResult>>,
}

impl TopKCollector {
    pub fn new(capacity: usize) -> Self {
        TopKCollector { capacity, min_heap: BinaryHeap::with_capacity(capacity) }
    }

    pub fn collect(&mut self, doc_id: u32, score: f32, source_id: Option<String>) {
        let new_ticket = SearchResult { score, doc_id, source_id };

        if self.min_heap.len() < self.capacity {
            self.min_heap.push(Reverse(new_ticket));
        } else {
            if let Some(weakest_vip) = self.min_heap.peek() {
                let weakest_score = weakest_vip.0.score;
                if score > weakest_score {
                    self.min_heap.pop();
                    self.min_heap.push(Reverse(new_ticket));
                }
            }
        }
    }

    pub fn into_sorted_vec(self) -> Vec<SearchResult> {
        let mut final_results: Vec<SearchResult> = self.min_heap
            .into_iter()
            .map(|reversed_ticket| reversed_ticket.0)
            .collect();

        final_results.sort_by(|a, b| b.score.total_cmp(&a.score));

        final_results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_top_k_collector() {
        let mut bouncer = TopKCollector::new(3);

        bouncer.collect(1, 15.0, None);
        bouncer.collect(2, 5.0, None);
        bouncer.collect(3, 42.0, None);
        bouncer.collect(4, 8.0, None);
        bouncer.collect(5, 30.0, None);
        bouncer.collect(6, 2.0, None);

        let winners = bouncer.into_sorted_vec();

        assert_eq!(winners.len(), 3);
        assert_eq!(winners[0].doc_id, 3);
        assert_eq!(winners[1].doc_id, 5);
        assert_eq!(winners[2].doc_id, 1);

        println!("Page 1 search results: ");
        for rank in winners {
            println!("Doc ID: {} | Score: {}", rank.doc_id, rank.score);
        }
    }
}
