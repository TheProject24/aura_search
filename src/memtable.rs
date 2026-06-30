use crossbeam_skiplist::SkipMap;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct MemTable {
    board: SkipMap<u32, String>,
    current_size_bytes: AtomicUsize,
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            board: SkipMap::new(),
            current_size_bytes: AtomicUsize::new(0),

        }
    }

    pub fn insert(&self, doc_id: u32, content: String) {
        let text_size = content.len();
        self.board.insert(doc_id, content);
        self.current_size_bytes.fetch_add(text_size, Ordering::SeqCst);
    }

    pub fn get_size_bytes(&self) -> usize {
        self.current_size_bytes.load(Ordering::SeqCst)
    }

    pub fn is_full(&self, max_size_bytes: usize) -> bool {
        self.get_size_bytes() >= max_size_bytes
    }

    pub fn extract_all_sorted(&self) -> Vec<(u32, String)> {
        let mut sorted_list = Vec::new();

        for entry in self.board.iter() {
            let doc_id = *entry.key();
            let content = entry.value().clone();

            sorted_list.push((doc_id, content));
        }

        sorted_list
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_concurrent_lock_free_board() {
        let memtable = Arc::new(MemTable::new());
        let mut worker_threads = Vec::new();

        for i in 0..3 {
            let board_clone = memtable.clone();
            let handle = thread::spawn(move || {
                if i == 0 {
                    board_clone.insert(30, "the quick brown fox".to_string());
                } else if i == 1 {
                    board_clone.insert(10, "Jumps over the lazy doc".to_string());
                } else {
                    board_clone.insert(20, "ZynSearch is blindingly fast".to_string());
                }
            });
            worker_threads.push(handle);
        }

        for handle in worker_threads {
            handle.join().unwrap();
        }

        assert!(memtable.get_size_bytes() > 0);

        let sorted_data = memtable.extract_all_sorted();

        assert_eq!(sorted_data.len(), 3);
        assert_eq!(sorted_data[0].0, 10);
        assert_eq!(sorted_data[1].0, 20);
        assert_eq!(sorted_data[2].0, 30);
    }
}
