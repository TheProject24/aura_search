use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct Posting {
    pub document_id: usize,
    pub frequency: usize,
}

pub struct InvertedIndex {
    pub index: HashMap<String, Vec<Posting>>,
    pub document_registry: HashMap<usize, String>,
    pub deleted_documents: HashSet<usize>,
    next_document_id: usize,
}

impl InvertedIndex {
    pub fn new() -> Self {
        InvertedIndex {
            index: HashMap::new(),
            document_registry: HashMap::new(),
            deleted_documents: HashSet::new(),
            next_document_id: 0,
        }
    }

    pub fn register_document(&mut self, path: &str) -> usize {
        let doc_id = self.next_document_id;
        self.document_registry.insert(doc_id, path.to_string());
        self.next_document_id += 1;
        doc_id
    }

    pub fn add_document(&mut self, doc_id: usize, tokens: Vec<String>) {
        let mut term_counts: HashMap<String, usize> = HashMap::new();
        for token in tokens {
            let count = term_counts.entry(token).or_insert(0);
            *count += 1;
        }

        for (term, freq) in term_counts {
            let posting = Posting {
                document_id: doc_id,
                frequency: freq,
            };

            self.index
                .entry(term)
                .or_insert_with(Vec::new)
                .push(posting);
        }
    }

    pub fn delete_document(&mut self, doc_id: usize) {
        self.deleted_documents.insert(doc_id);
        self.document_registry.remove(&doc_id);

        for postings in self.index.values_mut() {
            postings.retain(|p| p.document_id != doc_id);
        }
    }
}