use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Debug)]
pub struct IncomingDocument {
    pub document_id: String,
    pub text: String,
}

#[derive(Debug)]
pub struct ShardNode {
    pub name: String,
    pub storage_dir: String,
}

impl ShardNode {
    pub fn save_document(&self, doc: &IncomingDocument) {
        println!(
            "-> Saving Document '{}' into shard '{}' (Folder: {})",
            doc.document_id, self.name, self.storage_dir
        );
    }
}

pub struct ClusterOrchestrator {
    pub shards: Vec<ShardNode>,
}

impl ClusterOrchestrator {
    pub fn new(number_of_shards: usize) -> Self {
        let mut shards = Vec::new();

        for i in 0..number_of_shards {
            shards.push(ShardNode {
                name: format!("Node-{}", i),
                storage_dir: format!("./data/shard_{}", i),
            });
        }

        ClusterOrchestrator { shards }
    }

    pub fn calculate_shard_for_document(&self, document_id: &str) -> usize {
        let mut hasher = DefaultHasher::new();

        document_id.hash(&mut hasher);
        let giant_number = hasher.finish();
        let shard_index = giant_number % (self.shards.len() as u64);

        shard_index as usize
    }

    pub fn route_and_save(&self, document: IncomingDocument) {
        let target_index = self.calculate_shard_for_document(&document.document_id);
        let target_shard = &self.shards[target_index];

        println!(
            "Traffic Cop: Document '{}' routed to index {}.",
            document.document_id, target_index
        );

        target_shard.save_document(&document);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_document_sharding() {
        let orchestrator = ClusterOrchestrator::new(3);

        let doc1 = IncomingDocument { document_id: "ZYN-001".to_string(), text: "apple".to_string() };
        let doc2 = IncomingDocument { document_id: "ZYN-002".to_string(), text: "banana".to_string() };
        let doc3 = IncomingDocument { document_id: "ZYN-003".to_string(), text: "cherry".to_string() };

        orchestrator.route_and_save(doc1);
        orchestrator.route_and_save(doc2);
        orchestrator.route_and_save(doc3);

        let first_check = orchestrator.calculate_shard_for_document("ZYN-001");
        let second_check = orchestrator.calculate_shard_for_document("ZYN-001");

        assert_eq!(first_check, second_check)
    }
}
