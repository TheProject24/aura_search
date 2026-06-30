use std::collections::HashSet;

#[derive(Debug, PartialEq)]
pub enum Operator {
    Must,
    Should,
    MustNot,
}

#[derive(Debug)]
pub struct QueryClause {
    pub operator: Operator,
    pub word: String,
}

pub struct BooleanQuery {
    pub clauses: Vec<QueryClause>,
}

impl BooleanQuery {
    pub fn execute<F>(&self, fetch_documents_for_word: F) -> HashSet<u32>
    where F: Fn(&str) -> HashSet<u32>,
    {
        let mut must_bucket: Option<HashSet<u32>> = None;
        let mut should_bucket = HashSet::new();
        let mut must_not_bucket = HashSet::new();

        for clause in &self.clauses {
            let matching_docs = fetch_documents_for_word(&clause.word);

            match clause.operator {
                Operator::Must => {
                    if must_bucket.is_none() {
                        must_bucket = Some(matching_docs);
                    } else {
                        must_bucket.as_mut().unwrap().retain(|doc_id| matching_docs.contains(doc_id));
                    }
                }
                Operator::Should => {
                    should_bucket.extend(matching_docs);
                }
                Operator::MustNot => {
                    must_not_bucket.extend(matching_docs);
                }
            }
        }

        let mut final_results = if let Some(must) = must_bucket { must } else { should_bucket };

        for forbidden_doc in must_not_bucket {
            final_results.remove(&forbidden_doc);
        }

        final_results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_boolean_ice_cream_query() {

        let mock_database = |word: &str| -> HashSet<u32> {
            match word {
                "chocolate" => HashSet::from([1, 2]),
                "sprinkles" => HashSet::from([1, 3]),
                "peanuts"   => HashSet::from([2]),
                _           => HashSet::new(),
            }
        };

        let query = BooleanQuery {
            clauses: vec![
                QueryClause { operator: Operator::Must, word: "chocolate".to_string() },
                QueryClause { operator: Operator::Should, word: "sprinkles".to_string() },
                QueryClause { operator: Operator::MustNot, word: "peanuts".to_string() },
            ],
        };

        let results = query.execute(mock_database);
        assert_eq!(results.len(), 1);
        assert!(results.contains(&1)); // Only Document 1 perfectly survived!
    }
}
