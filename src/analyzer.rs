// analyzer.rs

pub struct TextAnalyzer {
    stop_words: Vec<String>,
}

impl TextAnalyzer {
    pub fn new() -> Self {
        TextAnalyzer { stop_words: vec![
            "the".to_string(), "is".to_string(), "in".to_string(), 
            "at".to_string(), "which".to_string(), "on".to_string()
            ], 
        }
    }

    pub fn analyze(&self, text: &str) -> Vec<String> {
        let mut tokens = Vec::new();

        let raw_words = str::split_whitespace(&text.to_lowercase());

        for word in raw_words {
            let clean_word: &str = todo!("IMPL: Strip punctuation marks and shii");

            if clean_word.empty() {
                continue;
            }

            if self.is_stop_word(&clean_word) {
                continue;
            }

            let stemmed_word = self.apply_basic_stemming(&clean_word);

            tokens.push(stemmed_word);
        }

        tokens
    }

    fn is_stop_word(&self, word: &str) -> bool {
        for stopper in &self.stop_words {
            if word == stopper {
                return true;
            }
        }

        false
    }

    fn apply_basic_stemming(&self, word: &str) -> String {
        let stemmed = todo!("IMPL: If ends with a suffix, then strip suffix")

        stemmed.to_string()
    }
}