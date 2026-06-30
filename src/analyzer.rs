// analyzer.rs

use std::collections::HashSet;

use rust_stemmers::Stemmer;


pub struct TextAnalyzer {
    stemmer: Stemmer,
    stop_words: HashSet<String>,
}

impl TextAnalyzer {
    pub fn new() -> Self {
        let words_vec = stop_words::get(stop_words::LANGUAGE::English);
        let stop_words_set: HashSet<String> = words_vec
            .iter()
            .map(|word| (*word).to_string())
            .collect();

        TextAnalyzer { 
            stemmer: Stemmer::create(rust_stemmers::Algorithm::English),
            stop_words: stop_words_set ,
        }
    }

    pub fn analyze(&self, text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|token| !token.is_empty())
            .filter(|token| !self.stop_words.contains(*token))
            .map(|token| self.stemmer.stem(token).into_owned())
            .collect()
    }
}
