// parser.rs

pub trait DocumentParser {
    fn parse(&self, raw_content: &str) -> String;
}

pub struct PlainTextParser;

impl DocumentParser for PlainTextParser {
    fn parse(&self, raw_content: &str) -> String {
        raw_content.to_string()
    }
}

pub struct MarkdownParser;

impl DocumentParser for MarkdownParser {
    fn parse(&self, raw_content: &str) -> String {
        let mut clean_text = String::with_capacity(raw_content.len());

        for line in raw_content.lines() {
            // let stripped_line = todo!("IMPL: Strip Markdown Syntax");
            let trimmed_line = str::trim(line);
            if trimmed_line.is_empty() || trimmed_line.starts_with("```") {
                continue;
            }
            let no_prefix_line = str::trim_start_matches(trimmed_line,"");

            let final_line = str::replace(no_prefix_line, "**", "_");
            clean_text.push_str(&final_line);
            clean_text.push(' ');
        }

        clean_text
    }
}
