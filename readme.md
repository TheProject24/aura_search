# ZynSearch

> A lightweight local search engine written in Rust for indexing and querying plain text and Markdown files.

ZynSearch is a small but expressive document search engine that walks a directory tree, extracts text from supported files, tokenizes content, builds an in-memory inverted index, and lets you run interactive searches from the terminal.

It is intentionally simple in architecture, which makes it a great project for learning:

- how a crawler discovers files
- how a parser normalizes content
- how an analyzer turns text into searchable tokens
- how an inverted index stores postings
- how a search layer intersects query terms
- how persistence can serialize and deserialize search data

The codebase is compact, but the design is clear enough to grow into a richer search system.

## Table Of Contents

- [What ZynSearch Does](#what-ZynSearch-does)
- [Project Highlights](#project-highlights)
- [Architecture Overview](#architecture-overview)
- [How It Works](#how-it-works)
- [Supported File Types](#supported-file-types)
- [Tokenization And Search Rules](#tokenization-and-search-rules)
- [Persistence Format](#persistence-format)
- [Repository Layout](#repository-layout)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Examples](#examples)
- [Implementation Notes](#implementation-notes)
- [Known Limitations](#known-limitations)
- [Future Ideas](#future-ideas)

## What ZynSearch Does

ZynSearch scans the current directory recursively, finds files with allowed extensions, reads their contents, converts Markdown or plain text into searchable text, analyzes the text into tokens, and stores those tokens in an inverted index.

At query time, it accepts a search string from standard input, tokenizes the query using the same analyzer, and returns documents that contain all query terms.

In simple terms:

1. discover documents
2. clean and normalize content
3. build a searchable index
4. accept a query
5. return matching files

## Project Highlights

- Written in Rust 2024 edition
- Uses a clean modular layout with dedicated files for crawling, parsing, indexing, searching, storage, and analysis
- Supports `.txt` and `.md` files out of the box
- Applies simple stop-word filtering and heuristic stemming
- Provides an interactive terminal-based search loop
- Includes a persistence layer for saving and loading the index

## Architecture Overview

ZynSearch is organized around a few core components:

- `DirectoryCrawler` finds files to ingest
- `DocumentParser` cleans raw file content
- `TextAnalyzer` normalizes text and generates tokens
- `InvertedIndex` stores terms and postings
- `SearchEngineCore` coordinates ingestion and search
- `SearchEngine` executes query matching
- `StorageManager` serializes and deserializes indexed data
- `ZeroCopyReader` offers a byte-slice-based lookup path for postings

The current runtime flow in `src/main.rs` is:

```text
directory crawl -> read file -> parse -> analyze -> ingest -> query loop
```

## How It Works

### 1. Crawling

`src/crawler.rs` uses [`walkdir`](https://docs.rs/walkdir) to recursively traverse the root path and collect files with approved extensions.

The crawler:

- visits each filesystem entry
- keeps only regular files
- filters by extension
- returns a `Vec<PathBuf>` of discovered documents

### 2. Parsing

`src/parser.rs` defines a `DocumentParser` trait with two implementations:

- `PlainTextParser` returns the raw text unchanged
- `MarkdownParser` performs a light cleanup pass

The Markdown parser currently:

- trims whitespace
- skips empty lines
- skips fenced code block markers
- removes bold markers by replacing `**`

This keeps the parsing logic intentionally lightweight while still making Markdown reasonably searchable.

### 3. Analysis

`src/analyzer.rs` converts text into tokens.

It performs:

- lowercase normalization
- splitting on whitespace and ASCII punctuation
- removal of stop words
- very simple heuristic stemming

Examples of stemming behavior:

- `running` -> `runn`
- `called` -> `call`
- `happiness` -> `happi`
- `documents` -> `document`

This is not a full linguistic stemmer, but it is fast and easy to understand.

### 4. Indexing

`src/index.rs` stores search data in an inverted index:

- a `HashMap<String, Vec<Posting>>` maps each term to a list of postings
- each `Posting` contains a `document_id` and `frequency`
- a separate registry maps document IDs back to their original file paths

When a document is ingested:

1. it receives a document ID
2. its tokens are counted
3. each unique term creates or extends a posting list
4. the posting records document frequency information

### 5. Searching

`src/searcher.rs` performs AND-style search over the query tokens.

If a query contains multiple terms, ZynSearch returns only documents that contain every term after analysis.

That means the query:

```text
rust search engine
```

behaves like:

- tokenize the query
- look up each token in the index
- intersect the matching document ID sets
- map document IDs back to file paths

### 6. Persistence

`src/storage.rs` contains a binary serializer/deserializer for the index.

The format currently writes:

- a file signature
- document count
- document registry entries
- term count
- term entries
- posting lists

It also includes a `ZeroCopyReader` helper for reading postings from a byte slice without fully reconstructing the entire index.

## Supported File Types

By default, the app indexes:

- `.txt`
- `.md`

You can change the allowed extensions in `src/main.rs`:

```rust
let allowed_extensions = vec!["txt".to_string(), "md".to_string()];
```

## Tokenization And Search Rules

ZynSearch currently follows a fairly strict, predictable text model.

### Analyzer rules

- everything is converted to lowercase
- tokens are split on whitespace and ASCII punctuation
- tokens shorter than 2 characters are skipped
- stop words are removed
- simple suffix stripping is applied for common endings

### Search rules

- search is token-based
- search terms are analyzed the same way as document text
- multiple terms are combined using intersection logic
- if any query token is missing from the index, the result is empty

### Stop words

The default stop-word list includes common high-frequency words such as:

- `the`
- `is`
- `in`
- `at`
- `and`
- `or`
- `with`
- `for`

## Persistence Format

The serializer writes a compact binary layout with little-endian integers.

High-level structure:

```text
signature
document_count
  document_id
  path_length
  path_bytes
term_count
  term_length
  term_bytes
  posting_count
    posting document_id
    posting frequency
```

Important note:

- the persistence layer is binary, not human-readable
- the format assumes the same internal type sizes and structure on read and write
- `StorageManager` and `ZeroCopyReader` are currently present as infrastructure for future save/load workflows

## Repository Layout

```text
.
├── Cargo.toml
├── readme.md
├── index.html
├── docs/
│   └── ZynSearch PRD.pdf
├── src/
│   ├── analyzer.rs
│   ├── crawler.rs
│   ├── engine.rs
│   ├── index.rs
│   ├── main.rs
│   ├── parser.rs
│   ├── searcher.rs
│   └── storage.rs
└── test_corpus/
    ├── doc1.txt
    ├── doc2.md
    └── doc3.txt
```

## Getting Started

### Prerequisites

- Rust toolchain
- `cargo`

### Build

```bash
cargo build
```

### Run

```bash
cargo run
```

When the program starts, it:

- scans the current directory
- indexes files with allowed extensions
- enters an interactive search prompt

### Search

Type a query and press Enter.

Examples:

```text
rust
markdown parser
search engine
```

Type `exit` or `quit` to leave the prompt.

## Usage

1. Place searchable `.txt` or `.md` files in the directory tree you want to scan.
2. Run the application with `cargo run`.
3. Wait for indexing to complete.
4. Enter search terms at the prompt.
5. Review the matching file paths.

## Examples

### Example Query

```text
search engine
```

Expected behavior:

- document text is analyzed into tokens
- the query is analyzed into tokens
- only documents containing both `search` and `engine` are returned

### Example Markdown Handling

For a Markdown file containing:

```md
# Notes

This is **important**.
```

The parser keeps the content searchable by trimming lines and removing some formatting markers.

## Implementation Notes

### `src/main.rs`

This file orchestrates the whole application:

- crawls the filesystem
- reads files into strings
- chooses a parser by extension
- analyzes the parsed text
- ingests the document into the engine
- starts the interactive query loop

### `src/engine.rs`

`SearchEngineCore` wraps the shared index and analyzer in `Arc<RwLock<_>>` and coordinates ingestion and search.

### `src/index.rs`

The inverted index stores term postings and the document registry.

### `src/searcher.rs`

The searcher performs token intersection to find documents that match every query token.

### `src/storage.rs`

The storage layer is designed for binary persistence and zero-copy reads.

### `src/analyzer.rs`

The analyzer performs normalization, filtering, and suffix-based stemming.

### `src/parser.rs`

The parser abstraction makes it easy to support additional document formats later.

### `src/crawler.rs`

The crawler handles recursive discovery of files with allowed extensions.

## Known Limitations

ZynSearch is intentionally minimal, so a few limitations are worth noting:

- search currently behaves like strict AND matching
- scoring/ranking is not implemented
- Markdown parsing is lightweight and not fully compliant
- persistence is binary and tightly coupled to the current data layout
- `ZeroCopyReader` is still an early utility and may need extra safety checks
- search results are returned as file paths only, not snippets or highlighted matches

## Future Ideas

If you want to grow ZynSearch, good next steps would be:

- add OR and phrase search
- add ranking with TF-IDF or BM25
- improve Markdown stripping and HTML handling
- support more file types like `.html`, `.json`, or `.csv`
- add saved index loading on startup
- expose a web UI
- generate snippets and highlight query matches
- benchmark indexing and query time

## License

No license file is currently present in the repository. If you plan to share or publish the project, add one to make usage terms clear.

