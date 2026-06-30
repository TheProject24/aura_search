# ZynSearch Roadmap

## Phase 1: Storage Layer & Bit-Packed Compression

Standard 32-bit integer arrays will choke your CPU cache lines. The goal here is to squeeze posting lists down to the bit level.
//TODO!

- [x] Implement delta encoding (d-gaps): store document IDs as incremental differences, such as `[1000, 1004, 1012]` becoming `[1000, 4, 8]`, to shrink integer storage.
      //TODO!
- [x] Build a frame-of-reference (FOR) encoder: group document IDs into 128-byte blocks and compress them based on the maximum bits required for the largest value in each block.
      //TODO!
- [x] Integrate Roaring Bitmaps: dynamically swap out FOR compression for highly optimized bitwise bitmaps when handling terms with exceptionally dense document matches.
      //TODO!
- [x] Design a variable-byte (VByte) layout: use bit shifting to compress integers such as term frequencies and payload lengths so small values use 1 byte instead of 4 or 8.

## Phase 2: Lucene-Style Segment Architecture

To eliminate read locks and maximize throughput, the storage engine should move from a single file to an immutable segment architecture.
//TODO!

- [x] Implement a write-ahead log (WAL): log raw document write operations to disk before executing them in memory to improve persistence guarantees.
      //TODO!
- [x] Build an in-memory buffer (MemTable): collect incoming document batches in a lock-free buffer structure before writing them to disk.
      //TODO!
- [x] Design the immutable segment writer: flush the memory buffer to disk as an unchangeable, independent mini inverted index once it reaches a size threshold.
      //TODO!
- [x] Develop a multi-segment reader layer: update search execution logic to query all active on-disk segments concurrently using a work-stealing thread pool such as `rayon`.
      //TODO!
- [x] Write a tiered segment merge policy: build a background worker that continuously monitors small segments and merges them into larger consolidated structures while cleaning up deleted document tags.

## Phase 3: Logarithmic Query Traversal Algorithms

Linear text matching scales poorly. The search layer needs structures that can skip straight to the answer.

//TODO!

- [x] Construct a finite state transducer (FST) dictionary: compress the vocabulary into a deterministically compiled byte array so lookups remain cache-friendly.
      //TODO!
- [x] Embed skip lists in postings blocks: add navigation offsets every 128 document IDs inside the binary schema to speed up set intersection.
      //TODO!
- [x] Implement the Block-Max WAND algorithm: use internal score trackers within postings blocks to skip evaluating document sequences that cannot beat the current top results.
      //TODO!
- [x] Support Boolean query operators: expand the execution core to parse complex query conditions beyond basic sequences, including `MUST`, `SHOULD`, and `MUST_NOT` clauses.

## Phase 4: Relevance Engine & Ranking Mechanics

Speed means little if the first returned result is irrelevant. A statistical ranking layer will make the engine much more useful.

//TODO!

- [x] Add in-memory collection statistics: maintain real-time tracking metrics such as total document counts and average document length.
      //TODO!
- [x] Build a native BM25 scorer module: calculate document weights dynamically using term frequency and document-length normalization.
      //TODO!
- [x] Implement bounded min-heaps: stream document IDs through a small priority queue to extract only the top `K` results instead of sorting everything.
      //TODO!
- [x] Support positional indexing: store positional byte offsets inside posting payloads to allow exact matching for phrase queries.

## Phase 5: Network Protocols & Cluster Sharding

To handle production traffic at scale, the single node should support network protocols and distribution across multiple machines.

//TODO!

- [x] Design a length-prefixed binary wire framing: prepend a 4-byte big-endian length header to outgoing socket operations to isolate TCP data packages cleanly.
      //TODO!
- [x] Expose a multi-protocol socket API: let clients toggle execution parameters via raw text strings, JSON requests, or binary payloads.
      //TODO!
- [x] Implement data partitioning (sharding): distribute document indices across independent cluster instances by hashing incoming source ID values.
      //TODO!
- [x] Construct a scatter-gather query coordinator: route incoming queries across multiple search nodes simultaneously and combine the results into a unified sorted response list.

## Phase 6: Plug-and-Play Distribution Layer

ZynSearch should be consumable by any language or framework without friction. This phase restructures the project into a multi-crate workspace and ships a complete distribution surface covering direct embedding, gRPC, HTTP, and thin language SDKs.

### 6.1 Workspace Restructure

//TODO!

- [x] Restructure the repository into a Cargo workspace with three crates: `zynsearch-core` (pure engine, no network concern), `zynsearch-server` (network layer), and `zynsearch-cli` (thin binary shell over core).
      //TODO!
- [x] Ensure `zynsearch-core` is independently publishable to crates.io so Rust projects can embed the engine directly without running a server.

### 6.2 gRPC Interface

//TODO!

- [x] Author a `.proto` file defining the full ZynSearch service contract: `Index`, `Search`, `Delete`, `BulkIndex` (client streaming), and `SearchStream` (server streaming) RPCs with strongly typed request and response messages.
      //TODO!
- [x] Implement the gRPC server in `zynsearch-server` using `tonic`, wiring each RPC handler into the corresponding `zynsearch-core` engine method.
      //TODO!
- [x] Expose a `BulkIndex` client-streaming RPC to allow high-throughput document ingestion over a single persistent connection without per-document HTTP overhead.
      //TODO!
- [x] Expose a `SearchStream` server-streaming RPC to pipe ranked results back to the client incrementally as they are scored, rather than waiting for a full sort.
      //TODO!
- [x] Ship the `.proto` file as a first-class artifact in the repository so any consumer can generate a fully typed client in their language using standard protoc tooling.

### 6.3 HTTP REST Interface

//TODO!

- [ ] Implement an HTTP layer in `zynsearch-server` using `axum`, exposing `POST /index`, `GET /search`, and `DELETE /index/:id` endpoints with JSON request and response bodies.
      //TODO!
- [ ] Support startup flags `--protocol http` and `--protocol grpc` (and optionally `--protocol both`) so operators choose their transport at deploy time without recompiling.
      //TODO!
- [ ] Return structured JSON error responses with a consistent shape across all HTTP endpoints so SDK authors and consumers can handle errors uniformly.

### 6.4 Language SDKs

//TODO!

- [ ] Build a JavaScript/TypeScript SDK (`zynsearch-js`) as a thin wrapper over the HTTP layer, exposing an idiomatic async client: `client.index()`, `client.search()`, `client.delete()`.
      //TODO!
- [ ] Build a Python SDK (`zynsearch-py`) wrapping the HTTP layer with a synchronous and async interface, publishable to PyPI.
      //TODO!
- [ ] Build a Go SDK (`zynsearch-go`) wrapping the HTTP layer as an importable Go module, publishable to pkg.go.dev.
      //TODO!
- [ ] Ensure all SDKs handle connection errors, timeout configuration, and structured error responses consistently so the developer experience is uniform across languages.
