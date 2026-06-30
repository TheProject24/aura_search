# ZynSearch Proto

This directory contains the canonical Protocol Buffers schema for the ZynSearch gRPC API.
Any language with `protoc` or the [Buf CLI](https://buf.build/docs/installation) can generate a
fully-typed client from these files in under a minute.

```
proto/
├── buf.yaml               # Buf module descriptor (lint + breaking-change rules)
├── buf.gen.yaml           # Codegen plugin config (all supported languages)
└── zynsearch/
    └── v1/
        └── zynsearch.proto   # Service contract
```

---

## Service at a Glance

| RPC            | Type             | Purpose                                            |
| -------------- | ---------------- | -------------------------------------------------- |
| `Index`        | unary            | Index a single document                            |
| `Search`       | unary            | Run a search, get all results at once              |
| `Delete`       | unary            | Remove a document by ID or source path             |
| `BulkIndex`    | client-streaming | Stream many documents over one connection          |
| `SearchStream` | server-streaming | Receive results as they score, not after full sort |

---

## Option A — Buf CLI (Recommended)

The Buf CLI handles downloading plugins, linting, and breaking-change detection without
a local `protoc` install.

### Install Buf

```bash
# macOS / Linux via Homebrew
brew install bufbuild/buf/buf

# or via the install script
curl -sSL https://github.com/bufbuild/buf/releases/latest/download/buf-Linux-x86_64 \
  -o /usr/local/bin/buf && chmod +x /usr/local/bin/buf
```

### Generate all languages in one command

Run this from inside the `proto/` directory:

```bash
cd proto
buf generate
```

Generated stubs land in `proto/gen/<language>/`.

### Lint the schema

```bash
cd proto
buf lint
```

### Check for breaking changes against main

```bash
cd proto
buf breaking --against '.git#branch=main'
```

---

## Option B — Raw `protoc`

If you prefer not to use the Buf CLI, you can generate a single language at a time
with the standard `protoc` compiler.

### Prerequisites

Install `protoc` from https://github.com/protocolbuffers/protobuf/releases and the
appropriate language plugin for your target.

All examples below are run from the **repository root** with the `proto/` directory
as the import root.

---

### Go

```bash
# Install plugins
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

protoc \
  --proto_path=proto \
  --go_out=gen/go \
  --go_opt=paths=source_relative \
  --go-grpc_out=gen/go \
  --go-grpc_opt=paths=source_relative \
  proto/zynsearch/v1/zynsearch.proto
```

Import path in your module: `github.com/knny/ZynSearch/gen/zynsearch/v1`

---

### Python

```bash
pip install grpcio-tools

python -m grpc_tools.protoc \
  --proto_path=proto \
  --python_out=gen/python \
  --pyi_out=gen/python \
  --grpc_python_out=gen/python \
  proto/zynsearch/v1/zynsearch.proto
```

---

### TypeScript / Node.js (ts-proto)

```bash
npm install -g ts-proto grpc-tools

protoc \
  --proto_path=proto \
  --plugin=./node_modules/.bin/protoc-gen-ts_proto \
  --ts_proto_out=gen/ts \
  --ts_proto_opt=esModuleInterop=true \
  --ts_proto_opt=outputServices=grpc-js \
  proto/zynsearch/v1/zynsearch.proto
```

---

### Java

```bash
protoc \
  --proto_path=proto \
  --java_out=gen/java \
  proto/zynsearch/v1/zynsearch.proto
```

For gRPC stubs, add the `grpc-java` plugin:
https://github.com/grpc/grpc-java/blob/master/compiler/README.md

Package: `com.zynsearch.v1`

---

### C# / .NET

```bash
# Add the Grpc.Tools NuGet package, then:
protoc \
  --proto_path=proto \
  --csharp_out=gen/csharp \
  --grpc_out=gen/csharp \
  --plugin=protoc-gen-grpc=<path/to/grpc_csharp_plugin> \
  proto/zynsearch/v1/zynsearch.proto
```

Namespace: `ZynSearch.V1`

---

### Ruby

```bash
gem install grpc-tools

grpc_tools_ruby_protoc \
  --proto_path=proto \
  --ruby_out=gen/ruby \
  --grpc_out=gen/ruby \
  proto/zynsearch/v1/zynsearch.proto
```

Module: `ZynSearch::V1`

---

### PHP

```bash
protoc \
  --proto_path=proto \
  --php_out=gen/php \
  --grpc_out=gen/php \
  --plugin=protoc-gen-grpc=<path/to/grpc_php_plugin> \
  proto/zynsearch/v1/zynsearch.proto
```

Namespace: `ZynSearch\V1`

---

### Rust (tonic / prost)

Rust consumers should let `tonic-build` handle code generation inside `build.rs`
rather than running `protoc` manually.

```toml
# Cargo.toml
[build-dependencies]
tonic-build = "0.12"
prost-build = "0.13"
```

```rust
// build.rs
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(false)   // true if you need the server stubs
        .build_client(true)
        .compile_protos(
            &["proto/zynsearch/v1/zynsearch.proto"],
            &["proto"],
        )?;
    Ok(())
}
```

Then in your code:

```rust
pub mod zynsearch {
    pub mod v1 {
        tonic::include_proto!("zynsearch.v1");
    }
}
```

---

## Versioning & Stability

| Version        | Status                                                      |
| -------------- | ----------------------------------------------------------- |
| `zynsearch.v1` | **Stable** — breaking changes go through a new `v2` package |

All backward-incompatible changes are validated by `buf breaking` in CI before merge.
Field additions and new RPCs are non-breaking and will be added in-place.
