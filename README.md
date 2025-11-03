# Parquet-Swift

A native Swift implementation of the Apache Parquet columnar storage format.

[![Swift Version](https://img.shields.io/badge/Swift-5.9+-orange.svg)](https://swift.org)
[![Platforms](https://img.shields.io/badge/Platforms-macOS%20%7C%20iOS%20%7C%20Linux-blue.svg)](https://swift.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)

## Status

🚧 **In Active Development** - Phase 1 (Practical Reader)

Current milestone: **M1.10 - File Reader API** ✅
Last completed: M1.9 - Column Reader ✅

**Phase 1 Complete!** All core reading components implemented. See [implementation roadmap](docs/implementation-roadmap.md) for next steps.

### Known Limitations

Phase 1 supports a **minimal subset** of Parquet:
- ✅ parquet-mr generated files (Spark, Hive, parquet-mr tools)
- ✅ PLAIN encoding only
- ✅ UNCOMPRESSED or GZIP compression
- ✅ Required (non-nullable) primitive columns
- ❌ PyArrow-generated files (metadata incompatibility)
- ❌ Dictionary encoding (most common for strings)
- ❌ Snappy compression (most common in production)
- ❌ Nullable columns

See [docs/limitations.md](docs/limitations.md) for complete details and workarounds.

### ⚠️ Pre-1.0 API Changes

This library is under active development and the API may change between milestones:

- **M1.6 (Current)**: `ParquetFileReader.readMetadata()` returns `FileMetadata` wrapper instead of raw `ThriftFileMetaData`. Use the new wrapper types for cleaner, more idiomatic Swift API.
  - Before: `let thrift = try ParquetFileReader.readMetadata(from: url)` → `ThriftFileMetaData`
  - After: `let metadata = try ParquetFileReader.readMetadata(from: url)` → `FileMetadata`
  - Migration: Most properties have the same names. Access schema via `metadata.schema` directly.

## Features (Planned)

### Phase 1 (Complete ✅) - Practical Reader
- ✅ Project setup and architecture
- ✅ Core type system
- ✅ Thrift metadata parsing (Compact Binary Protocol)
- ✅ Schema representation and tree building
- ✅ Basic I/O layer (file reading, buffered access)
- ✅ Metadata wrapper API (idiomatic Swift types)
- ✅ PLAIN encoding for all primitive types
- ✅ GZIP + UNCOMPRESSED codecs
- ✅ Column readers (Int32, Int64, Float, Double, String)
- ✅ File Reader API (instance-based, type-safe)

**Deferred to Phase 2:**
- Dictionary encoding
- Snappy compression
- Optional columns (null handling)
- Nested types

### Phase 2 (6-8 weeks) - Full Reader
- Nested types (lists, maps, structs)
- Delta encodings
- Complete RLE implementation

### Phase 3 (8 weeks) - Writer
- File writing support
- All encodings
- Statistics generation

### Phase 4 (6 weeks) - Advanced Features
- Bloom filters
- Page index
- Async I/O
- Performance optimizations

## Installation

### Swift Package Manager

Add to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/[user]/parquet-swift.git", from: "0.1.0")
]
```

Then import:

```swift
import Parquet
```

## Quick Start

```swift
import Parquet

// Open a Parquet file
let reader = try ParquetFileReader(url: fileURL)
defer { try? reader.close() }

print("Rows: \(reader.metadata.numRows)")
print("Columns: \(reader.metadata.schema.columnCount)")

// Access a row group
let rowGroup = try reader.rowGroup(at: 0)

// Read typed columns
let idColumn = try rowGroup.int32Column(at: 0)
let ids = try idColumn.readAll()  // Returns flattened [Int32] array

let nameColumn = try rowGroup.stringColumn(at: 4)
let names = try nameColumn.readAll()  // Returns flattened [String] array

// For large columns, use readBatch() to control memory:
let batch = try idColumn.readBatch(count: 1000)  // Read in chunks

print("First 10 IDs: \(ids.prefix(10))")
print("First 10 names: \(names.prefix(10))")
```

**Note:** Phase 1 supports PLAIN encoding with UNCOMPRESSED or GZIP compression only. See [Known Limitations](#known-limitations) for details.

## Requirements

- Swift 5.9 or later
- macOS 13+ (Ventura) / iOS 16+ / watchOS 9+ / tvOS 16+
- Linux support planned

### Dependencies

- **GZIP**: Built-in via Foundation's `Compression` framework
- **Snappy**: Optional, via system library or Swift package

## Documentation

- [Implementation Roadmap](docs/implementation-roadmap.md) - Development plan and timeline
- [Phase Review](docs/phase-review.md) - Detailed phase breakdown
- [C++ Analysis](docs/cpp-analysis.md) - Analysis of Apache Arrow C++ implementation
- [Swift Package Design](docs/swift-package-design.md) - Architecture and design
- [API Guide](docs/api-guide.md) - User-facing API documentation (draft)

## Project Structure

```
parquet-swift/
├── Package.swift              # Swift Package Manager manifest
├── Sources/
│   └── Parquet/              # Main library
│       ├── Core/             # Core types and protocols
│       ├── Schema/           # Schema representation
│       ├── Metadata/         # File metadata
│       ├── Thrift/           # Thrift serialization
│       ├── IO/               # I/O abstractions
│       ├── Compression/      # Compression codecs
│       ├── Encoding/         # Encoding/decoding
│       ├── Reader/           # Reading API
│       └── Writer/           # Writing API (Phase 3)
├── Tests/
│   └── ParquetTests/         # Test suite
└── docs/                     # Documentation
```

## Development

### Building

```bash
swift build
```

### Testing

```bash
swift test
```

### Running Examples

```bash
# Coming soon
swift run ParquetRead example.parquet
```

## Roadmap

**Phase 1** (Current): Practical Reader - 10 weeks
- Goal: Read 80%+ of real Parquet files (flat schema)
- Deliverable: Alpha release

**Phase 2**: Full Reader - 6-8 weeks
- Goal: Complete reader with nested types
- Deliverable: Beta release

**Phase 3**: Writer - 8 weeks
- Goal: Write Parquet files
- Deliverable: 1.0 release

**Phase 4**: Advanced Features - 6 weeks
- Goal: Production-ready optimizations

See [implementation roadmap](docs/implementation-roadmap.md) for details.

## Contributing

Contributions are welcome! This project is in early development.

Please see:
- [Phase Review](docs/phase-review.md) for current work
- [Implementation Roadmap](docs/implementation-roadmap.md) for upcoming milestones

## Reference Implementation

This project ports the Apache Arrow C++ Parquet implementation to Swift:
- Reference: [apache/arrow/cpp/src/parquet](https://github.com/apache/arrow/tree/master/cpp/src/parquet)
- Spec: [apache/parquet-format](https://github.com/apache/parquet-format)

## Resources

- [Apache Parquet](https://parquet.apache.org/)
- [Parquet Format Specification](https://github.com/apache/parquet-format)
- [Apache Arrow](https://arrow.apache.org/)
- [Parquet Testing Repository](https://github.com/apache/parquet-testing)

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Acknowledgments

- Apache Arrow and Parquet communities
- C++ implementation authors and contributors

---

**Status**: Alpha - Phase 1 Complete ✅
**Current Phase**: Phase 1 - Practical Reader ✅
**Next Phase**: Phase 2 - Full Reader (Dictionary encoding, Snappy, nulls, nested types)
