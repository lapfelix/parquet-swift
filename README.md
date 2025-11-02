# Parquet-Swift

A native Swift implementation of the Apache Parquet columnar storage format.

[![Swift Version](https://img.shields.io/badge/Swift-5.9+-orange.svg)](https://swift.org)
[![Platforms](https://img.shields.io/badge/Platforms-macOS%20%7C%20iOS%20%7C%20Linux-blue.svg)](https://swift.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)

## Status

🚧 **In Active Development** - Phase 1 (Practical Reader)

Current milestone: **M1.1 - Project Setup** ✅

See [implementation roadmap](docs/implementation-roadmap.md) for detailed development plan.

## Features (Planned)

### Phase 1 (10 weeks) - Practical Reader
- ✅ Project setup and architecture
- 🚧 Core type system
- 🚧 Thrift metadata parsing
- 🚧 Schema representation
- 🚧 PLAIN + DICTIONARY encoding
- 🚧 Optional columns (null handling)
- 🚧 GZIP + Snappy compression
- 🚧 Read flat-schema Parquet files

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

## Quick Start (Coming Soon)

```swift
import Parquet

// Read a Parquet file
let reader = try ParquetFileReader(path: "data.parquet")
print("Rows: \(reader.metadata.numRows)")

// Read a column
let rowGroup = try reader.rowGroup(at: 0)
let column = try rowGroup.column(at: 0)
let values: [Int32] = try column.read(count: 100)
```

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

**Status**: Pre-alpha, active development
**Current Phase**: Phase 1 - Project Setup ✅
**Next Milestone**: M1.2 - Core Type System
