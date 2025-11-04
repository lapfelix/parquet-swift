# Parquet-Swift

A native Swift implementation of the Apache Parquet columnar storage format.

[![Swift Version](https://img.shields.io/badge/Swift-5.9+-orange.svg)](https://swift.org)
[![Platforms](https://img.shields.io/badge/Platforms-macOS%20%7C%20iOS%20%7C%20watchOS%20%7C%20tvOS-blue.svg)](https://swift.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/Tests-436%20passing-brightgreen.svg)]()

## Status

🎉 **Version 1.0 - Production Ready!**

Complete implementation of Parquet **reader** and **writer** in pure Swift with full cross-implementation compatibility validated against PyArrow.

### ✅ Reader (R1-R5) - Complete
- All primitive types with PLAIN and RLE_DICTIONARY encoding
- Nullable and required columns (definition levels)
- Nested types: lists, maps, structs (multi-level nesting)
- Complex patterns: lists of structs, maps with list values, struct fields with arrays
- Compression: UNCOMPRESSED, GZIP, Snappy
- **PyArrow compatibility validated**

### ✅ Writer (W7) - Complete
- All primitive column writers with optional/required support
- List writers (single and multi-level nested lists)
- Map writers (map<string, int32/int64/string>)
- Statistics generation (min/max/null count)
- Compression: UNCOMPRESSED, GZIP, Snappy
- **PyArrow validation: ALL PASS** (cross-implementation compatibility confirmed)

**Test Suite:** 436 tests passing, 0 failures

## Features

### Supported Types

**Primitive Types:**
- Int32, Int64, Float, Double, String (UTF-8), Boolean
- Optional and required variants

**Nested Types:**
- **Lists:** `list<T>` with single and multi-level nesting
- **Maps:** `map<K, V>` with string keys and primitive values
- **Structs:** Complex records with nested fields

**Complex Patterns:**
- Lists of structs: `list<struct { fields }>`
- Maps with list values: `map<string, list<T>>`
- Structs with complex children: `struct { map<K,V>, list<T> }`
- Deeply nested combinations

### Encodings

**Reading:**
- PLAIN (all types)
- RLE_DICTIONARY (dictionary encoding)

**Writing:**
- PLAIN (all types)

### Compression

- UNCOMPRESSED
- GZIP (built-in via Foundation)
- Snappy (pure Swift implementation)

### File Compatibility

- ✅ Apache Spark-generated files
- ✅ Apache Hive-generated files
- ✅ PyArrow-generated files
- ✅ parquet-mr generated files
- ✅ **Files written by parquet-swift validated against PyArrow**

## Installation

### Swift Package Manager

Add to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/yourusername/parquet-swift.git", from: "1.0.0")
]
```

Then import:

```swift
import Parquet
```

## Quick Start

### Reading Files

```swift
import Parquet

// Open a Parquet file
let reader = try ParquetFileReader(url: fileURL)
defer { try? reader.close() }

print("Rows: \(reader.metadata.numRows)")
print("Columns: \(reader.metadata.schema.columnCount)")

// Access a row group
let rowGroup = try reader.rowGroup(at: 0)

// Read primitive columns
let idColumn = try rowGroup.int32Column(at: 0)
let ids = try idColumn.readAll()  // [Int32?] for nullable columns

let nameColumn = try rowGroup.stringColumn(at: 1)
let names = try nameColumn.readAll()  // [String?]

// Read nested columns
let tags = try rowGroup.readList(at: ["tags"])  // [Any?]
let attributes = try rowGroup.readMap(at: ["attributes"])  // [[String: Any]?]
let address = try rowGroup.readStruct(at: ["address"])  // [[String: Any]?]

// For large files, read in batches
let batch = try idColumn.readBatch(count: 1000)
```

### Writing Files

```swift
import Parquet

// Create schema
let idField = SchemaElement(
    name: "id",
    elementType: .primitive(physicalType: .int32, logicalType: nil),
    repetitionType: .required,
    fieldId: nil,
    children: [],
    parent: nil,
    depth: 1
)

let nameField = SchemaElement(
    name: "name",
    elementType: .primitive(physicalType: .byteArray, logicalType: .string),
    repetitionType: .optional,
    fieldId: nil,
    children: [],
    parent: nil,
    depth: 1
)

let root = SchemaElement(
    name: "schema",
    elementType: .group(logicalType: nil),
    repetitionType: nil,
    fieldId: nil,
    children: [idField, nameField],
    parent: nil,
    depth: 0
)
idField.parent = root
nameField.parent = root

let schema = Schema(root: root)

// Create writer
let writer = try ParquetFileWriter(url: outputURL)
try writer.setSchema(schema)
writer.setProperties(.default)

// Write data
let rowGroup = try writer.createRowGroup()

// Write required int32 column
let idWriter = try rowGroup.int32ColumnWriter(at: 0)
try idWriter.writeValues([1, 2, 3])
try rowGroup.finalizeColumn(at: 0)

// Write optional string column
let nameWriter = try rowGroup.stringColumnWriter(at: 1)
try nameWriter.writeOptionalValues(["Alice", "Bob", nil])
try rowGroup.finalizeColumn(at: 1)

// Close writer
try writer.close()
```

### Writing Lists

```swift
// Schema: list<int32>
let listWriter = try rowGroup.int32ListColumnWriter(at: 0)
try listWriter.writeValues([
    [1, 2, 3],          // Row 0: normal list
    nil,                // Row 1: NULL list
    [],                 // Row 2: empty list
    [42]                // Row 3: single element
])
try rowGroup.finalizeColumn(at: 0)
```

### Writing Maps

```swift
// Schema: map<string, int32>
let mapWriter = try rowGroup.stringInt32MapColumnWriter(at: 0)
try mapWriter.writeMaps([
    ["a": 1, "b": 2],   // Row 0: normal map
    nil,                // Row 1: NULL map
    [:],                // Row 2: empty map
    ["x": 100]          // Row 3: single entry
])
try rowGroup.finalizeColumn(at: 0)
```

### Writing Structs

For structs, write each field as an independent column:

```swift
// User-defined struct
struct User {
    let name: String?
    let age: Int32?
}

let users = [
    User(name: "Alice", age: 30),
    User(name: "Bob", age: nil),
    User(name: nil, age: 25)
]

// Extract fields
let names = users.map { $0.name }
let ages = users.map { $0.age }

// Write columns sequentially
let nameWriter = try rowGroup.stringColumnWriter(at: 0)
try nameWriter.writeOptionalValues(names)
try rowGroup.finalizeColumn(at: 0)

let ageWriter = try rowGroup.int32ColumnWriter(at: 1)
try ageWriter.writeOptionalValues(ages)
try rowGroup.finalizeColumn(at: 1)
```

## Requirements

- Swift 5.9 or later
- macOS 13+ (Ventura) / iOS 16+ / watchOS 9+ / tvOS 16+

### Dependencies

- **GZIP**: Built-in via Foundation's `Compression` framework
- **Snappy**: Pure Swift implementation via [snappy-swift](https://github.com/codelynx/snappy-swift)
- **No system dependencies required!**

## Documentation

### Core Documentation
- [CHANGELOG.md](CHANGELOG.md) - Version history and changes
- [CLAUDE.md](CLAUDE.md) - Development guide and architecture

### Technical Docs
- [Implementation Roadmap](docs/implementation-roadmap.md) - Development plan
- [Phase Review](docs/phase-review.md) - Detailed phase breakdown
- [C++ Analysis](docs/cpp-analysis.md) - Apache Arrow C++ reference
- [Swift Package Design](docs/swift-package-design.md) - Architecture decisions

## Project Structure

```
parquet-swift/
├── Package.swift              # Swift Package Manager manifest
├── Sources/
│   └── Parquet/              # Main library
│       ├── Core/             # Core types and enums
│       ├── Schema/           # Schema representation
│       ├── Metadata/         # File metadata
│       ├── Thrift/           # Thrift serialization
│       ├── IO/               # I/O abstractions
│       ├── Compression/      # Compression codecs
│       ├── Encoding/         # Encoding/decoding
│       ├── Reader/           # Reading API
│       └── Writer/           # Writing API
├── Tests/
│   └── ParquetTests/         # Test suite (436 tests)
└── docs/                     # Documentation
```

## Development

### Building

```bash
swift build
```

### Testing

```bash
# Run all tests
swift test

# Run specific test class
swift test --filter MapWritingTests

# Run specific test method
swift test --filter Int32ColumnReaderTests.testReadInt32Column
```

### PyArrow Validation

Cross-implementation compatibility is validated against PyArrow:

```bash
# Generate validation files
swift test --filter PyArrowValidationTests.testGenerateAllValidationFiles

# Validate with PyArrow
python3 Tests/ParquetTests/Fixtures/validate_with_pyarrow.py /path/to/validation-files
```

### Developer Setup (Optional)

The Apache Arrow C++ implementation serves as a reference during development but is **not required** for building or testing. To keep the package lean, the C++ code is excluded from the repository.

**For contributors who want the C++ reference:**

```bash
# Clone Apache Arrow C++ reference code (main branch)
./scripts/setup-dev.sh

# Remove reference code when no longer needed
./scripts/cleanup-dev.sh
```

The reference code will be cloned to `third_party/arrow/cpp/src/parquet/`. All documentation already links to GitHub, so this is entirely optional.

## Architecture

### Design Principles

1. **Layered architecture** mirroring Apache Arrow C++
2. **Type-safe API** with Swift wrappers over Thrift metadata
3. **Instance-based lifecycle management** (explicit open/close)
4. **Concrete type readers/writers** (no generic limitations)

### Key Components

**Thrift Layer:**
- Compact Binary Protocol serialization/deserialization
- Metadata reading and writing

**I/O Layer:**
- Reading: `RandomAccessFile`, `BufferedReader`, `ParquetFileReader`
- Writing: `OutputSink`, `FileOutputSink`, `ParquetFileWriter`

**Schema Layer:**
- Tree-based schema representation
- Column metadata with level information

**Page Layer:**
- Reading: `PageReader` (decompression, level decoding)
- Writing: `PageWriter` (compression, level encoding, statistics)

**Column Layer:**
- Primitive readers/writers: Int32, Int64, Float, Double, String, Boolean
- List readers/writers: All primitive types with multi-level nesting
- Map readers/writers: String keys with int32/int64/string values
- Struct reading: High-level API (`readStruct()`)
- Struct writing: Manual field extraction pattern

## Known Limitations

### Writer
- Dictionary encoding not yet implemented (PLAIN only)
- Data Page V2 not supported (V1 only)
- Additional compression codecs (LZ4, ZSTD, BROTLI) not implemented

### Reader & Writer
- Bloom filters not supported
- Page index not supported
- Column encryption not supported

These limitations do not affect most common use cases. Dictionary encoding writer and additional codecs can be added in future versions if needed.

## Roadmap

### Version 1.0 (Current) ✅
- ✅ Full Reader implementation (all phases R1-R5)
- ✅ Full Writer implementation (all phases W2-W7)
- ✅ PyArrow cross-validation
- ✅ 436 tests passing

### Version 1.1 (Future)
- Dictionary encoding writer
- Data Page V2 support
- Performance optimizations (SIMD, vectorization)

### Version 1.2+ (Future)
- Additional compression codecs (LZ4, ZSTD)
- Bloom filters
- Page index
- Swift Concurrency (async/await)

## Contributing

Contributions are welcome! This project follows the Apache Arrow C++ implementation as a reference.

Areas for contribution:
- Dictionary encoding writer
- Additional compression codecs
- Performance optimizations
- Documentation improvements

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
- PyArrow team for cross-validation reference

---

**Version:** 1.0.0
**Status:** Production Ready
**Reader:** ✅ Complete (R1-R5)
**Writer:** ✅ Complete (W7)
**Tests:** 436 passing, 0 failures
**PyArrow Validation:** ✅ ALL PASS
