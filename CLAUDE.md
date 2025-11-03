# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a native Swift implementation of the Apache Parquet columnar storage format, porting the Apache Arrow C++ implementation. The project is in Phase 2 (Full Reader) development.

**Current Status**: Phase 2 in progress - Snappy compression complete, Dictionary encoding next
**Phase 1**: Complete (Practical Reader)
**Phase 2**: In progress (Snappy ✅, Dictionary 🚧, Nulls 🚧, Nested types 🚧)

## Build & Test Commands

### Build
```bash
swift build
```

No system dependencies or environment variables required!

### Testing
```bash
# Run all tests
swift test

# Run specific test class (use filter)
swift test --filter IntegrationTests

# Run specific test method
swift test --filter Int32ColumnReaderTests.testReadInt32Column
```

### Dependencies
All dependencies are pure Swift and managed by Swift Package Manager:
- **Snappy**: Pure Swift implementation ([snappy-swift](https://github.com/codelynx/snappy-swift))
- **GZIP**: Built-in Foundation framework

## Architecture

### Core Design Principles
1. **Layered architecture**: Mirrors Apache Arrow C++ structure
2. **Type-safe API**: Swift wrappers over Thrift metadata
3. **Instance-based readers**: ParquetFileReader manages file lifecycle
4. **Concrete type readers**: Avoid Swift generic limitations with PlainDecoder

### Directory Structure

```
Sources/Parquet/
├── Core/              # Core enums (PhysicalType, Encoding, Compression, LogicalType)
├── Thrift/            # Thrift Compact Binary Protocol parser
├── Schema/            # Schema representation (tree structure)
├── Metadata/          # FileMetadata wrapper (Swift API over Thrift)
├── IO/                # I/O layer (RandomAccessFile, BufferedReader, ParquetFileReader)
├── Encoding/          # Encoders/Decoders (PLAIN only in Phase 1)
├── Compression/       # Compression codecs (UNCOMPRESSED, GZIP, Snappy)
└── Reader/            # Column readers (PageReader, RowGroupReader, type-specific readers)
```

### Key Components

#### Thrift Layer
- `ThriftReader`: Compact Binary Protocol deserializer
- Parses Parquet file footer metadata
- **Known Issue**: Cannot parse PyArrow-generated files (metadata format incompatibility)
- **Works with**: parquet-mr files (Spark, Hive, parquet-mr tools)

#### I/O Layer
- `RandomAccessFile`: Abstract file interface (FileRandomAccessFile, MemoryRandomAccessFile)
- `BufferedReader`: Efficient buffered I/O with seek
- `ParquetFileReader`: Main entry point - manages file lifecycle and metadata

#### Schema Layer
- `Schema`: Tree structure representing Parquet schema
- `SchemaElement`: Individual schema nodes (groups and primitives)
- `SchemaBuilder`: Reconstructs schema tree from flat Thrift list

#### Reading API
- `ParquetFileReader`: Opens file, reads metadata, provides row group access
- `RowGroupReader`: Accesses columns within a row group
- Type-specific readers: `Int32ColumnReader`, `Int64ColumnReader`, `FloatColumnReader`, `DoubleColumnReader`, `StringColumnReader`
- **Important**: Use concrete readers, not generic `ColumnReader<T>` (Swift type system limitation)

#### Page Reading
- `PageReader`: Reads pages from column chunk, handles decompression
- `DataPage`: Represents data page (V1 only in Phase 1)
- Decompression integrated at page level

### Type System Quirks

**Column Readers**: The generic `ColumnReader<T>` exists but is NOT usable due to Swift's limitations with protocol extensions on PlainDecoder. Always use concrete type-specific readers:
- Use `Int32ColumnReader`, NOT `ColumnReader<Int32>`
- Use `StringColumnReader`, NOT `ColumnReader<String>`
- See `Sources/Parquet/Reader/ColumnReader.swift` for explanation

**Reason**: PlainDecoder uses type-specific extensions (`extension PlainDecoder where T == Int32`), which prevents generic ColumnReader from calling the correct decoder methods.

## Current Limitations (Phase 1/2)

### Supported
- ✅ parquet-mr generated files (Spark, Hive)
- ✅ PLAIN encoding only
- ✅ UNCOMPRESSED, GZIP, Snappy compression
- ✅ Required (non-nullable) primitive columns
- ✅ Types: Int32, Int64, Float, Double, String (UTF-8)

### NOT Supported (Blockers)
- ❌ PyArrow-generated files (Thrift metadata parsing fails)
- ❌ Dictionary encoding (most common for strings, low-cardinality columns)
- ❌ Nullable columns (definition levels not implemented)
- ❌ Nested types (lists, maps, structs)
- ❌ Data Page V2
- ❌ Other compression: LZ4, ZSTD, BROTLI, LZO

See `docs/limitations.md` for complete details.

## API Usage Pattern

```swift
// Instance-based API (recommended)
let reader = try ParquetFileReader(url: fileURL)
defer { try? reader.close() }

print("Rows: \(reader.metadata.numRows)")
print("Columns: \(reader.metadata.schema.columnCount)")

// Access row group
let rowGroup = try reader.rowGroup(at: 0)

// Read typed columns using concrete readers
let idColumn = try rowGroup.int32Column(at: 0)
let ids = try idColumn.readAll()  // [Int32]

let nameColumn = try rowGroup.stringColumn(at: 4)
let names = try nameColumn.readAll()  // [String]

// For large columns, batch reading
let batch = try idColumn.readBatch(count: 1000)
```

## Testing Strategy

### Test Files
- Test fixtures in `Tests/ParquetTests/Fixtures/`
- **Important**: Most Apache Parquet test files use dictionary encoding or Snappy
- Phase 1 fixtures must be PLAIN encoding + UNCOMPRESSED/GZIP
- PyArrow-generated fixtures don't work (metadata incompatibility)

### Test Organization
- `Core/`: Enum and basic type tests
- `Thrift/`: Thrift protocol parser tests
- `Schema/`: Schema building tests
- `IO/`: File I/O and buffering tests
- `Encoding/`: PLAIN decoder tests
- `Compression/`: Codec tests
- `Reader/`: Column reader integration tests
- `Integration/`: End-to-end file reading tests

## Phase 2 Roadmap

1. **M2.0**: ✅ Snappy compression (COMPLETE)
2. **M2.1**: 🚧 Dictionary encoding (PLAIN_DICTIONARY + RLE_DICTIONARY)
3. **M2.2**: 🚧 Definition levels (nullable columns)
4. **M2.3**: 🚧 Nested types (lists, maps, structs)

Still deferred:
- PyArrow compatibility (requires Thrift parser investigation)
- Delta encodings
- RLE encoding for booleans
- Repetition levels (nested structures)

## Reference Documentation

Key docs in `docs/`:
- `implementation-roadmap.md`: Development plan and timeline
- `phase-review.md`: Detailed phase breakdown
- `limitations.md`: Known limitations and workarounds
- `cpp-analysis.md`: Analysis of Arrow C++ implementation
- `swift-package-design.md`: Architecture decisions
- `api-guide.md`: User-facing API documentation

## Development Notes

### When Adding New Features
1. Follow Apache Arrow C++ implementation as reference
2. Add tests in parallel with implementation
3. Update `limitations.md` if adding/removing restrictions
4. Use concrete types over generics where Swift has limitations
5. Handle errors explicitly - Parquet files can be malformed

### When Debugging
- Check if file is PyArrow-generated (won't work currently)
- Verify encoding is PLAIN (only supported encoding)
- Verify compression is UNCOMPRESSED, GZIP, or Snappy
- Check column is required (no nulls support yet)
- Look at Thrift metadata parsing if metadata read fails

### Code Style
- Use explicit error types (not generic Error)
- Prefer `throw` over `Result` types
- Document public API with doc comments
- Keep layering strict (don't skip abstraction layers)
- File lifecycle: Use `defer { try? reader.close() }` pattern
