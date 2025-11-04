# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a native Swift implementation of the Apache Parquet columnar storage format, porting the Apache Arrow C++ implementation.

**Current Status**: W7 COMPLETE - Full Parquet Writer Implementation! 🎉

### Reader Implementation (R1-R5)
**Phase 1**: Complete (Practical Reader)
**Phase 2**: Complete (Snappy ✅, Dictionary ✅, Compression ✅)
**Phase 3**: Complete (Nulls ✅, Arrays ✅, Multi-level lists ✅)
**Phase 4**: Complete (Structs ✅, Maps ✅, LevelInfo ✅, DefRepLevelsToListInfo ✅, DefRepLevelsToBitmap ✅)
**Phase 4.5**: Complete (Structs with complex children ✅)
**Phase 5**: Complete (Lists of structs with complex children ✅, Maps with list values ✅)

### Writer Implementation (W7)
**Phase 2**: Complete (Primitive column writers ✅, Compression ✅, Statistics ✅)
**Phase 3**: Complete (Optional/required columns ✅, Definition levels ✅)
**Phase 4**: Complete (List writers ✅, Multi-level nested lists ✅, Repetition levels ✅)
**Phase 5**: Complete (Map writers ✅, Separate def levels for keys/values ✅)
**PyArrow Validation**: ✅ ALL PASS (lists, nested lists, maps, structs)
**Test Suite**: 436 tests passing, 0 failures

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

### Developer Setup (Optional)

The Apache Arrow C++ implementation is used as a reference during development but is **NOT required** for building or testing. The repository excludes it to keep the package size small.

**To optionally clone the Arrow C++ reference code:**
```bash
./scripts/setup-dev.sh
```

This clones the Apache Arrow repository (main branch) to `third_party/arrow/`. The C++ implementation at `third_party/arrow/cpp/src/parquet/` can be referenced while developing.

**To remove the reference code:**
```bash
./scripts/cleanup-dev.sh
```

**Note:** All documentation links point to the Apache Arrow GitHub repository, so the reference code is not strictly necessary.

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
├── IO/                # I/O layer (RandomAccessFile, BufferedReader, OutputSink)
├── Encoding/          # Encoders/Decoders (PLAIN, RLE_DICTIONARY)
├── Compression/       # Compression codecs (UNCOMPRESSED, GZIP, Snappy)
├── Reader/            # Column readers (PageReader, RowGroupReader, type-specific readers)
└── Writer/            # Column writers (PageWriter, RowGroupWriter, type-specific writers)
```

### Key Components

#### Thrift Layer
- `ThriftReader`: Compact Binary Protocol deserializer
- Parses Parquet file footer metadata
- `ThriftWriter`: Compact Binary Protocol serializer
- Writes file metadata to footer

#### I/O Layer
- **Reading**: `RandomAccessFile`, `BufferedReader`, `ParquetFileReader`
- **Writing**: `OutputSink`, `FileOutputSink`, `ParquetFileWriter`
- Manages file lifecycle, metadata, and row group access

#### Schema Layer
- `Schema`: Tree structure representing Parquet schema
- `SchemaElement`: Individual schema nodes (groups and primitives)
- `SchemaBuilder`: Reconstructs schema tree from flat Thrift list
- `Column`: Flattened view of schema with level information

#### Reading API
- `ParquetFileReader`: Opens file, reads metadata, provides row group access
- `RowGroupReader`: Accesses columns within a row group
- Primitive readers: `Int32ColumnReader`, `StringColumnReader`, etc.
- Nested readers: `readList()`, `readMap()`, `readStruct()`
- **Important**: Use concrete readers, not generic `ColumnReader<T>` (Swift type system limitation)

#### Writing API
- `ParquetFileWriter`: Creates file, writes schema and metadata
- `RowGroupWriter`: Sequential column writer access
- Primitive writers: `Int32ColumnWriter`, `StringColumnWriter`, etc.
- List writers: `Int32ListColumnWriter`, `StringListColumnWriter`, etc.
- Map writers: `StringInt32MapColumnWriter`, `StringInt64MapColumnWriter`, `StringStringMapColumnWriter`
- Struct writing: Manual field extraction pattern (documented in `StructWritingPatternTests`)

#### Page Layer
- **Reading**: `PageReader` (reads pages, handles decompression)
- **Writing**: `PageWriter` (writes pages, handles compression, level encoding)
- Data Page V1 support (definition/repetition levels, compression, statistics)

### Type System Quirks

**Column Readers**: The generic `ColumnReader<T>` exists but is NOT usable due to Swift's limitations with protocol extensions on PlainDecoder. Always use concrete type-specific readers:
- Use `Int32ColumnReader`, NOT `ColumnReader<Int32>`
- Use `StringColumnReader`, NOT `ColumnReader<String>`
- See `Sources/Parquet/Reader/ColumnReader.swift` for explanation

**Reason**: PlainDecoder uses type-specific extensions (`extension PlainDecoder where T == Int32`), which prevents generic ColumnReader from calling the correct decoder methods.

## Feature Support

### Reader (R1-R5) - COMPLETE
- ✅ PLAIN and RLE_DICTIONARY encoding
- ✅ UNCOMPRESSED, GZIP, Snappy compression
- ✅ Optional/required columns with definition levels
- ✅ Nested types: lists, maps, structs (multi-level nesting)
- ✅ Primitive types: Int32, Int64, Float, Double, String (UTF-8), Boolean
- ✅ Complex nesting: lists of structs, maps with list values, struct fields with arrays
- ✅ PyArrow-generated files (with RLE_DICTIONARY encoding)

### Writer (W7) - COMPLETE
- ✅ Primitive column writers: Int32, Int64, Float, Double, String, Boolean
- ✅ PLAIN encoding
- ✅ UNCOMPRESSED, GZIP, Snappy compression
- ✅ Optional/required columns with definition levels
- ✅ List writers (single and multi-level nested lists)
- ✅ Map writers (map<string, int32/int64/string>)
- ✅ Repetition levels for nested structures
- ✅ Statistics generation (min/max/null count)
- ✅ Separate definition levels for map keys/values
- ✅ **PyArrow validation: ALL PASS** (cross-implementation compatibility confirmed)

### Struct Writing Pattern
- ✅ Manual field extraction (write struct fields as independent columns)
- ✅ Round-trip compatibility with struct reader
- ✅ Documented pattern in `StructWritingPatternTests.swift`

### Known Limitations
- ❌ Dictionary encoding writer (PLAIN only)
- ❌ Data Page V2 (V1 only)
- ❌ Other compression: LZ4, ZSTD, BROTLI, LZO
- ❌ Bloom filters
- ❌ Column encryption

## API Usage Patterns

### Reading Files

```swift
// Open a Parquet file
let reader = try ParquetFileReader(url: fileURL)
defer { try? reader.close() }

print("Rows: \(reader.metadata.numRows)")
print("Columns: \(reader.metadata.schema.columnCount)")

// Access row group
let rowGroup = try reader.rowGroup(at: 0)

// Read primitive columns
let idColumn = try rowGroup.int32Column(at: 0)
let ids = try idColumn.readAll()  // [Int32]

let nameColumn = try rowGroup.stringColumn(at: 1)
let names = try nameColumn.readAll()  // [String?]

// Read nested columns (lists, maps, structs)
let tags = try rowGroup.readList(at: ["tags"])  // [Any?]
let attributes = try rowGroup.readMap(at: ["attributes"])  // [[String: Any]?]
let address = try rowGroup.readStruct(at: ["address"])  // [[String: Any]?]
```

### Writing Files

```swift
// Create schema (example: simple struct with two fields)
let schema = try SchemaBuilder.buildSimpleSchema(
    fields: [
        ("id", .int32, .required),
        ("name", .string, .optional)
    ]
)

// Create writer
let writer = try ParquetFileWriter(url: outputURL)
try writer.setSchema(schema)
writer.setProperties(.default)

// Write data
let rowGroup = try writer.createRowGroup()

// Write primitive columns
let idWriter = try rowGroup.int32ColumnWriter(at: 0)
try idWriter.writeValues([1, 2, 3])
try rowGroup.finalizeColumn(at: 0)

let nameWriter = try rowGroup.stringColumnWriter(at: 1)
try nameWriter.writeOptionalValues(["Alice", "Bob", nil])
try rowGroup.finalizeColumn(at: 1)

// Write lists
let listWriter = try rowGroup.int32ListColumnWriter(at: 2)
try listWriter.writeValues([[1, 2], nil, [3, 4, 5]])
try rowGroup.finalizeColumn(at: 2)

// Write maps
let mapWriter = try rowGroup.stringInt32MapColumnWriter(at: 3)
try mapWriter.writeMaps([["a": 1, "b": 2], nil, [:]])
try rowGroup.finalizeColumn(at: 3)

// Close writer
try writer.close()
```

## Testing Strategy

### Test Organization (436 tests total)
- `Core/`: Enum and basic type tests
- `Thrift/`: Thrift protocol parser tests
- `Schema/`: Schema building tests
- `IO/`: File I/O and buffering tests
- `Encoding/`: PLAIN and RLE_DICTIONARY decoder tests
- `Compression/`: Codec tests (UNCOMPRESSED, GZIP, Snappy)
- `Reader/`: Column reader integration tests (primitives, lists, maps, structs)
- `Writer/`: Column writer integration tests (primitives, lists, maps)
- `Integration/`: End-to-end reading and writing tests
- `PyArrowValidationTests`: Cross-implementation validation with PyArrow

### Test Files
- Test fixtures in `Tests/ParquetTests/Fixtures/`
- Includes PyArrow-generated files with RLE_DICTIONARY encoding
- Writer-generated files validated against PyArrow (lists, nested lists, maps, structs)
- Python validation script: `Tests/ParquetTests/Fixtures/validate_with_pyarrow.py`

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
- Check encoding (PLAIN and RLE_DICTIONARY supported)
- Verify compression (UNCOMPRESSED, GZIP, Snappy supported)
- For writer issues, check column sequencing (columns must be written in order)
- For map issues, remember keys and values have separate definition levels
- Use PyArrow validation script for cross-implementation compatibility checks

### Code Style
- Use explicit error types (not generic Error)
- Prefer `throw` over `Result` types
- Document public API with doc comments
- Keep layering strict (don't skip abstraction layers)
- File lifecycle: Use `defer { try? reader.close() }` pattern
