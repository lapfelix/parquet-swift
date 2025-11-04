# Changelog

All notable changes to parquet-swift will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-11-04

🎉 **First stable release!** Complete reader and writer implementation with full PyArrow cross-validation.

### Added - Writer Implementation (W7)

**Phase 2-3: Primitive Writers**
- Primitive column writers for all types: Int32, Int64, Float, Double, String, Boolean
- Optional and required column support with definition levels
- PLAIN encoding for all types
- Compression support: UNCOMPRESSED, GZIP, Snappy
- Statistics generation (min/max/null count)
- Metadata tracking and file footer writing

**Phase 4: List Writers**
- List column writers for all primitive types
- Multi-level nested list support (maxRepetitionLevel > 1)
- Repetition level encoding
- Proper handling of NULL lists, empty lists, and nested NULLs
- Tests: `ListWritingTests` (16 tests)

**Phase 5: Map Writers**
- Map column writers: `map<string, int32>`, `map<string, int64>`, `map<string, string>`
- **Critical architecture**: Separate definition levels for keys and values
  - Keys (required): maxDefinitionLevel = 2
  - Values (optional): maxDefinitionLevel = 3
  - Shared repetition levels for map boundaries
- Proper handling of NULL maps, empty maps, and NULL values
- Tests: `MapWritingTests` (4 tests)

**Struct Writing Pattern**
- Documented manual field extraction pattern
- Write struct fields as independent columns
- Round-trip compatibility with struct reader
- Tests: `StructWritingPatternTests` (3 tests)

**PyArrow Cross-Validation**
- `PyArrowValidationTests`: Generates validation files
- Python validation script: `validate_with_pyarrow.py`
- **Results: ALL PASS** (4/4 files validated)
  - ✅ lists.parquet
  - ✅ nested_lists.parquet
  - ✅ maps.parquet
  - ✅ structs.parquet

### Added - Reader Features

**Phase 5: Complex Nesting**
- Lists of structs with complex children
- Maps with list values
- Struct fields containing arrays and maps
- Full nested type parity with Apache Arrow C++

### Changed

- Updated README.md to reflect v1.0 production-ready status
- Updated CLAUDE.md with complete W7 documentation
- Improved error messages for column writer validation

### Fixed

- Map writer definition levels (critical bug)
  - Previously: Shared single definition level stream for keys and values
  - Now: Separate definition level streams per column
  - Fixed PyArrow compatibility: "Malformed levels" error resolved

### Test Suite

- **Total Tests:** 436 passing, 0 failures
- **New Tests:** 23 writer tests added
- **Coverage:** All reader and writer paths validated

## [0.9.0] - 2024-11-02

### Added - Reader Phase 5

- Lists of structs with complex children (maps, lists)
- Full support for deeply nested structures
- `readRepeatedStruct()` API for list<struct> patterns

### Changed

- Enhanced `LevelInfo` to handle complex nesting patterns
- Improved struct validity computation for repeated children

## [0.8.0] - 2024-10-30

### Added - Reader Phase 4.5

- Structs with complex children (maps and lists as fields)
- `DefRepLevelsToBitmap` for struct validity
- Full `LevelInfo` infrastructure for nested types

## [0.7.0] - 2024-10-28

### Added - Reader Phase 4

- Struct column reading with `readStruct()` API
- Map column reading with `readMap()` API
- `LevelInfo` support for complex nested types
- `DefRepLevelsToListInfo` algorithm for array reconstruction

## [0.6.0] - 2024-10-25

### Added - Reader Phase 3

- Multi-level nested list support (maxRepetitionLevel > 1)
- Array reconstruction from flat value sequences
- `readAllRepeated()` API for [[T?]] return type

## [0.5.0] - 2024-10-20

### Added - Reader Phase 3 (Early)

- Definition levels for nullable columns
- Optional column support for all primitive types
- PyArrow compatibility fixes (critical Thrift parsing bugs)
- Single-level repeated fields

## [0.4.0] - 2024-10-15

### Added - Reader Phase 2

- Dictionary encoding (RLE_DICTIONARY, PLAIN_DICTIONARY)
- Support for all primitive types: Int32, Int64, Float, Double, String
- Snappy compression codec

## [0.3.0] - 2024-10-10

### Added - Reader Phase 2 (Early)

- GZIP compression codec
- Enhanced buffered I/O
- Metadata wrapper API (idiomatic Swift types)

## [0.2.0] - 2024-10-05

### Added - Reader Phase 1

- Thrift Compact Binary Protocol parser
- Schema representation and tree building
- PLAIN encoding for primitive types
- Basic column readers: Int32, Int64, Float, Double, String
- File reader API with instance-based lifecycle
- UNCOMPRESSED codec

## [0.1.0] - 2024-10-01

### Added

- Initial project setup
- Swift Package Manager configuration
- Core type system (PhysicalType, LogicalType, Encoding, Compression)
- Basic I/O abstractions
- Apache License 2.0

---

## Release Notes

### v1.0.0 Highlights

This release marks the completion of both the **reader** and **writer** implementations, making parquet-swift a production-ready library for working with Parquet files in Swift.

**Key Achievements:**
- ✅ Full reader implementation (phases R1-R5)
- ✅ Full writer implementation (phases W2-W7)
- ✅ PyArrow cross-validation (ALL PASS)
- ✅ 436 tests passing, 0 failures
- ✅ Pure Swift implementation with no system dependencies
- ✅ Support for all common Parquet patterns

**Architecture Highlights:**
- Mirrors Apache Arrow C++ implementation
- Type-safe API with explicit error handling
- Instance-based lifecycle management
- Validated against PyArrow for cross-implementation compatibility

**Known Limitations:**
- Dictionary encoding writer not yet implemented (PLAIN only)
- Data Page V2 not supported (V1 only)
- Additional compression codecs (LZ4, ZSTD) not implemented

These limitations do not affect most common use cases and can be added in future versions.

### Migration from 0.9.x to 1.0.0

No breaking API changes. New writer functionality is purely additive.

**New APIs:**
- `ParquetFileWriter` for creating files
- `RowGroupWriter` for sequential column writing
- Primitive column writers: `int32ColumnWriter()`, `stringColumnWriter()`, etc.
- List column writers: `int32ListColumnWriter()`, etc.
- Map column writers: `stringInt32MapColumnWriter()`, etc.

**Struct Writing:**
Continue using the manual field extraction pattern (write each field as an independent column). No dedicated struct writer is provided.

### Future Roadmap

**v1.1 (Next Minor Release):**
- Dictionary encoding writer
- Data Page V2 support
- Performance optimizations (SIMD, vectorization)

**v1.2+ (Future):**
- Additional compression codecs (LZ4, ZSTD)
- Bloom filters
- Page index
- Swift Concurrency (async/await)

[1.0.0]: https://github.com/yourusername/parquet-swift/releases/tag/v1.0.0
[0.9.0]: https://github.com/yourusername/parquet-swift/releases/tag/v0.9.0
[0.8.0]: https://github.com/yourusername/parquet-swift/releases/tag/v0.8.0
[0.7.0]: https://github.com/yourusername/parquet-swift/releases/tag/v0.7.0
[0.6.0]: https://github.com/yourusername/parquet-swift/releases/tag/v0.6.0
[0.5.0]: https://github.com/yourusername/parquet-swift/releases/tag/v0.5.0
[0.4.0]: https://github.com/yourusername/parquet-swift/releases/tag/v0.4.0
[0.3.0]: https://github.com/yourusername/parquet-swift/releases/tag/v0.3.0
[0.2.0]: https://github.com/yourusername/parquet-swift/releases/tag/v0.2.0
[0.1.0]: https://github.com/yourusername/parquet-swift/releases/tag/v0.1.0
