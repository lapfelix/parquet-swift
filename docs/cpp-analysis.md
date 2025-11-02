# Apache Arrow C++ Parquet Implementation Analysis

**Date:** 2025-11-02
**Purpose:** Comprehensive analysis of the C++ Parquet implementation for porting to Swift

## Executive Summary

This document analyzes the Apache Arrow C++ Parquet implementation (located in `third_party/arrow/cpp/src/parquet/`) to guide the development of a native Swift implementation. The C++ codebase is well-structured, mature, and serves as the authoritative reference for Parquet format compliance.

**Key Findings:**
- **~110 files** in the core Parquet directory
- **Clear separation** between format handling, encoding, and I/O
- **Moderate dependencies** on Arrow C++ infrastructure and Thrift
- **Well-tested** with comprehensive test coverage
- **Complexity varies** from straightforward type definitions to advanced SIMD-optimized encoding

---

## 1. Architecture Overview

### 1.1 High-Level Design

The C++ implementation follows a layered architecture:

```
┌─────────────────────────────────────────────┐
│  Public API Layer                            │
│  - ParquetFileReader / ParquetFileWriter     │
│  - StreamReader / StreamWriter               │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  Row Group Layer                             │
│  - RowGroupReader / RowGroupWriter           │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  Column Layer                                │
│  - ColumnReader / ColumnWriter               │
│  - PageReader / PageWriter                   │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  Encoding/Decoding Layer                     │
│  - Encoder / Decoder (per type)              │
│  - Dictionary encoding                       │
│  - PLAIN, RLE, DELTA, etc.                   │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  Foundation Layer                            │
│  - Types, Schema, Metadata                   │
│  - Statistics, Bloom Filters                 │
│  - Compression (via Arrow)                   │
└─────────────────────────────────────────────┘
```

### 1.2 Core Design Patterns

1. **PIMPL (Pointer to Implementation)**
   - Used extensively in public API classes
   - Example: `ParquetFileReader` has `Contents` struct
   - **Swift consideration:** Not needed; use private properties directly

2. **Template-based Type Handling**
   - `TypedEncoder<DType>` and `TypedDecoder<DType>`
   - **Swift consideration:** Use generics with protocols

3. **Factory Pattern**
   - `MakeEncoder()`, `MakeDecoder()` functions
   - **Swift consideration:** Maintain for flexibility

4. **Virtual Interfaces**
   - Abstract base classes for extensibility
   - **Swift consideration:** Use protocols

---

## 2. Core Components

### 2.1 File-Level Operations

**Key Files:**
- `file_reader.h/cc` (2,800 lines) - File reading interface
- `file_writer.h/cc` (900 lines) - File writing interface
- `stream_reader.h/cc` (600 lines) - High-level streaming read API
- `stream_writer.h/cc` (600 lines) - High-level streaming write API

**Responsibilities:**
- File format validation (magic bytes, footer parsing)
- Metadata deserialization (Thrift)
- Row group management
- Pre-buffering and caching strategies
- Encryption/decryption coordination

**Porting Complexity:** **Medium**
- Clean abstraction layers
- Depends on Arrow I/O primitives (`RandomAccessFile`)
- Async support using Arrow's Future API
  - **Note:** Async APIs (`OpenAsync`, `WhenBuffered`) are **optional helpers**
  - All core functionality has **synchronous entry points** (`Open`, `PreBuffer`)
  - **Swift port:** Can defer async adoption without breaking feature parity
  - See `file_reader.h:L114-L150` for both sync and async variants

### 2.2 Metadata and Schema

**Key Files:**
- `metadata.h/cc` (2,500 lines) - File/RowGroup/ColumnChunk metadata
- `schema.h/cc` (1,500 lines) - Schema tree representation
- `types.h/cc` (900 lines) - Type system definitions

**Key Classes:**
- `FileMetaData` - Top-level file metadata
- `RowGroupMetaData` - Per-row-group information
- `ColumnChunkMetaData` - Per-column statistics and offsets
- `SchemaDescriptor` - Flattened schema with computed levels
- `Node` (abstract) → `PrimitiveNode` / `GroupNode`

**Responsibilities:**
- Parquet schema representation (nested structures)
- Definition/repetition level calculation
- Column statistics (min/max/null count)
- Thrift serialization/deserialization

**Porting Complexity:** **Medium-High**
- Schema is conceptually complex (nested types)
- Repetition/definition levels require careful implementation
- Thrift dependency (need Swift Thrift library or manual serialization)

### 2.3 Encoding and Decoding

**Key Files:**
- `encoding.h` - Encoder/Decoder interfaces
- `encoder.cc` (2,100 lines) - Encoding implementations
- `decoder.cc` (2,800 lines) - Decoding implementations
- `level_conversion.h/cc` - Definition/repetition level handling
- `level_comparison.h/cc` - Level comparison utilities

**Encoding Types:**
- `PLAIN` - Raw values
- `PLAIN_DICTIONARY` / `RLE_DICTIONARY` - Dictionary encoding
- `RLE` - Run-length encoding (for booleans and levels)
- `DELTA_BINARY_PACKED` - Delta encoding for integers
- `DELTA_LENGTH_BYTE_ARRAY` - For variable-length data
- `BYTE_STREAM_SPLIT` - For floating point
- `BIT_PACKED` - Legacy bit packing

**Porting Complexity:** **High**
- Complex bit manipulation and packing logic
- SIMD optimizations in C++ (AVX2, BMI2)
  - `level_comparison_avx2.cc`
  - `level_conversion_bmi2.cc`
- Dictionary encoding requires hash maps and careful memory management
- Swift consideration: Start with PLAIN encoding, add others incrementally

### 2.4 Column Reading and Writing

**Key Files:**
- `column_reader.h/cc` (3,500 lines)
- `column_writer.h/cc` (3,400 lines)
- `column_scanner.h/cc` (500 lines)
- `column_page.h` - Page metadata structures

**Key Classes:**
- `ColumnReader` - Public API for reading column chunks
- `TypedColumnReader<T>` - Typed facade over internal reader
- **`parquet::internal::RecordReader`** - **Critical internal class**
  - This is where definition/repetition level decoding actually happens
  - Handles page-by-page reading and level unpacking
  - Buffers decoded values and levels
  - See `column_reader.cc:L1800+` for implementation
- `PageReader` - Reads individual data pages from disk

**Responsibilities:**
- Page-level I/O
- Buffering and batching
- **Definition/repetition level decoding** (via `RecordReader`)
- Decompression (delegated to Arrow)
- Statistics collection (on write)

**Porting Complexity:** **High**
- Handles chunked reading/writing
- Complex state management
- Null handling via definition levels
- **RecordReader is the heart of nested type support** - study carefully
- Integration with decoders and decompression

**Critical for Swift port:**
- The `RecordReader` abstraction cleanly separates level handling from value decoding
- Consider porting this architecture to Swift rather than mixing concerns

### 2.5 Additional Features

#### Bloom Filters
**Files:** `bloom_filter.h/cc` (900 lines), `bloom_filter_reader.h/cc`
- Split-block Bloom filter implementation
- Used for fast existence checks
- **Porting Complexity:** Medium (can defer to Phase 2)

#### Page Index
**Files:** `page_index.h/cc` (1,300 lines)
- Column/offset index for selective reading
- Enables predicate pushdown
- **Porting Complexity:** Medium (Phase 2+)

#### Statistics
**Files:** `statistics.h/cc` (1,600 lines), `size_statistics.h/cc`
- Min/max/null count per column chunk and page
- Encoded statistics in metadata
- **Porting Complexity:** Medium

#### Encryption
**Directory:** `encryption/` (52 files)
- AES-GCM encryption support
- Key management, KMS integration
- **Porting Complexity:** High (defer to Phase 3+)

---

## 3. Dependencies Analysis

### 3.1 Arrow C++ Dependencies

**Required Arrow Components:**
1. **`arrow::io`** - I/O abstractions
   - `RandomAccessFile` - File reading
   - `OutputStream` - File writing
   - `BufferReader`, `BufferedInputStream` - Buffering
   - **Swift port:** Implement similar I/O protocol

2. **`arrow::Buffer`** and memory management
   - `Buffer`, `ResizableBuffer`, `PoolBuffer`
   - `MemoryPool` abstraction
   - **Swift port:** Use Swift's memory management, possibly wrap `UnsafeRawBufferPointer`

3. **`arrow::util::Codec`** - Compression
   - Snappy, GZIP, LZ4, ZSTD, Brotli
   - **Important:** Codecs are accessed through **Arrow's Codec factory** (`arrow::util::Codec::Create`)
   - Parquet code doesn't directly call compression libraries; it uses Arrow's abstraction
   - See `third_party/arrow/cpp/src/arrow/util/compression.h` for the Codec interface
   - **Swift port:** Options:
     - Create thin wrappers over `libarrow` codec implementations (via C interop)
     - Implement own Swift codec protocol with bindings to compression C libraries
     - Start simple: Foundation's `Compression` for GZIP + system Snappy

4. **`arrow::Future`** - Async operations
   - Used for async file opening and pre-buffering
   - **Swift port:** Use Swift's async/await

5. **Arrow Array Builders**
   - Used in `DecodeArrow()` methods
   - **Swift port:** Can skip initially (focus on raw value decoding)

### 3.2 Third-Party Dependencies

**From CMakeLists.txt:**

1. **Apache Thrift** (`thrift::thrift`)
   - Serialization of metadata
   - Parquet format uses Thrift for file metadata
   - **Swift port:** Options:
     - Use Swift Thrift library (if available)
     - Manual implementation of required Thrift types
     - Code generation from `parquet.thrift`

2. **Boost Headers** (`Boost::headers`)
   - Used sparingly in tests
   - **Swift port:** Not needed

3. **Compression Libraries**
   - Snappy, GZIP, LZ4, ZSTD, Brotli
   - **Swift port:** Link C libraries or use Swift wrappers

4. **OpenSSL** (for encryption)
   - Only if `PARQUET_REQUIRE_ENCRYPTION`
   - **Swift port:** Defer encryption to later phase

5. **RapidJSON**
   - For JSON logical type support
   - **Swift port:** Use Swift's Foundation `JSONEncoder`/`JSONDecoder`

### 3.3 Platform-Specific Code

**Files:**
- `platform.h/cc` - Endianness, compiler attributes
- `windows_fixup.h`, `windows_compatibility.h` - Windows compatibility
- **Swift port:** Swift has built-in endianness support

---

## 4. Memory and Performance Considerations

### 4.1 Memory Management Patterns

1. **Smart Pointers**
   - `std::unique_ptr` - Exclusive ownership
   - `std::shared_ptr` - Shared ownership
   - **Swift:** Automatic reference counting (ARC)

2. **Memory Pools**
   - Arrow's `MemoryPool` for buffer allocation
   - Allows tracking and limiting memory usage
   - **Swift:** Could implement simple pool or rely on Swift's allocator

3. **Buffer Reuse**
   - Encoders/decoders reuse buffers where possible
   - **Swift:** Same pattern applicable

### 4.2 Performance Optimizations

1. **SIMD Instructions**
   - AVX2 for level comparison (`level_comparison_avx2.cc`)
   - BMI2 for level conversion (`level_conversion_bmi2.cc`)
   - **Swift:** Swift supports SIMD via `simd` module, but not as extensive as C++

2. **Buffering**
   - Configurable buffer sizes (`kDefaultBufferSize = 16KB`)
   - Pre-buffering for high-latency file systems
   - **Swift:** Same strategy

3. **Dictionary Encoding**
   - Hash maps for deduplication
   - **Swift:** Use `Dictionary` or custom hash table

---

## 5. Testing and Validation

### 5.1 Test Organization

**From CMakeLists.txt, test files:**
- `encoding_test.cc` (3,100 lines)
- `column_reader_test.cc` (2,200 lines)
- `column_writer_test.cc` (3,000 lines)
- `metadata_test.cc` (900 lines)
- `schema_test.cc` (3,100 lines)
- `statistics_test.cc` (2,200 lines)
- Many more...

**Key Test Files to Mirror in Swift:**

1. **Core Functionality Tests:**
   - `file_deserialize_test.cc` - Reading complete files, good integration test template
   - `file_serialize_test.cc` - Writing complete files end-to-end
   - `reader_test.cc` (2,500 lines) - Comprehensive reader scenarios
   - `schema_test.cc` - Schema parsing, nested types, level calculation

2. **Encoding/Decoding Tests:**
   - `encoding_test.cc` - All encoding types (PLAIN, DICT, RLE, DELTA)
   - `level_conversion_test.cc` (900 lines) - Definition/repetition level logic
   - Critical for validating nested type support

3. **Arrow Integration Tests:**
   - `arrow/arrow_reader_writer_test.cc` (3,000 lines) - Round-trip tests with Arrow
   - `arrow/arrow_statistics_test.cc` - Statistics generation and validation
   - Useful patterns even without Arrow dependency

4. **Regression Tests:**
   - Tests often embed small Parquet files as byte arrays
   - Example: `bloom_filter_test.cc` has binary test data from parquet-mr
   - **Swift port:** Can reuse these embedded test cases

### 5.2 Cross-Compatibility Tests

- `bloom_filter_test.cc` includes compatibility test with parquet-mr (Java)
- `parquet-testing` repository contains cross-language test files
- **Swift port:** Must pass the same test files to ensure compatibility

### 5.3 Test Data Resources

**parquet-testing Repository:**
- URL: https://github.com/apache/parquet-testing
- Contains `.parquet` files with various encodings, schemas, and edge cases
- Files to prioritize for Swift testing:
  - `alltypes_plain.parquet` - All physical types with PLAIN encoding
  - `nested_lists.snappy.parquet` - Nested repeated types
  - `datapage_v2.snappy.parquet` - DataPageV2 format
  - `decimal/` - Decimal type variations

**Generating Custom Test Files:**
- Use PyArrow to generate Swift test data:
  ```python
  import pyarrow as pa
  import pyarrow.parquet as pq

  table = pa.table({'col': [1, 2, 3]})
  pq.write_table(table, 'test.parquet')
  ```

---

## 6. Porting Complexity Assessment

### 6.1 Difficulty Levels by Component

| Component | Complexity | Rationale |
|-----------|------------|-----------|
| **Type System** | Low | Enums and simple structs |
| **Schema Representation** | Medium-High | Nested structures, level calculation |
| **Metadata Parsing** | Medium | Thrift dependency, well-defined format |
| **File Reader Structure** | Medium | Clear interfaces, depends on I/O layer |
| **File Writer Structure** | Medium | Similar to reader |
| **PLAIN Encoding** | Low | Straightforward binary serialization |
| **Dictionary Encoding** | Medium | Hash maps, index management |
| **RLE Encoding** | Medium-High | Bit packing, complex state machine |
| **Delta Encodings** | High | Complex algorithms, bit manipulation |
| **Level Encoding** | High | Critical for nested types, complex |
| **Column Reader** | High | State management, buffering, null handling |
| **Column Writer** | High | Batching, statistics, page management |
| **Compression** | Low | Delegate to libraries |
| **Bloom Filters** | Medium | Well-defined algorithm |
| **Statistics** | Medium | Straightforward aggregation |
| **Encryption** | High | Security-critical, defer |

### 6.2 C++ Features to Replace in Swift

| C++ Feature | Swift Equivalent |
|-------------|------------------|
| Templates | Generics with protocols |
| Virtual inheritance | Protocol inheritance |
| PIMPL | Direct private properties |
| Smart pointers | ARC |
| `std::vector` | `Array` or `ContiguousArray` |
| `std::unique_ptr` | Plain properties (move semantics) |
| `std::shared_ptr` | Class reference |
| SIMD intrinsics | `simd` module (limited) |
| `arrow::Buffer` | `Data` or `UnsafeRawBufferPointer` |

---

## 7. Porting Recommendations

### 7.1 Incremental Approach (Phases)

**Phase 1: Foundation (Minimal Reader)**
- Type system (`types.swift`)
- Schema representation (`schema.swift`)
- Thrift metadata parsing (manual or generated)
- File structure parsing (magic bytes, footer)
- PLAIN encoding only
- Required compression (SNAPPY, GZIP)
- Simple I/O layer (read-only)
- **Deliverable:** Read simple, flat Parquet files

**Phase 2: Full Reader Support**
- Dictionary encoding
- RLE encoding (for levels and booleans)
- Delta encodings
- Nested type support (definition/repetition levels)
- Column reader with batching
- Statistics reading
- **Deliverable:** Read complex Parquet files with nested types

**Phase 3: Writer Support**
- File writer structure
- Column writer
- All encodings (write path)
- Statistics generation
- **Deliverable:** Write Parquet files compatible with other implementations

**Phase 4: Advanced Features**
- Bloom filters
- Page index
- Streaming APIs
- Async I/O
- Performance optimizations (SIMD)

**Phase 5: Encryption (Optional)**
- AES-GCM encryption
- Key management

### 7.2 Dependency Strategy

1. **Thrift**
   - **Option A:** Use Swift Thrift library (if mature)
   - **Option B:** Generate Swift code from `parquet.thrift`
   - **Option C:** Manually implement required Thrift types (Parquet only uses a subset)

2. **Compression**
   - **Option A:** C interop with compression libraries (libz, libsnappy, etc.)
   - **Option B:** Swift wrappers (if available)
   - Start with Snappy and GZIP (most common)

3. **I/O Layer**
   - Build abstraction similar to `arrow::io::RandomAccessFile`
   - Start with `FileHandle` and `Data`
   - Add buffering layer

### 7.3 Testing Strategy

1. **Unit Tests**
   - Test each encoding independently
   - Test schema parsing
   - Test metadata serialization

2. **Integration Tests**
   - Use files from `apache/parquet-testing` repository
   - Validate against known good data

3. **Cross-Compatibility**
   - Generate files with Swift implementation
   - Read with PyArrow, parquet-mr, DuckDB
   - Vice versa

---

## 8. Key Files for Initial Study

When starting the port, focus on these files in this order:

1. **Type System:**
   - `types.h` - Core type definitions
   - `type_fwd.h` - Forward declarations

2. **Schema:**
   - `schema.h` - Schema tree structure

3. **Simple Encoding:**
   - `encoding.h` - Encoder/Decoder interfaces
   - `encoder.cc` - Study `PlainEncoder`
   - `decoder.cc` - Study `PlainDecoder`

4. **File Structure:**
   - `file_reader.h` - Public API
   - `metadata.h` - Metadata structures

5. **Thrift:**
   - `parquet.thrift` - Format specification
   - `thrift_internal.h` - Thrift utilities

---

## 9. Estimated Code Volume

Based on the C++ implementation:

| Component | C++ LOC (approx) | Swift LOC (est) |
|-----------|------------------|-----------------|
| Core types | 1,500 | 800 |
| Schema | 2,000 | 1,200 |
| Metadata | 3,000 | 2,000 |
| Encoders | 2,500 | 2,000 |
| Decoders | 3,000 | 2,500 |
| Column I/O | 7,000 | 5,000 |
| File I/O | 3,000 | 2,000 |
| Statistics | 1,500 | 1,000 |
| Bloom filters | 1,000 | 800 |
| Tests | 15,000 | 12,000 |
| **Total** | **~40,000** | **~30,000** |

Swift code is estimated to be ~75% of C++ LOC due to:
- Less boilerplate (no header files)
- No PIMPL pattern
- Simpler error handling
- But similar algorithmic complexity

---

## 10. Risks and Challenges

1. **Thrift Dependency**
   - **Risk:** Immature Swift Thrift library
   - **Mitigation:** Manual Thrift implementation for Parquet subset

2. **Performance**
   - **Risk:** Swift may be slower than C++ (especially without SIMD)
   - **Mitigation:** Profile and optimize hot paths, consider C interop for critical sections

3. **Compression Libraries**
   - **Risk:** Linking C libraries in Swift Package Manager can be tricky
   - **Mitigation:** Use system libraries where possible, provide clear setup instructions

4. **Complexity of Nested Types**
   - **Risk:** Definition/repetition level logic is subtle and error-prone
   - **Mitigation:** Comprehensive testing with parquet-testing files

5. **Maintenance**
   - **Risk:** Parquet format evolves (though slowly)
   - **Mitigation:** Track Apache Parquet spec changes, align with C++ implementation updates

---

## 11. Next Steps

1. **Set up Swift Package structure**
   - Define module layout
   - Set up dependencies (Thrift, compression)
   - Configure build system

2. **Create initial type definitions**
   - Port `types.h` to `Types.swift`
   - Port `schema.h` to `Schema.swift`

3. **Implement Thrift support**
   - Decide on Thrift strategy
   - Implement or integrate Thrift serialization

4. **Build minimal reader**
   - File format validation
   - Metadata parsing
   - Simple file with PLAIN encoding

5. **Write initial tests**
   - Unit tests for types and schema
   - Integration test with a simple Parquet file

---

## Appendix A: Parquet Format Spec

The Parquet format is defined in:
- Repository: `https://github.com/apache/parquet-format`
- Key file: `third_party/arrow/cpp/src/parquet/parquet.thrift` (1,250 lines)

---

## Appendix B: Useful Resources

1. **Official Documentation**
   - https://parquet.apache.org/docs/
   - https://github.com/apache/parquet-format/blob/master/README.md

2. **Arrow C++ Docs**
   - https://arrow.apache.org/docs/dev/developers/cpp/

3. **Parquet Testing Repository**
   - https://github.com/apache/parquet-testing

4. **Other Implementations (for reference)**
   - Java: `apache/parquet-mr`
   - Rust: `apache/arrow-rs/parquet`
   - Go: `apache/parquet-go`

---

## Appendix C: Implementation Notes & Lessons Learned

> **Note:** This section will be updated as the Swift implementation progresses to capture practical insights, gotchas, and deviations from the original C++ design.

### Cross-References to C++ Source

**Critical files to study closely:**

1. **RecordReader - Level Decoding** (start here for nested types):
   - `third_party/arrow/cpp/src/parquet/column_reader.cc:L1800-L2500` - RecordReader implementation
   - This is the **core of definition/repetition level handling**
   - Study the `ReadRecordData` and `ReadRecordsAndFlatten` methods
   - Separates level decoding from value decoding - excellent architecture to port

2. **Level Encoding** (most complex):
   - `third_party/arrow/cpp/src/parquet/level_conversion.cc` - Definition/repetition level packing
   - `third_party/arrow/cpp/src/parquet/level_comparison.cc` - Level comparison utilities
   - Study carefully: bit-packing logic, SIMD optimizations

3. **Dictionary Encoding**:
   - `third_party/arrow/cpp/src/parquet/encoder.cc:L800-L1100` - DictEncoder implementation
   - `third_party/arrow/cpp/src/parquet/decoder.cc:L1200-L1500` - DictDecoder implementation

4. **RLE Encoding**:
   - `third_party/arrow/cpp/src/parquet/encoder.cc:L200-L400` - RLE bit-packing hybrid
   - Used for both boolean values and definition/repetition levels

5. **Thrift Metadata**:
   - `third_party/arrow/cpp/src/parquet/thrift_internal.h` - Thrift utilities
   - `third_party/arrow/cpp/src/parquet/parquet.thrift` - Format specification

6. **Compression (via Arrow)**:
   - `third_party/arrow/cpp/src/arrow/util/compression.h` - Codec interface
   - Note: Parquet doesn't directly call compression libs, goes through Arrow's factory

### Implementation Lessons

**To be filled in as development progresses:**

#### Thrift Integration (Milestone 1.3)
- _Decision made:_ [TBD]
- _Challenges encountered:_ [TBD]
- _Workarounds:_ [TBD]

#### Encoding Implementation (Milestones 1.7, 2.1-2.3)
- _Performance considerations:_ [TBD]
- _Swift-specific optimizations:_ [TBD]
- _Differences from C++:_ [TBD]

#### Nested Types (Milestone 2.4)
- _Definition/repetition level handling:_ [TBD]
- _Edge cases:_ [TBD]

#### Performance Benchmarks
- _Reading speed vs. PyArrow:_ [TBD]
- _Writing speed vs. PyArrow:_ [TBD]
- _Memory usage:_ [TBD]

### Known Deviations from C++

_To be documented as they arise._

---

**End of Analysis Document**
