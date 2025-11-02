# Parquet-Swift Implementation Roadmap

**Project:** Native Swift implementation of Apache Parquet format
**Date:** 2025-11-02
**Status:** Planning Phase

---

## Project Goals

1. **Primary Goal:** Create a native Swift library for reading and writing Apache Parquet files
2. **Compatibility:** 100% compatible with Apache Parquet specification
3. **Performance:** Acceptable performance for typical use cases
4. **Platform Support:** macOS, iOS, Linux
5. **API Design:** Idiomatic Swift, type-safe, easy to use

---

## Dependencies & Requirements

### Build Requirements
| Requirement | Version | Notes |
|------------|---------|-------|
| **Swift** | 5.9+ | For C++ interop capability (optional) |
| **Platform** | macOS 10.15+, iOS 13+, Linux | Via Swift Package Manager |
| **Xcode** | 14.0+ | For macOS/iOS development |

### Runtime Dependencies
| Dependency | Status | Integration Method |
|-----------|--------|-------------------|
| **Thrift** | TBD | Options: Swift library, manual impl, or codegen |
| **Compression - GZIP** | Required | Foundation `Compression` framework (built-in) |
| **Compression - Snappy** | Required | C library via SwiftPM system target |
| **Compression - LZ4** | Optional | C library (Phase 2+) |
| **Compression - ZSTD** | Optional | C library (Phase 2+) |
| **Compression - Brotli** | Optional | C library (Phase 3+) |

### Development Dependencies
- XCTest (unit testing, built-in)
- swift-format (code formatting, optional)
- swift-docc (documentation, optional)

**Decision Points:**
- ⏳ **Thrift strategy**: Evaluate week 2-3 of Phase 1
- ⏳ **Compression libraries**: Start with GZIP (built-in) + Snappy
- ⏳ **SIMD optimizations**: Defer to Phase 4

---

## Development Phases

### Phase 1: Foundation (Minimal Reader) ✓ Target: 4-6 weeks

**Goal:** Read simple, flat Parquet files with basic encodings

#### Milestone 1.0: Planning & Analysis ✅ (Completed 2025-11-02)
- [x] Analyze C++ Arrow/Parquet implementation
- [x] Document architecture and dependencies
- [x] Create implementation roadmap
- [x] Design Swift package structure
- [x] Identify porting complexity by component

**Deliverables:**
- ✅ `docs/cpp-analysis.md` - Comprehensive C++ analysis
- ✅ `docs/implementation-roadmap.md` - Phased development plan
- ✅ `docs/swift-package-design.md` - Package architecture
- ✅ `docs/README.md` - Documentation index

#### Milestone 1.1: Project Setup (Week 1)
- [x] Initialize git repository
- [x] Add Apache Arrow as submodule (for C++ reference)
- [x] Create planning documentation (docs/)
- [ ] Create Swift Package structure (`Package.swift`)
- [ ] Set up directory layout (`Sources/`, `Tests/`)
- [ ] Add LICENSE file (Apache 2.0)
- [ ] Create project README.md

**Deliverables:**
- `Package.swift` with basic structure
- `README.md` with project overview and usage
- `LICENSE` file (Apache 2.0)
- Directory structure created

#### Milestone 1.2: Core Type System (Week 1-2)
- [ ] Define physical types (`Types.swift`)
  - `Boolean`, `Int32`, `Int64`, `Int96`, `Float`, `Double`
  - `ByteArray`, `FixedLenByteArray`
- [ ] Define logical types
  - `String`, `Date`, `Timestamp`, `Decimal`, etc.
- [ ] Encoding types enumeration
- [ ] Compression types enumeration
- [ ] Repetition types (required/optional/repeated)

**Deliverables:**
- `Sources/Parquet/Types.swift`
- `Sources/Parquet/LogicalTypes.swift`
- Unit tests for type definitions

#### Milestone 1.3: Thrift Integration (Week 2-3)
- [ ] Evaluate Swift Thrift libraries
- [ ] Decision: use library vs. manual implementation
- [ ] Implement or integrate Thrift serialization for:
  - `FileMetaData`
  - `RowGroup`
  - `ColumnChunk`
  - `ColumnMetaData`
  - Schema elements
- [ ] Test Thrift roundtrip

**Deliverables:**
- `Sources/Parquet/Thrift/` directory with Thrift support
- Integration tests

#### Milestone 1.4: Schema Representation (Week 3)
- [ ] Implement `Node` protocol
- [ ] Implement `PrimitiveNode`
- [ ] Implement `GroupNode`
- [ ] Implement `SchemaDescriptor`
- [ ] Implement `ColumnDescriptor`
- [ ] Definition/repetition level calculation (simple cases)

**Deliverables:**
- `Sources/Parquet/Schema.swift`
- Schema parsing tests
- Level calculation tests

#### Milestone 1.5: Basic I/O Layer (Week 3-4)
- [ ] Define `RandomAccessFile` protocol
- [ ] Implement `FileRandomAccessFile` using `FileHandle`
- [ ] Implement `BufferedReader`
- [ ] Implement `BufferedWriter` (for future)

**Deliverables:**
- `Sources/Parquet/IO/RandomAccessFile.swift`
- `Sources/Parquet/IO/BufferedIO.swift`
- I/O tests

#### Milestone 1.6: Metadata Parsing (Week 4)
- [ ] Implement `FileMetaData` wrapper
- [ ] Implement `RowGroupMetaData` wrapper
- [ ] Implement `ColumnChunkMetaData` wrapper
- [ ] Parse Parquet file footer
- [ ] Validate magic bytes
- [ ] Read footer metadata

**Deliverables:**
- `Sources/Parquet/Metadata.swift`
- Metadata parsing tests with real files

#### Milestone 1.7: PLAIN Encoding (Week 4-5)
- [ ] Implement `Decoder` protocol
- [ ] Implement `PlainDecoder` for each physical type
- [ ] Handle endianness correctly
- [ ] Test with various data types

**Deliverables:**
- `Sources/Parquet/Encoding/Decoder.swift`
- `Sources/Parquet/Encoding/PlainDecoder.swift`
- Encoding tests

#### Milestone 1.8: Basic Compression (Week 5)
- [ ] Implement `Codec` protocol
- [ ] Integrate GZIP (via `Compression` framework)
- [ ] Integrate Snappy (C library or Swift wrapper)
- [ ] Test compression roundtrip

**Deliverables:**
- `Sources/Parquet/Compression/Codec.swift`
- Compression tests

#### Milestone 1.9: Column Reader (Week 5-6)
- [ ] Implement `PageReader`
- [ ] Implement `ColumnReader` (flat columns only)
- [ ] Handle data pages
- [ ] Handle dictionary pages (read dictionary)
- [ ] Decompress pages
- [ ] Decode values

**Deliverables:**
- `Sources/Parquet/Reader/ColumnReader.swift`
- Column reading tests

#### Milestone 1.10: File Reader API (Week 6)
- [ ] Implement `ParquetFileReader`
- [ ] Implement `RowGroupReader`
- [ ] Public API for reading rows/columns
- [ ] Error handling
- [ ] Documentation

**Deliverables:**
- `Sources/Parquet/Reader/ParquetFileReader.swift`
- Complete end-to-end reading test
- API documentation

#### Phase 1 Exit Criteria:
- ✅ Can read simple Parquet files (flat schema, PLAIN encoding)
- ✅ Passes basic integration tests with parquet-testing files
- ✅ Clean public API
- ✅ Documented code

---

### Phase 2: Full Reader Support ✓ Target: 6-8 weeks

**Goal:** Read complex Parquet files with all encodings and nested types

#### Milestone 2.1: Dictionary Encoding (Week 7-8)
- [ ] Implement `DictionaryDecoder`
- [ ] Handle dictionary pages
- [ ] Dictionary index decoding
- [ ] Support for all dictionary-encoded types

**Deliverables:**
- `Sources/Parquet/Encoding/DictionaryDecoder.swift`
- Dictionary encoding tests

#### Milestone 2.2: RLE Encoding (Week 8-9)
- [ ] Implement RLE/Bit-packing hybrid decoder
- [ ] Use for boolean values
- [ ] Use for definition/repetition levels
- [ ] Optimize bit manipulation

**Deliverables:**
- `Sources/Parquet/Encoding/RLEDecoder.swift`
- RLE decoding tests

#### Milestone 2.3: Delta Encodings (Week 9-10)
- [ ] Implement `DELTA_BINARY_PACKED` decoder
- [ ] Implement `DELTA_LENGTH_BYTE_ARRAY` decoder
- [ ] Implement `DELTA_BYTE_ARRAY` decoder
- [ ] Test with generated data

**Deliverables:**
- `Sources/Parquet/Encoding/DeltaDecoders.swift`
- Delta encoding tests

#### Milestone 2.4: Nested Type Support (Week 10-12)
- [ ] Full definition level handling
- [ ] Full repetition level handling
- [ ] Support for nested structs (GroupNode)
- [ ] Support for arrays (repeated fields)
- [ ] Support for maps
- [ ] Complex column reading

**Deliverables:**
- Enhanced `ColumnReader` for nested types
- Nested type tests with parquet-testing files

#### Milestone 2.5: Statistics (Week 12-13)
- [ ] Parse column statistics (min/max/null count)
- [ ] Parse page-level statistics
- [ ] Expose statistics via API

**Deliverables:**
- `Sources/Parquet/Statistics.swift`
- Statistics tests

#### Milestone 2.6: Comprehensive Testing (Week 13-14)
- [ ] Integration tests with all parquet-testing files
- [ ] Cross-compatibility tests (generate files with other tools, read with Swift)
- [ ] Performance benchmarks
- [ ] Memory usage profiling

**Deliverables:**
- Full test suite
- Benchmark results
- Bug fixes

#### Phase 2 Exit Criteria:
- ✅ Can read all valid Parquet files (all encodings, nested types)
- ✅ Passes all relevant parquet-testing files
- ✅ Statistics are correctly parsed
- ✅ Performance is acceptable (within 2x of PyArrow for reading)

---

### Phase 3: Writer Support ✓ Target: 6-8 weeks

**Goal:** Write Parquet files compatible with all readers

#### Milestone 3.1: Writer Foundation (Week 15-16)
- [ ] Implement `Encoder` protocol
- [ ] Implement `PlainEncoder` for all types
- [ ] Implement page writer
- [ ] Implement column chunk writer

**Deliverables:**
- `Sources/Parquet/Encoding/Encoder.swift`
- `Sources/Parquet/Writer/PageWriter.swift`

#### Milestone 3.2: All Encodings (Week 16-18)
- [ ] Implement `DictionaryEncoder`
- [ ] Implement RLE encoder
- [ ] Implement Delta encoders (optional, can use PLAIN)
- [ ] Encoding selection logic

**Deliverables:**
- Complete encoder implementations
- Encoder tests

#### Milestone 3.3: Column Writer (Week 18-19)
- [ ] Implement `ColumnWriter`
- [ ] Handle batching
- [ ] Statistics generation
- [ ] Compression
- [ ] Dictionary page writing

**Deliverables:**
- `Sources/Parquet/Writer/ColumnWriter.swift`
- Column writing tests

#### Milestone 3.4: File Writer API (Week 19-20)
- [ ] Implement `ParquetFileWriter`
- [ ] Implement `RowGroupWriter`
- [ ] Metadata serialization (Thrift)
- [ ] Footer writing
- [ ] Properties and configuration

**Deliverables:**
- `Sources/Parquet/Writer/ParquetFileWriter.swift`
- File writing tests

#### Milestone 3.5: Nested Type Writing (Week 20-21)
- [ ] Definition/repetition level encoding
- [ ] Write nested structs
- [ ] Write arrays
- [ ] Write maps

**Deliverables:**
- Nested type writing support
- Complex schema tests

#### Milestone 3.6: Cross-Compatibility Testing (Week 21-22)
- [ ] Write files, read with PyArrow
- [ ] Write files, read with DuckDB
- [ ] Write files, read with Spark
- [ ] Verify statistics
- [ ] Fix incompatibilities

**Deliverables:**
- Compatibility test suite
- Bug fixes

#### Phase 3 Exit Criteria:
- ✅ Can write Parquet files with all encodings
- ✅ Written files are readable by PyArrow, DuckDB, Spark
- ✅ Statistics are correctly generated
- ✅ Compression works correctly

---

### Phase 4: Advanced Features ✓ Target: 4-6 weeks

**Goal:** Add production-ready features

#### Milestone 4.1: Bloom Filters (Week 23-24)
- [ ] Implement Bloom filter reading
- [ ] Implement Bloom filter writing
- [ ] Split-block Bloom filter algorithm
- [ ] API for bloom filter queries

**Deliverables:**
- `Sources/Parquet/BloomFilter.swift`
- Bloom filter tests

#### Milestone 4.2: Page Index (Week 24-25)
- [ ] Implement column index reading
- [ ] Implement offset index reading
- [ ] Implement page index writing
- [ ] Enable predicate pushdown

**Deliverables:**
- `Sources/Parquet/PageIndex.swift`
- Page index tests

#### Milestone 4.3: Streaming APIs (Week 25-26)
- [ ] Implement `StreamReader` API
- [ ] Implement `StreamWriter` API
- [ ] Simplified row-oriented API
- [ ] Examples and documentation

**Deliverables:**
- `Sources/Parquet/StreamReader.swift`
- `Sources/Parquet/StreamWriter.swift`
- Examples

#### Milestone 4.4: Async I/O (Week 26-27)
- [ ] Async file reading
- [ ] Async file writing
- [ ] Concurrent row group reading
- [ ] Pre-buffering

**Deliverables:**
- Async API additions
- Async tests

#### Milestone 4.5: Performance Optimization (Week 27-28)
- [ ] Profile hot paths
- [ ] Optimize encoders/decoders
- [ ] Consider SIMD for critical paths
- [ ] Reduce allocations
- [ ] Benchmark improvements

**Deliverables:**
- Performance improvements (target: within 50% of PyArrow)
- Benchmark results

#### Phase 4 Exit Criteria:
- ✅ Bloom filters and page index working
- ✅ Streaming APIs available
- ✅ Async support
- ✅ Performance competitive with other implementations

---

### Phase 5: Encryption (Optional) ✓ Target: 4-6 weeks

**Goal:** Support Parquet encryption (if needed)

#### Milestones:
- AES-GCM encryption/decryption
- Key management
- KMS integration
- Encrypted metadata handling

**Note:** This phase can be skipped initially and added later based on user demand.

---

## Testing Strategy

### Unit Tests
- Test each component in isolation
- Use XCTest framework
- Aim for >80% code coverage

### Integration Tests
- Use files from `apache/parquet-testing`
- Generate test files with PyArrow
- Test round-trip (write and read back)

### Performance Tests
- Benchmark reading large files
- Benchmark writing large files
- Compare with PyArrow baseline
- Track performance over time

### Compatibility Tests
- Generate files with parquet-swift, read with:
  - PyArrow
  - DuckDB
  - Apache Spark
- Read files generated by those tools with parquet-swift

---

## Project Structure

```
parquet-swift/
├── Package.swift
├── README.md
├── LICENSE
├── docs/
│   ├── cpp-analysis.md
│   ├── implementation-roadmap.md (this file)
│   └── api-design.md (to be created)
├── Sources/
│   └── Parquet/
│       ├── Types.swift
│       ├── LogicalTypes.swift
│       ├── Schema.swift
│       ├── Metadata.swift
│       ├── Thrift/
│       │   └── (Thrift integration)
│       ├── IO/
│       │   ├── RandomAccessFile.swift
│       │   └── BufferedIO.swift
│       ├── Compression/
│       │   └── Codec.swift
│       ├── Encoding/
│       │   ├── Decoder.swift
│       │   ├── Encoder.swift
│       │   ├── PlainCoding.swift
│       │   ├── DictionaryCoding.swift
│       │   ├── RLECoding.swift
│       │   └── DeltaCoding.swift
│       ├── Reader/
│       │   ├── ParquetFileReader.swift
│       │   ├── RowGroupReader.swift
│       │   ├── ColumnReader.swift
│       │   ├── PageReader.swift
│       │   └── StreamReader.swift
│       ├── Writer/
│       │   ├── ParquetFileWriter.swift
│       │   ├── RowGroupWriter.swift
│       │   ├── ColumnWriter.swift
│       │   ├── PageWriter.swift
│       │   └── StreamWriter.swift
│       ├── Statistics.swift
│       ├── BloomFilter.swift
│       └── PageIndex.swift
├── Tests/
│   └── ParquetTests/
│       ├── TypesTests.swift
│       ├── SchemaTests.swift
│       ├── EncodingTests.swift
│       ├── ReaderTests.swift
│       ├── WriterTests.swift
│       ├── IntegrationTests.swift
│       └── Resources/
│           └── (test .parquet files)
├── Examples/
│   ├── ReadExample/
│   └── WriteExample/
└── third_party/
    └── arrow/ (submodule - for reference)
```

---

## Dependencies

### Build-Time
- Swift 5.9+ (for C++ interop if needed)
- Swift Package Manager

### Runtime
- **Compression:**
  - zlib (GZIP) - system library
  - Snappy - needs integration
  - LZ4, ZSTD (optional)
- **Thrift:**
  - Swift Thrift library OR manual implementation

### Development
- XCTest (unit tests)
- swift-format (code formatting)
- swift-docc (documentation)

---

## Milestones Summary

| Phase | Duration | Cumulative | Key Deliverable |
|-------|----------|------------|-----------------|
| Phase 1 | 6 weeks | 6 weeks | Read simple Parquet files |
| Phase 2 | 8 weeks | 14 weeks | Read complex Parquet files |
| Phase 3 | 8 weeks | 22 weeks | Write Parquet files |
| Phase 4 | 6 weeks | 28 weeks | Production features |
| Phase 5 | 6 weeks | 34 weeks | Encryption (optional) |

**Total estimated time for Phases 1-4: ~7 months**

---

## Success Criteria

### Phase 1 Success
- [ ] Can parse Parquet file metadata
- [ ] Can read a simple flat table (e.g., integers, strings)
- [ ] Basic error handling in place
- [ ] At least 10 unit tests passing

### Phase 2 Success
- [ ] Can read files with all standard encodings
- [ ] Can read files with nested schemas
- [ ] Passes 80%+ of parquet-testing files
- [ ] Comprehensive test coverage

### Phase 3 Success
- [ ] Can write files readable by PyArrow and DuckDB
- [ ] Can write files with nested schemas
- [ ] Statistics are correctly generated
- [ ] Round-trip tests pass (write and read back)

### Phase 4 Success
- [ ] Bloom filters work correctly
- [ ] Page index enables selective reading
- [ ] Performance within 2x of PyArrow
- [ ] Clean, documented API

---

## Risk Mitigation

### Risk: Thrift dependency is problematic
**Mitigation:** Manually implement required Thrift types (limited subset)

### Risk: Performance is too slow
**Mitigation:** Profile early, optimize hot paths, consider C interop for critical sections

### Risk: Compression library integration is difficult
**Mitigation:** Start with zlib (system library), defer exotic codecs

### Risk: Scope creep
**Mitigation:** Stick to the roadmap, defer features to later phases

### Risk: Lack of Swift Parquet expertise
**Mitigation:** Study C++ implementation closely, reference Rust implementation, ask for community feedback

---

## Community and Release Plan

### Alpha Release (End of Phase 1)
- Basic reading functionality
- Solicit early feedback
- GitHub issues enabled

### Beta Release (End of Phase 2)
- Full reading support
- Public API review
- First external users

### 1.0 Release (End of Phase 3)
- Reading and writing support
- Production-ready
- Full documentation
- Performance benchmarks published

### 1.1+ Releases (Phase 4+)
- Advanced features
- Performance improvements
- Community-requested features

---

## Contributing Guidelines

(To be created in separate document)

- Code style (swift-format)
- Testing requirements (all PRs must have tests)
- Documentation requirements
- Issue templates
- PR templates

---

**Next Immediate Steps:**

1. ✅ Complete project analysis (this document)
2. Create Swift Package structure (`Package.swift`)
3. Set up directory layout
4. Implement core types (`Types.swift`)
5. Write first unit tests

---

**End of Roadmap**
