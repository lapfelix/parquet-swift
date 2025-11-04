# Phase 10 Design: Core Writer Implementation

**Date**: 2025-11-04
**Status**: Design Phase
**Goal**: Write Parquet files with primitive types, nullable columns, and nested structures
**Target**: Version 1.0 Release

---

## Executive Summary

Phase 10 implements the **Core Writer** - the ability to create Parquet files from Swift data. This is the final major component needed for the 1.0 release.

**Scope**:
- Write flat and nested schemas
- PLAIN encoding for all primitive types
- Dictionary encoding for strings/low-cardinality columns
- Snappy and GZIP compression
- Definition and repetition level encoding
- Statistics generation (min/max, null counts)

**Timeline**: 8-10 weeks
**Complexity**: High (writing is inherently more complex than reading)

---

## Problem Statement

### Current State
Parquet-Swift has a **production-ready reader** that can handle virtually all real-world Parquet files. However, users cannot:
- Generate Parquet files from Swift applications
- Export data from iOS/macOS apps in Parquet format
- Create data pipelines that produce Parquet output
- Use Parquet as a storage format for Swift-based analytics

### Goals for Phase 10

**Primary Goals**:
1. Write files readable by Apache Arrow C++, PyArrow, and Spark
2. Support all primitive types (Int32, Int64, Float, Double, String, Boolean)
3. Support nullable and required columns
4. Support nested types (lists, maps, structs)
5. Generate correct metadata and statistics
6. Compression support (UNCOMPRESSED, GZIP, Snappy)

**Non-Goals** (defer to post-1.0):
- Bloom filter generation
- Page index generation
- Data Page V2 format
- Delta encodings
- Column encryption

---

## Arrow C++ Writer Architecture Analysis

### File Writing Flow

From `cpp/src/parquet/arrow/writer.cc`:

```cpp
// 1. Create file writer with schema
auto writer = ParquetFileWriter::Open(sink, schema, properties);

// 2. Create row group writer
auto rg_writer = writer->AppendRowGroup();

// 3. For each column:
auto col_writer = rg_writer->NextColumn();

// 4. Write values with levels
col_writer->WriteBatch(num_values, def_levels, rep_levels, values);

// 5. Close row group
rg_writer->Close();

// 6. Close file (writes footer)
writer->Close();
```

### Key Components

**ParquetFileWriter**:
- Manages file output sink
- Serializes schema to Thrift
- Writes magic number and footer
- Tracks row group metadata

**RowGroupWriter**:
- Manages column writers for a row group
- Tracks row group statistics
- Finalizes row group metadata

**ColumnWriter** (typed by physical type):
- Encodes values (PLAIN, Dictionary, etc.)
- Encodes definition/repetition levels
- Compresses pages
- Writes pages to output
- Generates statistics

**PageWriter**:
- Writes data pages to output stream
- Writes dictionary pages
- Tracks page offsets and sizes

### Level Encoding

**Definition Levels** (for nullable/optional fields):
- Encoded using RLE/bit-packed hybrid encoding
- Written before values in each page
- Indicates which values are NULL

**Repetition Levels** (for repeated/nested fields):
- Encoded using RLE/bit-packed hybrid encoding
- Written before definition levels in each page
- Indicates list boundaries

### Statistics Generation

Arrow C++ computes per-page and per-column-chunk statistics:
- **min/max**: For all comparable types
- **null_count**: Count of NULL values
- **distinct_count**: Cardinality (optional, expensive)

These are written to ColumnMetaData in the footer.

---

## Proposed API Design

### File Writer API

```swift
import Parquet

// 1. Create writer with output URL
let writer = try ParquetFileWriter(url: outputURL)
defer { try? writer.close() }

// 2. Define schema
let schema = try SchemaBuilder()
    .addInt32Column("id", required: true)
    .addStringColumn("name", required: false)
    .addInt64Column("timestamp", required: true)
    .build()

writer.setSchema(schema)

// 3. Optional: Configure writer properties
var properties = WriterProperties()
properties.compression = .snappy
properties.enableDictionary = true
properties.dictionaryPageSizeLimit = 1024 * 1024  // 1MB
writer.setProperties(properties)

// 4. Create row group
let rowGroupWriter = try writer.createRowGroup()

// 5. Write columns
let idWriter = try rowGroupWriter.int32ColumnWriter(at: 0)
try idWriter.writeValues([1, 2, 3, 4, 5])

let nameWriter = try rowGroupWriter.stringColumnWriter(at: 1)
try nameWriter.writeValues(["Alice", nil, "Charlie", "David", nil])

let timestampWriter = try rowGroupWriter.int64ColumnWriter(at: 2)
try timestampWriter.writeValues([1000, 2000, 3000, 4000, 5000])

// 6. Close row group (finalizes statistics, writes metadata)
try rowGroupWriter.close()

// 7. Close file (writes footer)
try writer.close()
```

### Advanced Usage - Nested Types

```swift
// Schema: list<struct { name: string, scores: map<string, int64> }>
let schema = try SchemaBuilder()
    .beginList("students")
        .beginStruct("element")
            .addStringColumn("name", required: true)
            .beginMap("scores")
                .addStringColumn("key", required: true)
                .addInt64Column("value", required: true)
            .endMap()
        .endStruct()
    .endList()
    .build()

writer.setSchema(schema)

let rowGroupWriter = try writer.createRowGroup()

// Write nested data
let studentsWriter = try rowGroupWriter.listWriter(at: [["students"]])

// Row 0: List with 1 student
try studentsWriter.beginList()
try studentsWriter.beginStruct()
try studentsWriter.writeField("name", value: "Alice")
try studentsWriter.writeMapField("scores", value: ["math": 90, "english": 85])
try studentsWriter.endStruct()
try studentsWriter.endList()

// Row 1: Empty list
try studentsWriter.beginList()
try studentsWriter.endList()

// Row 2: NULL list
try studentsWriter.writeNullList()

try rowGroupWriter.close()
try writer.close()
```

**Note**: Nested writer API may need refinement during implementation. Consider columnar approach vs row-oriented approach.

---

## Architecture Design

### Component Structure

```
Sources/Parquet/Writer/
├── ParquetFileWriter.swift       # Main file writer
├── RowGroupWriter.swift          # Row group management
├── ColumnWriter.swift            # Base column writer protocol
├── TypedColumnWriters.swift      # Int32Writer, StringWriter, etc.
├── PageWriter.swift              # Page-level writing
├── LevelEncoder.swift            # Definition/repetition level encoding
├── Statistics.swift              # Statistics computation
├── WriterProperties.swift        # Configuration
└── SchemaBuilder.swift           # Fluent schema building API
```

### File Writing Lifecycle

```
ParquetFileWriter.init(url:)
    ↓
setSchema(_:)  // Must be called before creating row groups
    ↓
createRowGroup() → RowGroupWriter
    ↓
RowGroupWriter.columnWriter(at:) → TypedColumnWriter
    ↓
TypedColumnWriter.writeValues(_:)
    → Encode values (PLAIN or Dictionary)
    → Encode definition levels (if nullable)
    → Compress page
    → Write page via PageWriter
    → Update statistics
    ↓
RowGroupWriter.close()
    → Finalize all column writers
    → Write ColumnMetaData for each column
    → Write RowGroup metadata
    ↓
ParquetFileWriter.close()
    → Write FileMetaData (footer)
    → Write footer offset (4 bytes)
    → Write magic number "PAR1"
```

### Memory Management

**Buffering Strategy**:
- Buffer values in memory until page size limit reached
- Default page size: 1MB (configurable)
- Flush page when:
  - Page size limit reached
  - Row group closing
  - Explicit flush requested

**Dictionary Encoding**:
- Accumulate unique values in dictionary
- Track dictionary size
- Fall back to PLAIN if dictionary exceeds size limit
- Write dictionary page before first data page

---

## Implementation Plan

### Milestone W1: Basic File Writer (3 weeks)

**Week 1: File Structure & Metadata**
- [ ] ParquetFileWriter with file output sink
- [ ] Write magic number "PAR1"
- [ ] Serialize schema to Thrift FileMetaData
- [ ] Write footer at file close

**Week 2: Column Writers - Primitives**
- [ ] ColumnWriter protocol
- [ ] Int32ColumnWriter (PLAIN encoding)
- [ ] Int64ColumnWriter (PLAIN encoding)
- [ ] FloatColumnWriter (PLAIN encoding)
- [ ] DoubleColumnWriter (PLAIN encoding)
- [ ] StringColumnWriter (PLAIN encoding)

**Week 3: Page Writing & Integration**
- [ ] PageWriter implementation
- [ ] Data page creation
- [ ] Uncompressed page writing
- [ ] RowGroupWriter integration
- [ ] End-to-end test: Write simple file, read back with existing reader

**Deliverable**: Can write flat, required-only schemas with PLAIN encoding

---

### Milestone W2: Compression & Dictionary (2 weeks)

**Week 4: Compression**
- [ ] Integrate Snappy compression for page writing
- [ ] Integrate GZIP compression for page writing
- [ ] WriterProperties for compression configuration
- [ ] Test compressed output with Arrow C++

**Week 5: Dictionary Encoding**
- [ ] Dictionary builder for StringColumnWriter
- [ ] Dictionary page writing
- [ ] RLE_DICTIONARY encoding for data pages
- [ ] Adaptive fallback (Dictionary → PLAIN when limit exceeded)
- [ ] Test dictionary files with PyArrow

**Deliverable**: Can write compressed files with dictionary encoding

---

### Milestone W3: Nullable Columns (2 weeks)

**Week 6: Definition Levels**
- [ ] LevelEncoder for RLE/bit-packed hybrid
- [ ] Definition level computation for nullable columns
- [ ] Definition level writing before values in page
- [ ] Update ColumnWriter to handle optionals: `writeValues([Int32?])`

**Week 7: Statistics with Nulls**
- [ ] Statistics computation with null handling
- [ ] null_count tracking
- [ ] min/max with NULL exclusion
- [ ] Write statistics to ColumnMetaData

**Deliverable**: Can write nullable columns with correct level encoding

---

### Milestone W4: Nested Types (3-4 weeks)

**Week 8: Repetition Levels & Lists**
- [ ] Repetition level computation for lists
- [ ] Repetition level encoding
- [ ] ListColumnWriter for `list<primitive>`
- [ ] Test single-level lists

**Week 9: Multi-level Lists & Structs**
- [ ] Multi-level repetition handling
- [ ] StructColumnWriter
- [ ] Nested list support: `list<list<T>>`
- [ ] Test nested structures

**Week 10: Maps**
- [ ] MapColumnWriter
- [ ] Map key/value writing
- [ ] Map-specific level computation
- [ ] Test map files

**Week 11: Integration & Polish**
- [ ] Nested struct children (maps/lists in structs)
- [ ] Lists of structs
- [ ] Comprehensive nested type tests
- [ ] Validation against Arrow C++ reader

**Deliverable**: Can write all nested types supported by reader

---

## Level Encoding Design

### Definition Levels

**Purpose**: Indicate which values are NULL in nullable columns

**Encoding**:
- Use RLE/bit-packed hybrid (same as reading)
- Format: `[length: varint] [data: RLE or bit-packed runs]`

**Algorithm**:
```swift
func computeDefinitionLevels(
    values: [T?],
    maxDefLevel: Int
) -> [UInt16] {
    var levels: [UInt16] = []
    for value in values {
        if value == nil {
            levels.append(UInt16(maxDefLevel - 1))  // NULL
        } else {
            levels.append(UInt16(maxDefLevel))      // Present
        }
    }
    return levels
}
```

### Repetition Levels

**Purpose**: Indicate list boundaries in repeated fields

**Encoding**: RLE/bit-packed hybrid (same format as definition levels)

**Algorithm for Lists**:
```swift
func computeRepetitionLevels<T>(
    lists: [[T?]?],
    repLevel: Int
) -> [UInt16] {
    var levels: [UInt16] = []

    for list in lists {
        guard let list = list else {
            // NULL list - single entry with rep_level = 0
            levels.append(0)
            continue
        }

        if list.isEmpty {
            // Empty list - single entry with rep_level = 0
            levels.append(0)
            continue
        }

        // First element of list
        levels.append(0)

        // Subsequent elements
        for _ in 1..<list.count {
            levels.append(UInt16(repLevel))
        }
    }

    return levels
}
```

**Multi-level Lists** (e.g., `list<list<T>>`):
- Outer list: rep_level = 1
- Inner list: rep_level = 2
- First element of new outer list: rep_level = 0
- First element of new inner list: rep_level = 1
- Continuation of inner list: rep_level = 2

---

## Statistics Design

### Statistics Types

**ColumnChunkMetaData Statistics**:
```swift
struct Statistics {
    var min: Data?           // Encoded min value
    var max: Data?           // Encoded max value
    var nullCount: Int64?    // Count of NULL values
    var distinctCount: Int64? // Cardinality (optional)
}
```

### Computation

**For Each Page**:
```swift
class StatisticsAccumulator<T: Comparable> {
    private var min: T?
    private var max: T?
    private var nullCount: Int64 = 0
    private var valueCount: Int64 = 0

    func update(values: [T?]) {
        for value in values {
            valueCount += 1
            if let v = value {
                if let currentMin = min {
                    min = Swift.min(currentMin, v)
                } else {
                    min = v
                }

                if let currentMax = max {
                    max = Swift.max(currentMax, v)
                } else {
                    max = v
                }
            } else {
                nullCount += 1
            }
        }
    }

    func build() -> Statistics {
        Statistics(
            min: min.map { encode($0) },
            max: max.map { encode($0) },
            nullCount: nullCount,
            distinctCount: nil  // Defer to post-1.0
        )
    }
}
```

**Merging Statistics** (across pages):
```swift
func merge(stats: [Statistics]) -> Statistics {
    var merged = Statistics()

    // min = minimum of all page mins
    merged.min = stats.compactMap { $0.min }.min()

    // max = maximum of all page maxs
    merged.max = stats.compactMap { $0.max }.max()

    // nullCount = sum of all page null counts
    merged.nullCount = stats.compactMap { $0.nullCount }.reduce(0, +)

    return merged
}
```

---

## Dictionary Encoding Strategy

### When to Use Dictionary

**Heuristics** (from Arrow C++):
1. Enable by default for String columns
2. Track unique value count as we write
3. Fall back to PLAIN if:
   - Dictionary size exceeds limit (default: 1MB)
   - Cardinality is high (> 50% unique values)

### Dictionary Building

```swift
class DictionaryBuilder<T: Hashable> {
    private var dictionary: [T] = []
    private var indexMap: [T: Int] = [:]
    private var indices: [Int] = []

    func add(_ value: T) -> Int {
        if let index = indexMap[value] {
            indices.append(index)
            return index
        } else {
            let index = dictionary.count
            dictionary.append(value)
            indexMap[value] = index
            indices.append(index)
            return index
        }
    }

    var dictionarySize: Int {
        // Estimate encoded size
        dictionary.reduce(0) { $0 + encodedSize($1) }
    }

    var shouldUseDictionary: Bool {
        let maxSize = 1024 * 1024  // 1MB
        return dictionarySize < maxSize
    }

    func buildDictionaryPage() -> Data {
        // Encode dictionary values using PLAIN encoding
        var buffer = Data()
        for value in dictionary {
            buffer.append(encodePlain(value))
        }
        return buffer
    }

    func buildDataPage() -> Data {
        // Encode indices using RLE encoding
        return encodeRLE(indices)
    }
}
```

### Page Layout with Dictionary

```
[Dictionary Page]
  - PLAIN encoded values (all unique dictionary entries)

[Data Page 1]
  - RLE encoded indices (references to dictionary)

[Data Page 2]
  - RLE encoded indices

...
```

**Important**: Dictionary page must be written **before** any data pages for that column chunk.

---

## Schema Building API

### Fluent Builder Pattern

```swift
class SchemaBuilder {
    private var fields: [SchemaField] = []

    // Primitive columns
    func addInt32Column(_ name: String, required: Bool = true) -> Self {
        fields.append(.primitive(name, .int32, required ? .required : .optional))
        return self
    }

    func addStringColumn(_ name: String, required: Bool = true) -> Self {
        fields.append(.primitive(name, .byteArray(.string), required ? .required : .optional))
        return self
    }

    // Nested types
    func beginList(_ name: String, required: Bool = true) -> ListBuilder {
        ListBuilder(parent: self, name: name, required: required)
    }

    func beginStruct(_ name: String, required: Bool = true) -> StructBuilder {
        StructBuilder(parent: self, name: name, required: required)
    }

    func beginMap(_ name: String, required: Bool = true) -> MapBuilder {
        MapBuilder(parent: self, name: name, required: required)
    }

    func build() throws -> Schema {
        try Schema(fields: fields)
    }
}

class ListBuilder {
    private let parent: SchemaBuilder
    private let name: String
    private let required: Bool
    private var elementType: SchemaField?

    func withInt32Elements() -> Self {
        elementType = .primitive("element", .int32, .required)
        return self
    }

    func withStructElements(_ configure: (StructBuilder) -> Void) -> Self {
        let structBuilder = StructBuilder(parent: nil, name: "element", required: true)
        configure(structBuilder)
        elementType = structBuilder.build()
        return self
    }

    func endList() -> SchemaBuilder {
        parent.fields.append(.list(name, elementType!, required ? .required : .optional))
        return parent
    }
}
```

**Usage**:
```swift
let schema = try SchemaBuilder()
    .addInt32Column("id")
    .addStringColumn("name", required: false)
    .beginList("tags")
        .withStringElements()
    .endList()
    .beginStruct("address")
        .addStringColumn("street")
        .addStringColumn("city")
    .endStruct()
    .build()
```

---

## Testing Strategy

### Unit Tests

1. **Encoding Tests**:
   - PLAIN encoding for all types
   - Dictionary encoding
   - RLE encoding for indices
   - Level encoding (definition, repetition)

2. **Statistics Tests**:
   - Min/max computation
   - Null count tracking
   - Statistics merging

3. **Page Writing Tests**:
   - Uncompressed pages
   - Snappy compressed pages
   - GZIP compressed pages
   - Dictionary pages

### Integration Tests

1. **Round-trip Tests**:
   - Write file with writer → read with existing reader
   - Validate values match exactly
   - Test all data types

2. **Compatibility Tests**:
   - Write with Parquet-Swift → read with PyArrow
   - Write with Parquet-Swift → read with Arrow C++
   - Write with Parquet-Swift → read with parquet-tools
   - Validate metadata structure

3. **Nested Type Tests**:
   - Lists of primitives
   - Lists of structs
   - Maps
   - Nested lists
   - Structs with complex children

### Validation Tools

**Use parquet-tools for validation**:
```bash
# Inspect schema
parquet-tools schema output.parquet

# Show metadata
parquet-tools meta output.parquet

# Dump first few rows
parquet-tools head output.parquet

# Validate checksums
parquet-tools check output.parquet
```

**Use PyArrow for validation**:
```python
import pyarrow.parquet as pq

# Read file
table = pq.read_table('output.parquet')
print(table.schema)
print(table.to_pandas())

# Validate metadata
metadata = pq.read_metadata('output.parquet')
print(metadata)
```

---

## Error Handling

### Writer Errors

```swift
enum WriterError: Error, LocalizedError {
    case schemaNotSet
    case invalidState(String)
    case rowGroupNotOpen
    case columnAlreadyWritten(Int)
    case incompatibleType(expected: PhysicalType, actual: PhysicalType)
    case valueSizeMismatch(expected: Int, actual: Int)
    case compressionFailed(CompressionType, underlying: Error)
    case encodingFailed(Encoding, underlying: Error)
    case ioError(underlying: Error)
    case invalidSchema(String)
}
```

### State Validation

```swift
class ParquetFileWriter {
    private enum State {
        case created
        case schemaSet
        case rowGroupOpen
        case closed
    }

    private var state: State = .created

    func setSchema(_ schema: Schema) throws {
        guard state == .created else {
            throw WriterError.invalidState("Schema must be set before creating row groups")
        }
        self.schema = schema
        state = .schemaSet
    }

    func createRowGroup() throws -> RowGroupWriter {
        guard state == .schemaSet || state == .rowGroupOpen else {
            throw WriterError.invalidState("Must set schema before creating row group")
        }
        // If previous row group open, close it first
        if state == .rowGroupOpen {
            try currentRowGroup?.close()
        }

        let rowGroup = RowGroupWriter(...)
        currentRowGroup = rowGroup
        state = .rowGroupOpen
        return rowGroup
    }
}
```

---

## Performance Considerations

### Memory Efficiency

1. **Page-level buffering**: Don't hold entire column in memory
2. **Streaming writes**: Flush pages as they fill
3. **Dictionary size limits**: Prevent unbounded growth

### Write Throughput

**Target**: 100-200 MB/s for PLAIN encoding (without compression)

**Optimizations** (defer to Phase 13):
- SIMD for encoding
- Parallel compression
- Async I/O

### Memory Budget

**Per Column Writer**:
- Page buffer: 1MB (configurable)
- Dictionary: 1MB max (fallback to PLAIN if exceeded)
- Level buffers: ~10KB typically

**Per Row Group**:
- N columns × (1-2 MB per column) = manageable for typical schemas

---

## Dependencies

### Internal
- ✅ Thrift serialization (already implemented for reading)
- ✅ Compression codecs (Snappy, GZIP)
- ✅ Schema representation
- ✅ PlainEncoder (inverse of PlainDecoder)

### New Components
- RLE Encoder (inverse of RLE Decoder)
- Level computation logic
- Statistics computation
- File output sink abstraction

---

## Risks & Mitigation

### Risk 1: Level Encoding Bugs (HIGH)
**Impact**: Corrupt files, unreadable by other tools
**Mitigation**:
- Extensive unit tests for level computation
- Round-trip tests (write → read back)
- Validation with PyArrow/Arrow C++
- Start with simple cases, add complexity gradually

### Risk 2: Statistics Correctness (MEDIUM)
**Impact**: Query engines may skip row groups incorrectly
**Mitigation**:
- Test min/max computation thoroughly
- Compare with PyArrow-generated statistics
- Allow disabling statistics if needed

### Risk 3: Dictionary Fallback Edge Cases (MEDIUM)
**Impact**: Performance degradation or encoding errors
**Mitigation**:
- Test dictionary size limit handling
- Test fallback during page writes
- Provide clear configuration options

### Risk 4: Nested Type Complexity (HIGH)
**Impact**: Development time, potential bugs
**Mitigation**:
- Implement flat types first (W1-W3)
- Add nested types incrementally (W4)
- Comprehensive test fixtures
- Reference Arrow C++ implementation closely

---

## Success Criteria

### Functional Requirements
- ✅ Write files readable by PyArrow
- ✅ Write files readable by Arrow C++
- ✅ Write files readable by parquet-tools
- ✅ Write files readable by existing Parquet-Swift reader
- ✅ Support all primitive types
- ✅ Support nullable columns
- ✅ Support nested types (lists, maps, structs)
- ✅ Correct metadata structure
- ✅ Valid checksums (if enabled)

### Quality Requirements
- 200+ writer-specific unit tests
- 50+ integration tests (round-trip)
- 20+ compatibility tests (PyArrow, Arrow C++)
- All tests passing on macOS and Linux
- Memory usage < 2x data size for typical workloads
- Write throughput ≥ 50 MB/s for PLAIN+Snappy

---

## Milestone Summary

| Milestone | Duration | Focus | Deliverable |
|-----------|----------|-------|-------------|
| **W1** | 3 weeks | Basic file writer | Write simple flat files |
| **W2** | 2 weeks | Compression & Dictionary | Production-quality files |
| **W3** | 2 weeks | Nullable columns | Full nullable support |
| **W4** | 3-4 weeks | Nested types | Complete nested type support |
| **Total** | **10-11 weeks** | | **1.0 Release Candidate** |

---

## Next Steps

1. ✅ Complete this design document
2. Create `Sources/Parquet/Writer/` directory structure
3. Implement ParquetFileWriter shell (file creation, footer writing)
4. Implement PlainEncoder for Int32 (simplest case)
5. Implement basic PageWriter (uncompressed)
6. Write first integration test (write → read Int32 column)
7. Iterate on remaining types and features

**Start with**: Milestone W1, Week 1 - File Structure & Metadata

---

## Appendix: File Format Reminder

### Parquet File Structure
```
[Magic Number: "PAR1"]
[Row Group 1]
  [Column Chunk 1]
    [Dictionary Page] (optional)
    [Data Page 1]
    [Data Page 2]
    ...
  [Column Chunk 2]
    ...
[Row Group 2]
  ...
[Footer: FileMetaData (Thrift)]
[Footer Length: 4 bytes]
[Magic Number: "PAR1"]
```

### Data Page Structure (V1)
```
[Page Header (Thrift)]
  - page_type: DATA_PAGE
  - uncompressed_size: int32
  - compressed_size: int32
  - num_values: int32
  - encoding: PLAIN | RLE_DICTIONARY
  - definition_level_encoding: RLE (always)
  - repetition_level_encoding: RLE (always)

[Page Data]
  [Repetition Levels: RLE encoded] (if maxRepLevel > 0)
  [Definition Levels: RLE encoded] (if maxDefLevel > 0)
  [Values: PLAIN or RLE_DICTIONARY encoded]
```

### Dictionary Page Structure
```
[Page Header (Thrift)]
  - page_type: DICTIONARY_PAGE
  - uncompressed_size: int32
  - compressed_size: int32
  - num_values: int32 (number of dictionary entries)
  - encoding: PLAIN (always for dictionary page)

[Page Data]
  [Dictionary Values: PLAIN encoded]
```

---

**Document Status**: Complete - Ready for implementation
**Next Action**: Begin Milestone W1 - File Structure & Metadata
