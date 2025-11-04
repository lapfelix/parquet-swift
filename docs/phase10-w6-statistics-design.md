# Phase 10 W6: Statistics Generation

**Date**: 2025-11-04
**Status**: Implementation Phase
**Goal**: Add type-safe statistics computation (min/max/null_count) for all column writers

---

## Overview

**Scope**: Track min/max/null_count for all primitive column types and write accurate statistics to ColumnMetaData footer.

**Timeline**: 1-2 weeks
- Part 1 (Statistics Core): 3-4 days
- Part 2 (Integration): 2-3 days
- Part 3 (Testing): 2-3 days

**Prerequisites**: W4 (String nullable), W5 (All primitives nullable)

---

## Design Principles

### 1. Type Safety (No `Any?` Boxing)
**Problem**: Generic `StatisticsAccumulator` with `update([Any?])` requires boxing every value, allocating wrapper arrays, and runtime type checks that fail for non-nullable values.

**Solution**: Strongly-typed accumulators with separate APIs:
```swift
protocol StatisticsAccumulator {
    func build() -> ColumnChunkStatistics
    func reset()
}

// Type-safe, zero-copy APIs
class Int32StatisticsAccumulator: StatisticsAccumulator {
    func update(_ values: [Int32])        // Required columns
    func updateNullable(_ values: [Int32?]) // Nullable columns
}
```

### 2. Zero-Copy Updates
**Problem**: Mapping `values.map { $0 as Any? }` duplicates data and adds heap allocation per batch.

**Solution**: Pass native buffers directly to typed accumulators. No intermediate allocations.

### 3. Correct Thrift Fields
**Problem**: Parquet spec considers `min`/`max` deprecated in favor of `min_value`/`max_value` with different ordering guarantees.

**Solution**: Populate both fields:
- `minValue`/`maxValue`: Modern fields (preferred by Arrow/parquet-mr)
- `min`/`max`: Legacy fields (for backward compatibility)

### 4. Byte-Wise String Comparison
**Problem**: Swift's `String <` uses locale-sensitive ordering, not raw byte ordering required by Parquet spec.

**Solution**: Store strings as `Data` (UTF-8 bytes) and use `.lexicographicallyPrecedes()` for comparison.

### 5. Correct Thrift Serialization
**Problem**: `ThriftStatistics(min:max:...)` constructor doesn't exist - must assign fields individually.

**Solution**: Create empty struct, assign optional properties manually.

---

## Architecture

### Statistics Data Structures

```swift
/// Statistics for a column chunk (matches Thrift format)
public struct ColumnChunkStatistics {
    // Modern fields (preferred by Arrow/parquet-mr)
    var minValue: Data?         // Raw bytes in physical type encoding
    var maxValue: Data?         // Raw bytes in physical type encoding

    // Legacy fields (populate for compatibility)
    var min: Data?              // Same as minValue
    var max: Data?              // Same as maxValue

    var nullCount: Int64?       // Count of NULL values
    var distinctCount: Int64?   // Cardinality (not implemented - future)
}

/// Base protocol for type-safe statistics accumulators
protocol StatisticsAccumulator {
    func build() -> ColumnChunkStatistics
    func reset()
}
```

### Type-Specific Accumulators

Each physical type has its own accumulator:

**Int32StatisticsAccumulator**:
- `update([Int32])` - for required columns
- `updateNullable([Int32?])` - for nullable columns
- Tracks min/max using Swift.min/max
- Encodes as PLAIN Int32 (4 bytes, little-endian)

**Int64StatisticsAccumulator**:
- Same pattern, 8-byte encoding

**FloatStatisticsAccumulator**:
- Skips NaN in min/max (not counted as NULL)
- IEEE 754 single precision encoding

**DoubleStatisticsAccumulator**:
- Same as Float, double precision

**StringStatisticsAccumulator**:
- Stores as `Data` (UTF-8 bytes) internally
- Uses `.lexicographicallyPrecedes()` for byte-wise comparison
- Encodes as PLAIN ByteArray: `[length: 4 bytes LE] [UTF-8 bytes]`

---

## Implementation Plan

### Part 1: Type-Safe Statistics Core (3-4 days)

#### Task 1.1: Statistics Protocol & Struct (0.5 day)
Create `Sources/Parquet/Writer/Statistics.swift` with:
- `ColumnChunkStatistics` struct
- `StatisticsAccumulator` protocol

#### Task 1.2: Int32 Statistics (0.5 day)
Implement `Int32StatisticsAccumulator` with:
- `update([Int32])`
- `updateNullable([Int32?])`
- `encodePlainInt32()` helper
- Populate both modern and legacy fields

#### Task 1.3: Int64 Statistics (0.5 day)
Similar to Int32, 8-byte encoding

#### Task 1.4: Float Statistics (0.5 day)
With NaN handling:
- Skip NaN in min/max computation
- Don't count NaN as NULL

#### Task 1.5: Double Statistics (0.5 day)
Similar to Float, double precision

#### Task 1.6: String Statistics (1 day)
With byte-wise comparison:
- Store as `Data` internally
- Use `.lexicographicallyPrecedes()`
- Encode as PLAIN ByteArray

---

### Part 2: Zero-Copy Integration (2-3 days)

#### Task 2.1: Update Column Writers (1.5 days)

Add to each column writer (Int32, Int64, Float, Double, String):

```swift
private var statisticsAccumulator: Int32StatisticsAccumulator?

init(...) {
    if properties.enableStatistics {
        self.statisticsAccumulator = Int32StatisticsAccumulator()
    }
}

public func writeValues(_ values: [Int32]) throws {
    // Zero-copy statistics update
    statisticsAccumulator?.update(values)

    // ... existing write logic ...
}

public func writeOptionalValues(_ values: [Int32?]) throws {
    // Zero-copy nullable statistics update
    statisticsAccumulator?.updateNullable(values)

    // ... existing write logic ...
}

func close() throws -> WriterColumnChunkMetadata {
    try flush()

    let statistics = statisticsAccumulator?.build()

    return WriterColumnChunkMetadata(
        // ... existing fields ...
        statistics: statistics  // ← NEW
    )
}
```

#### Task 2.2: Update WriterProperties (0.5 day)

Add statistics configuration:
```swift
public struct WriterProperties {
    var enableStatistics: Bool = true  // Enable by default
}
```

#### Task 2.3: Serialize to Thrift (1 day)

Update `ParquetFileWriter.serializeColumnMetaData()`:

```swift
if let stats = meta.statistics {
    var thriftStats = ThriftStatistics()

    // Modern fields (preferred)
    thriftStats.minValue = stats.minValue
    thriftStats.maxValue = stats.maxValue

    // Legacy fields (compatibility)
    thriftStats.min = stats.min
    thriftStats.max = stats.max

    thriftStats.nullCount = stats.nullCount
    thriftStats.distinctCount = stats.distinctCount

    thriftMeta.statistics = thriftStats
}
```

---

### Part 3: Testing (2-3 days)

#### Task 3.1: Type-Safe Unit Tests (1 day)

**`Tests/ParquetTests/Writer/StatisticsTests.swift`**

Test cases:
- Int32 required column (no boxing)
- Int32 nullable column (type-safe)
- String byte-wise comparison (with non-ASCII)
- Float NaN exclusion
- All nulls (no min/max)
- Empty string vs NULL distinction

#### Task 3.2: Integration Tests (1 day)

Test cases:
- Required column round-trip with statistics
- Nullable column round-trip with statistics
- Multi-page statistics (verify merge across pages)
- Verify both modern and legacy fields populated
- Arrow C++/PyArrow compatibility validation

---

## Edge Cases

### Float/Double NaN Handling
- NaN is excluded from min/max computation
- NaN is NOT counted as NULL (it's a valid float value)
- Infinity IS included in min/max

### String Comparison
- Use byte-wise UTF-8 comparison (not locale-sensitive)
- Empty string `""` is valid, different from NULL
- Non-ASCII strings compared by UTF-8 byte values

### All NULL Columns
- min/max fields should be `nil`
- nullCount should equal total rows

### Mixed Pages
- Statistics accumulator tracks across all pages
- Final statistics = min of all page mins, max of all page maxs

---

## API Examples

### Required Column

```swift
let writer = try ParquetFileWriter(url: url)
try writer.setSchema(schema)

let rowGroup = try writer.createRowGroup()
let columnWriter = try rowGroup.int32ColumnWriter(at: 0)

// Type-safe, zero-copy
let values: [Int32] = [100, 50, 200, 75]
try columnWriter.writeValues(values)

try rowGroup.finalizeColumn(at: 0)
try writer.close()

// Read back statistics
let reader = try ParquetFileReader(url: url)
let stats = reader.metadata.rowGroup(0).column(0).statistics!

// min=50, max=200, nullCount=nil
```

### Nullable Column

```swift
let columnWriter = try rowGroup.int32ColumnWriter(at: 0)

// Type-safe nullable API
let values: [Int32?] = [100, nil, 200, nil, 50]
try columnWriter.writeOptionalValues(values)

// Statistics: min=50, max=200, nullCount=2
```

---

## Success Criteria

### Functional
- ✅ Type-safe APIs (no `Any?` boxing)
- ✅ Zero-copy updates (no array mapping)
- ✅ Populate both modern (`minValue`/`maxValue`) and legacy (`min`/`max`) fields
- ✅ Byte-wise string comparison
- ✅ Correct NaN handling for floats
- ✅ Statistics in Thrift footer

### Quality
- ✅ 30+ unit tests (6 per type × 5 types)
- ✅ 10+ integration tests
- ✅ Arrow C++/PyArrow can read statistics
- ✅ Statistics match PyArrow-generated files

### Performance
- ✅ Statistics tracking adds < 5% overhead
- ✅ No heap allocations in update path
- ✅ No type checks at runtime

---

## Task Checklist

### Part 1: Core Implementation
- [ ] Create `Statistics.swift` with protocol and structs
- [ ] Implement `Int32StatisticsAccumulator` (typed APIs)
- [ ] Implement `Int64StatisticsAccumulator`
- [ ] Implement `FloatStatisticsAccumulator` (NaN handling)
- [ ] Implement `DoubleStatisticsAccumulator`
- [ ] Implement `StringStatisticsAccumulator` (byte-wise)

### Part 2: Integration
- [ ] Add statistics to all 5 column writers
- [ ] Update `WriterProperties` with `enableStatistics`
- [ ] Update `WriterColumnChunkMetadata` with `statistics` field
- [ ] Serialize statistics to Thrift (both modern and legacy fields)

### Part 3: Testing
- [ ] Unit tests for all 5 types (required + nullable)
- [ ] Unit tests for edge cases (NaN, all nulls, empty string)
- [ ] Integration tests (round-trip validation)
- [ ] Arrow C++/PyArrow compatibility tests

---

## References

- **Parquet Spec**: Statistics in ColumnMetaData
- **Arrow C++**: `cpp/src/parquet/statistics.cc`
- **PyArrow**: Statistics generation and validation

---

**Ready for Implementation**: Yes
**Next Action**: Task 1.1 - Create Statistics.swift
