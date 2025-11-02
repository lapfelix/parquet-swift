# Parquet-Swift API Guide

**Status:** Draft / Under Construction
**Last Updated:** 2025-11-02

> **Note:** This guide will be populated as the API is implemented. The examples shown are planned designs and may change during development.

---

## Overview

Parquet-Swift provides a native Swift interface for reading and writing Apache Parquet files. The API is designed to be type-safe, idiomatic, and easy to use while maintaining compatibility with the Parquet specification.

---

## Installation

### Swift Package Manager

Add to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/[user]/parquet-swift.git", from: "1.0.0")
]
```

Then import:

```swift
import Parquet
```

---

## Quick Start

### Reading a Parquet File

**Basic Example:**

```swift
import Parquet

// Open a file
let reader = try ParquetFileReader(path: "data.parquet")

// Access metadata
print("Schema: \(reader.metadata.schema)")
print("Number of rows: \(reader.metadata.numRows)")
print("Number of row groups: \(reader.metadata.numRowGroups)")

// Read a row group
let rowGroup = try reader.rowGroup(at: 0)

// Read a column
let column = try rowGroup.column(at: 0)
let values: [Int32] = try column.read(count: 100)

print("First 10 values: \(values.prefix(10))")
```

### Writing a Parquet File

**Basic Example:**

```swift
import Parquet

// Define schema
let schema = try SchemaBuilder()
    .addColumn("id", type: .int32, repetition: .required)
    .addColumn("name", type: .string, repetition: .optional)
    .addColumn("age", type: .int32, repetition: .optional)
    .build()

// Create writer
let writer = try ParquetFileWriter(path: "output.parquet", schema: schema)

// Write a row group
let rowGroupWriter = try writer.appendRowGroup()

// Write columns
try rowGroupWriter.writeColumn(0, values: [1, 2, 3, 4, 5])
try rowGroupWriter.writeColumn(1, values: ["Alice", "Bob", nil, "Charlie", "Diana"])
try rowGroupWriter.writeColumn(2, values: [30, 25, nil, 35, 28])

// Close the file
try writer.close()
```

---

## Core Concepts

### Schema

Parquet uses a hierarchical schema with two types of nodes:
- **PrimitiveNode**: Leaf columns with actual data
- **GroupNode**: Nested structures containing other nodes

**Example:**

```swift
// Flat schema
let flatSchema = try SchemaBuilder()
    .addColumn("id", type: .int64)
    .addColumn("name", type: .string)
    .build()

// Nested schema
let nestedSchema = try SchemaBuilder()
    .addColumn("id", type: .int64)
    .addGroup("person", repetition: .required) { builder in
        builder
            .addColumn("name", type: .string)
            .addColumn("age", type: .int32)
    }
    .addGroup("tags", repetition: .repeated) { builder in
        builder.addColumn("tag", type: .string)
    }
    .build()
```

### Physical Types

Parquet defines these physical types:
- `boolean` - Single bit
- `int32` - 32-bit signed integer
- `int64` - 64-bit signed integer
- `int96` - 96-bit integer (deprecated, for timestamps)
- `float` - IEEE 32-bit floating point
- `double` - IEEE 64-bit floating point
- `byteArray` - Variable-length byte array
- `fixedLenByteArray` - Fixed-length byte array

### Logical Types

Logical types add semantic meaning:
- `String` (UTF-8)
- `Date`
- `Timestamp` (milliseconds, microseconds, nanoseconds)
- `Decimal`
- `UUID`
- `JSON`, `BSON`
- And more...

---

## API Reference

> **Note:** Detailed API documentation will be generated with Swift-DocC and hosted separately.

### Reading API

#### ParquetFileReader

**Main entry point for reading files.**

```swift
public final class ParquetFileReader {
    /// Open a Parquet file
    public init(path: String) throws

    /// File metadata
    public let metadata: FileMetadata

    /// Number of row groups
    public var numRowGroups: Int { get }

    /// Access a row group
    public func rowGroup(at index: Int) throws -> RowGroupReader

    /// Close the file
    public func close() throws
}
```

#### RowGroupReader

**Access columns within a row group.**

```swift
public final class RowGroupReader {
    /// Row group metadata
    public let metadata: RowGroupMetadata

    /// Number of columns
    public var numColumns: Int { get }

    /// Access a column
    public func column(at index: Int) throws -> ColumnReader

    /// Access a column by name
    public func column(named name: String) throws -> ColumnReader
}
```

#### ColumnReader

**Read values from a column.**

```swift
public final class ColumnReader {
    /// Column metadata
    public let metadata: ColumnChunkMetadata

    /// Read values (type-safe)
    public func read<T: ColumnValue>(count: Int) throws -> [T]

    /// Read a batch with null handling
    public func readBatch<T: ColumnValue>(
        count: Int
    ) throws -> (values: [T], definitionLevels: [Int16], nullCount: Int)

    /// Skip values
    public func skip(count: Int) throws
}
```

### Writing API

#### ParquetFileWriter

**Main entry point for writing files.**

```swift
public final class ParquetFileWriter {
    /// Create a new Parquet file
    public init(path: String, schema: Schema, properties: WriterProperties = .default) throws

    /// Append a row group
    public func appendRowGroup() throws -> RowGroupWriter

    /// Close and finalize the file
    public func close() throws
}
```

#### RowGroupWriter

**Write columns in a row group.**

```swift
public final class RowGroupWriter {
    /// Write a column by index
    public func writeColumn<T: ColumnValue>(
        _ index: Int,
        values: [T?]
    ) throws

    /// Close the row group
    public func close() throws
}
```

### Schema API

#### SchemaBuilder

**Fluent API for building schemas.**

```swift
public final class SchemaBuilder {
    /// Add a primitive column
    public func addColumn(
        _ name: String,
        type: PhysicalType,
        logicalType: LogicalType? = nil,
        repetition: Repetition = .required
    ) -> SchemaBuilder

    /// Add a group (nested structure)
    public func addGroup(
        _ name: String,
        repetition: Repetition = .required,
        _ build: (SchemaBuilder) -> Void
    ) -> SchemaBuilder

    /// Build the schema
    public func build() throws -> Schema
}
```

---

## Advanced Usage

### Streaming API (Phase 4)

**Row-oriented streaming:**

```swift
// Streaming read
for try await row in ParquetStreamReader(path: "data.parquet") {
    let id = row["id"] as? Int32
    let name = row["name"] as? String
    print("\(id): \(name)")
}

// Streaming write
let writer = try ParquetStreamWriter(path: "output.parquet", schema: schema)
try await writer.write(["id": 1, "name": "Alice"])
try await writer.write(["id": 2, "name": "Bob"])
try await writer.close()
```

### Bloom Filters (Phase 4)

**Fast existence checks:**

```swift
let reader = try ParquetFileReader(path: "data.parquet")
let rowGroup = try reader.rowGroup(at: 0)
let column = try rowGroup.column(at: 0)

// Check if value might exist
if try column.bloomFilter?.mightContain(value: "Alice") == true {
    // Value might be in this row group, read it
}
```

### Page Index (Phase 4)

**Selective reading with predicate pushdown:**

```swift
let reader = try ParquetFileReader(path: "data.parquet")
let pageIndex = try reader.pageIndex(for: 0) // Column 0

// Check min/max statistics for each page
for (pageNum, stats) in pageIndex.enumerated() {
    if stats.min <= targetValue && targetValue <= stats.max {
        // Target value might be in this page
    }
}
```

---

## Configuration

### Reader Properties

```swift
var properties = ReaderProperties()
properties.bufferSize = 32 * 1024 // 32 KB buffer
properties.enableBufferedStream = true
properties.verifyPageChecksums = true

let reader = try ParquetFileReader(path: "data.parquet", properties: properties)
```

### Writer Properties

```swift
var properties = WriterProperties()
properties.compression = .snappy
properties.enableDictionary = true
properties.dictionaryPageSizeLimit = 1024 * 1024
properties.dataPageSize = 1024 * 1024
properties.enableStatistics = true

let writer = try ParquetFileWriter(path: "output.parquet", schema: schema, properties: properties)
```

---

## Error Handling

All Parquet operations throw typed errors:

```swift
do {
    let reader = try ParquetFileReader(path: "data.parquet")
    let values: [Int32] = try reader.rowGroup(at: 0).column(at: 0).read(count: 100)
} catch ParquetError.fileNotFound(let path) {
    print("File not found: \(path)")
} catch ParquetError.typeMismatch(expected: let expected, actual: let actual) {
    print("Type mismatch: expected \(expected), got \(actual)")
} catch ParquetError.corruptedMetadata(let message) {
    print("Corrupted metadata: \(message)")
} catch {
    print("Unexpected error: \(error)")
}
```

---

## Type Safety

Parquet-Swift uses Swift's type system for safe value reading:

```swift
// Type-safe reading
let intValues: [Int32] = try column.read(count: 100)
let stringValues: [String] = try column.read(count: 100)

// Runtime error if types don't match
let wrongType: [Int64] = try column.read(count: 100) // Throws typeMismatch
```

---

## Performance Tips

1. **Use buffered reading** for small random accesses
2. **Batch operations** when reading/writing large datasets
3. **Choose appropriate compression** (Snappy for speed, GZIP for size)
4. **Enable dictionary encoding** for repetitive string/binary data
5. **Use row groups wisely** (64-128 MB recommended)
6. **Pre-allocate buffers** when reading known amounts of data

---

## Platform Considerations

### macOS / iOS
- Full support for all features
- Use Foundation's `Compression` framework for GZIP

### Linux
- Full support for all features
- Requires compression libraries (libsnappy, etc.)

---

## Examples

Complete examples are available in the `Examples/` directory:

- **ReadExample** - Basic file reading
- **WriteExample** - Basic file writing
- **NestedSchemaExample** - Working with nested data
- **StreamingExample** - Row-oriented streaming (Phase 4)

---

## Migration from Other Libraries

### From PyArrow (Python)

**PyArrow:**
```python
import pyarrow.parquet as pq

table = pq.read_table('data.parquet')
df = table.to_pandas()
```

**Parquet-Swift:**
```swift
let reader = try ParquetFileReader(path: "data.parquet")
// Process row groups and columns as needed
```

---

## Troubleshooting

### Common Issues

**File not found:**
```swift
// Ensure the path is absolute or relative to working directory
let reader = try ParquetFileReader(path: "/absolute/path/to/data.parquet")
```

**Type mismatch:**
```swift
// Check the column type in metadata first
let columnType = rowGroup.metadata.column(at: 0).type
```

**Unsupported encoding:**
```swift
// Some encodings may not be implemented yet
// Check the metadata for encoding types
```

---

## Contributing

See `CONTRIBUTING.md` for guidelines on:
- Code style
- Testing requirements
- Pull request process
- Reporting bugs

---

## Resources

- [Apache Parquet Documentation](https://parquet.apache.org/docs/)
- [Parquet Format Specification](https://github.com/apache/parquet-format)
- [Swift-DocC Generated API Docs](TBD)

---

**Note:** This API guide will evolve as the library is developed. Check back for updates, or refer to the Swift-DocC generated documentation for the latest API details.

**Last Updated:** 2025-11-02 (Planning Phase)
