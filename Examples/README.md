# Parquet-Swift Examples

This directory contains example code demonstrating how to use parquet-swift for reading and writing Parquet files.

## Examples

### BasicRead.swift
Demonstrates reading Parquet files:
- Opening files and reading metadata
- Reading primitive columns (Int32, String, etc.)
- Reading nested columns (lists, maps, structs)
- Batch reading for large files

### BasicWrite.swift
Demonstrates writing Parquet files:
- Creating schemas
- Writing primitive columns (required and optional)
- Writing lists
- Writing maps
- Writing structs (manual field extraction pattern)

## Running Examples

These examples are provided as reference code. To use them in your own project:

1. Add parquet-swift to your project via Swift Package Manager
2. Copy the example code and adapt it to your needs
3. Update file paths and data structures as needed

## Quick Reference

### Reading a File

```swift
let reader = try ParquetFileReader(url: fileURL)
defer { try? reader.close() }

let rowGroup = try reader.rowGroup(at: 0)
let column = try rowGroup.int32Column(at: 0)
let values = try column.readAll()
```

### Writing a File

```swift
let writer = try ParquetFileWriter(url: outputURL)
try writer.setSchema(schema)
writer.setProperties(.default)

let rowGroup = try writer.createRowGroup()
let columnWriter = try rowGroup.int32ColumnWriter(at: 0)
try columnWriter.writeValues([1, 2, 3])
try rowGroup.finalizeColumn(at: 0)

try writer.close()
```

## Additional Resources

- [README.md](../README.md) - Main project documentation
- [CLAUDE.md](../CLAUDE.md) - Development guide and architecture
- [CHANGELOG.md](../CHANGELOG.md) - Version history

## Notes

- **Schema Creation**: All examples show manual schema creation. In a real application, you might want to create helper functions to build schemas more easily.
- **Struct Writing**: Parquet-swift uses a manual field extraction pattern for writing structs. Extract each field into a separate array and write them as independent columns.
- **Error Handling**: Examples use `try` for simplicity. In production code, use proper error handling with `do-catch` or `Result` types.
- **Compression**: The writer supports UNCOMPRESSED, GZIP, and Snappy compression. Specify via `WriterProperties`.
