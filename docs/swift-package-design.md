# Parquet-Swift Package Design

**Date:** 2025-11-02
**Purpose:** Define the Swift Package structure and architecture

---

## Package Overview

**Name:** `parquet-swift`
**Platforms:** macOS 10.15+, iOS 13+, Linux
**Swift Version:** 5.9+ (for potential C++ interop)
**License:** Apache 2.0

---

## Package Structure

### Directory Layout

```
parquet-swift/
├── Package.swift                      # Swift Package manifest
├── README.md                          # Project overview
├── LICENSE                            # Apache 2.0 license
├── .gitignore                         # Git ignore rules
├── .gitmodules                        # Git submodule config (arrow)
│
├── docs/                              # Documentation
│   ├── cpp-analysis.md               # C++ analysis (✓ created)
│   ├── implementation-roadmap.md     # Roadmap (✓ created)
│   ├── swift-package-design.md       # This file
│   └── api-guide.md                  # User-facing API guide (TBD)
│
├── Sources/                           # Source code
│   └── Parquet/                      # Main module
│       ├── Core/                     # Core types and protocols
│       │   ├── Types.swift
│       │   ├── LogicalTypes.swift
│       │   ├── PhysicalTypes.swift
│       │   └── Errors.swift
│       │
│       ├── Schema/                   # Schema representation
│       │   ├── Node.swift
│       │   ├── PrimitiveNode.swift
│       │   ├── GroupNode.swift
│       │   ├── SchemaDescriptor.swift
│       │   └── ColumnDescriptor.swift
│       │
│       ├── Metadata/                 # File metadata
│       │   ├── FileMetadata.swift
│       │   ├── RowGroupMetadata.swift
│       │   ├── ColumnChunkMetadata.swift
│       │   └── Statistics.swift
│       │
│       ├── Thrift/                   # Thrift serialization
│       │   ├── ThriftProtocol.swift
│       │   ├── ParquetTypes.swift    # Generated/manual Thrift types
│       │   └── ThriftSerialization.swift
│       │
│       ├── IO/                       # I/O abstractions
│       │   ├── RandomAccessFile.swift
│       │   ├── OutputStream.swift
│       │   ├── BufferedReader.swift
│       │   ├── BufferedWriter.swift
│       │   └── FileHandle+Extensions.swift
│       │
│       ├── Compression/              # Compression support
│       │   ├── Codec.swift
│       │   ├── GZIPCodec.swift
│       │   ├── SnappyCodec.swift
│       │   ├── LZ4Codec.swift        # Optional
│       │   └── ZSTDCodec.swift       # Optional
│       │
│       ├── Encoding/                 # Encoding/decoding
│       │   ├── Encoder.swift
│       │   ├── Decoder.swift
│       │   ├── PlainEncoder.swift
│       │   ├── PlainDecoder.swift
│       │   ├── DictionaryEncoder.swift
│       │   ├── DictionaryDecoder.swift
│       │   ├── RLEEncoder.swift
│       │   ├── RLEDecoder.swift
│       │   ├── DeltaEncoders.swift
│       │   ├── DeltaDecoders.swift
│       │   └── LevelEncoding.swift   # Def/rep levels
│       │
│       ├── Reader/                   # Reading API
│       │   ├── ParquetFileReader.swift
│       │   ├── RowGroupReader.swift
│       │   ├── ColumnReader.swift
│       │   ├── PageReader.swift
│       │   └── StreamReader.swift    # Phase 4
│       │
│       ├── Writer/                   # Writing API (Phase 3)
│       │   ├── ParquetFileWriter.swift
│       │   ├── RowGroupWriter.swift
│       │   ├── ColumnWriter.swift
│       │   ├── PageWriter.swift
│       │   └── StreamWriter.swift    # Phase 4
│       │
│       ├── Properties/               # Configuration
│       │   ├── ReaderProperties.swift
│       │   └── WriterProperties.swift
│       │
│       ├── Advanced/                 # Advanced features (Phase 4)
│       │   ├── BloomFilter.swift
│       │   ├── PageIndex.swift
│       │   └── ColumnIndex.swift
│       │
│       └── Utilities/                # Utilities
│           ├── BitPacking.swift
│           ├── ByteBuffer.swift
│           ├── VarintEncoding.swift
│           └── Endianness.swift
│
├── Tests/                             # Test suite
│   └── ParquetTests/
│       ├── Core/
│       │   ├── TypesTests.swift
│       │   └── LogicalTypesTests.swift
│       ├── Schema/
│       │   ├── SchemaTests.swift
│       │   └── NodeTests.swift
│       ├── Encoding/
│       │   ├── PlainEncodingTests.swift
│       │   ├── DictionaryEncodingTests.swift
│       │   └── RLEEncodingTests.swift
│       ├── Reader/
│       │   ├── FileReaderTests.swift
│       │   └── ColumnReaderTests.swift
│       ├── Writer/
│       │   ├── FileWriterTests.swift
│       │   └── ColumnWriterTests.swift
│       ├── Integration/
│       │   ├── RoundTripTests.swift
│       │   └── CompatibilityTests.swift
│       └── Resources/
│           └── TestData/             # Test .parquet files
│               ├── simple.parquet
│               ├── nested.parquet
│               └── (files from parquet-testing)
│
├── Examples/                          # Example projects
│   ├── ReadExample/
│   │   ├── Package.swift
│   │   └── Sources/
│   │       └── main.swift
│   └── WriteExample/
│       ├── Package.swift
│       └── Sources/
│           └── main.swift
│
└── third_party/                       # Third-party dependencies
    └── arrow/                         # Apache Arrow (reference only)
```

---

## Package.swift

```swift
// swift-tools-version: 5.9

import PackageDescription

let package = Package(
    name: "parquet-swift",
    platforms: [
        .macOS(.v10_15),
        .iOS(.v13),
        .watchOS(.v6),
        .tvOS(.v13),
        .linux
    ],
    products: [
        .library(
            name: "Parquet",
            targets: ["Parquet"]
        ),
    ],
    dependencies: [
        // Compression dependencies (if not using system libraries)
        // .package(url: "https://github.com/apple/swift-nio.git", from: "2.0.0"), // For ByteBuffer?
    ],
    targets: [
        .target(
            name: "Parquet",
            dependencies: [],
            path: "Sources/Parquet",
            resources: [
                // If we need to embed any resources
            ]
        ),
        .testTarget(
            name: "ParquetTests",
            dependencies: ["Parquet"],
            path: "Tests/ParquetTests",
            resources: [
                .copy("Resources/TestData")
            ]
        ),
    ]
)
```

---

## Module Architecture

### Core Module: `Parquet`

All code lives in a single module for simplicity. Internal components are marked with `internal` or `private` access control.

### Public API Surface

**Phase 1 (Reading):**
```swift
// Reading a file
let reader = try ParquetFileReader(path: "data.parquet")
let metadata = reader.metadata
let rowGroup = try reader.rowGroup(at: 0)
let column = try rowGroup.column(at: 0)
let values = try column.readBatch(count: 1000)
```

**Phase 3 (Writing):**
```swift
// Writing a file
let schema = try SchemaBuilder()
    .addColumn("id", type: .int32, repetition: .required)
    .addColumn("name", type: .string, repetition: .optional)
    .build()

let writer = try ParquetFileWriter(path: "output.parquet", schema: schema)
let rowGroupWriter = try writer.appendRowGroup()
try rowGroupWriter.writeColumn(0, values: [1, 2, 3])
try rowGroupWriter.writeColumn(1, values: ["Alice", "Bob", nil])
try writer.close()
```

**Phase 4 (Streaming):**
```swift
// Stream reading
for try await row in ParquetStreamReader(path: "data.parquet") {
    print(row["name"])
}

// Stream writing
let writer = try ParquetStreamWriter(path: "output.parquet", schema: schema)
try await writer.write(["id": 1, "name": "Alice"])
try await writer.write(["id": 2, "name": "Bob"])
try writer.close()
```

---

## Type System Design

### Physical Types

```swift
public enum PhysicalType {
    case boolean
    case int32
    case int64
    case int96
    case float
    case double
    case byteArray
    case fixedLenByteArray(Int)
}
```

### Logical Types

```swift
public protocol LogicalType {
    var physicalType: PhysicalType { get }
    func isCompatible(with: PhysicalType) -> Bool
}

public struct StringLogicalType: LogicalType {
    public let physicalType: PhysicalType = .byteArray
}

public struct DateLogicalType: LogicalType {
    public let physicalType: PhysicalType = .int32
}

// etc.
```

### Type-Safe Value Reading

```swift
public protocol ColumnValue {
    associatedtype Value
    static var physicalType: PhysicalType { get }
}

extension Int32: ColumnValue {
    public static let physicalType: PhysicalType = .int32
}

extension String: ColumnValue {
    public static let physicalType: PhysicalType = .byteArray
}

// Usage:
let values: [Int32] = try column.read(count: 100)
```

---

## Schema Representation

### Node Hierarchy

```swift
public protocol Node: AnyObject {
    var name: String { get }
    var repetition: Repetition { get }
    var logicalType: LogicalType? { get }
    var fieldID: Int? { get }
}

public final class PrimitiveNode: Node {
    public let physicalType: PhysicalType
    public let typeLength: Int? // For FIXED_LEN_BYTE_ARRAY
    // ... other properties
}

public final class GroupNode: Node {
    public let fields: [Node]

    public func field(at index: Int) -> Node
    public func field(named name: String) -> Node?
}
```

### Schema Builder

```swift
public final class SchemaBuilder {
    public func addColumn(
        _ name: String,
        type: PhysicalType,
        logicalType: LogicalType? = nil,
        repetition: Repetition = .required
    ) -> SchemaBuilder

    public func addGroup(
        _ name: String,
        repetition: Repetition = .required,
        _ buildFields: (SchemaBuilder) -> Void
    ) -> SchemaBuilder

    public func build() throws -> Schema
}
```

---

## Error Handling

### Error Types

```swift
public enum ParquetError: Error {
    // File errors
    case fileNotFound(String)
    case invalidMagicBytes
    case corruptedMetadata(String)

    // Schema errors
    case incompatibleSchema(String)
    case columnNotFound(String)
    case typeMismatch(expected: PhysicalType, actual: PhysicalType)

    // Encoding errors
    case unsupportedEncoding(Encoding)
    case decodingFailed(String)
    case encodingFailed(String)

    // I/O errors
    case readFailed(String)
    case writeFailed(String)

    // Compression errors
    case unsupportedCompression(Compression)
    case decompressionFailed(String)
    case compressionFailed(String)

    // Other
    case notImplemented(String)
    case internalError(String)
}
```

---

## Encoding/Decoding Design

### Decoder Protocol

```swift
public protocol Decoder: AnyObject {
    var encoding: Encoding { get }
    var valuesLeft: Int { get }

    func setData(_ data: Data, valueCount: Int) throws
}

public protocol TypedDecoder<T>: Decoder {
    associatedtype T
    func decode(into buffer: UnsafeMutableBufferPointer<T>) throws -> Int
}
```

### Encoder Protocol

```swift
public protocol Encoder: AnyObject {
    var encoding: Encoding { get }
    var estimatedSize: Int { get }

    func flush() throws -> Data
}

public protocol TypedEncoder<T>: Encoder {
    associatedtype T
    func encode(_ values: UnsafeBufferPointer<T>) throws
}
```

---

## Memory Management

### Buffer Strategy

- Use `Data` for most operations (safe, but copies)
- Use `UnsafeRawBufferPointer` for zero-copy operations where performance matters
- Consider `ContiguousArray` for homogeneous value storage
- Implement simple `ByteBuffer` wrapper if needed

### Memory Pools

Optional: Implement simple memory pool for frequently allocated buffers
```swift
public protocol MemoryPool {
    func allocate(size: Int) -> UnsafeMutableRawBufferPointer
    func deallocate(_ buffer: UnsafeMutableRawBufferPointer)
}
```

---

## I/O Abstraction

### RandomAccessFile Protocol

```swift
public protocol RandomAccessFile {
    func read(at offset: Int64, count: Int) throws -> Data
    func size() throws -> Int64
    func close() throws
}

public final class FileRandomAccessFile: RandomAccessFile {
    private let fileHandle: FileHandle

    public init(path: String) throws {
        self.fileHandle = try FileHandle(forReadingFrom: URL(fileURLWithPath: path))
    }

    public func read(at offset: Int64, count: Int) throws -> Data {
        try fileHandle.seek(toOffset: UInt64(offset))
        return try fileHandle.read(upToCount: count) ?? Data()
    }

    // ...
}
```

### Buffering

```swift
public final class BufferedReader {
    private let source: RandomAccessFile
    private var buffer: Data
    private var bufferOffset: Int64

    public init(source: RandomAccessFile, bufferSize: Int = 16384) {
        self.source = source
        // ...
    }

    public func read(at offset: Int64, count: Int) throws -> Data {
        // Check if data is in buffer, otherwise fetch
    }
}
```

---

## Compression Integration

### Codec Protocol

```swift
public protocol Codec {
    var compression: Compression { get }

    func compress(_ data: Data) throws -> Data
    func decompress(_ data: Data, uncompressedSize: Int) throws -> Data
}
```

### Implementation Strategy

**GZIP:** Use Foundation's `Compression` framework
```swift
import Compression

public final class GZIPCodec: Codec {
    public let compression: Compression = .gzip

    public func decompress(_ data: Data, uncompressedSize: Int) throws -> Data {
        return try data.withUnsafeBytes { (input: UnsafeRawBufferPointer) in
            var output = Data(count: uncompressedSize)
            return try output.withUnsafeMutableBytes { (outputBuffer: UnsafeMutableRawBufferPointer) in
                let size = compression_decode_buffer(
                    outputBuffer.baseAddress!.assumingMemoryBound(to: UInt8.self),
                    outputBuffer.count,
                    input.baseAddress!.assumingMemoryBound(to: UInt8.self),
                    input.count,
                    nil,
                    COMPRESSION_ZLIB
                )
                // ...
            }
        }
    }
}
```

**Snappy:** Use C library via SwiftPM system target
```swift
// In Package.swift:
.systemLibrary(
    name: "CSnappy",
    pkgConfig: "snappy",
    providers: [
        .brew(["snappy"]),
        .apt(["libsnappy-dev"])
    ]
)

// In code:
import CSnappy

public final class SnappyCodec: Codec {
    // Wrap C functions
}
```

---

## Testing Strategy

### Unit Tests

Each component tested in isolation with XCTest:

```swift
import XCTest
@testable import Parquet

final class PlainDecoderTests: XCTestCase {
    func testDecodeInt32() throws {
        let data = Data([0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00])
        let decoder = PlainDecoder<Int32>()
        try decoder.setData(data, valueCount: 2)

        var buffer = [Int32](repeating: 0, count: 2)
        let count = try buffer.withUnsafeMutableBufferPointer { ptr in
            try decoder.decode(into: ptr)
        }

        XCTAssertEqual(count, 2)
        XCTAssertEqual(buffer, [1, 2])
    }
}
```

### Integration Tests

Test with real Parquet files:

```swift
final class IntegrationTests: XCTestCase {
    func testReadSimpleFile() throws {
        let bundle = Bundle.module
        let url = bundle.url(forResource: "simple", withExtension: "parquet")!

        let reader = try ParquetFileReader(path: url.path)
        XCTAssertEqual(reader.metadata.numRows, 100)

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.column(at: 0)
        let values: [Int32] = try column.read(count: 100)

        XCTAssertEqual(values.count, 100)
    }
}
```

---

## Documentation

### Inline Documentation

Use Swift's documentation markup:

```swift
/// Reads a Parquet file from disk.
///
/// This class provides the main entry point for reading Parquet files.
/// It handles file-level metadata parsing and provides access to row groups.
///
/// # Example
///
/// ```swift
/// let reader = try ParquetFileReader(path: "data.parquet")
/// print("Number of rows: \(reader.metadata.numRows)")
/// ```
///
/// - Note: The file is opened when the reader is created and closed when the reader is deallocated.
public final class ParquetFileReader {
    /// The file metadata containing schema and statistics.
    public let metadata: FileMetadata

    /// Creates a new Parquet file reader.
    ///
    /// - Parameter path: The path to the Parquet file.
    /// - Throws: `ParquetError.fileNotFound` if the file doesn't exist.
    public init(path: String) throws {
        // ...
    }
}
```

### DocC Documentation

Generate with `swift package generate-documentation`

---

## Build and Distribution

### Swift Package Manager

Primary distribution method. Users add:

```swift
dependencies: [
    .package(url: "https://github.com/user/parquet-swift.git", from: "1.0.0")
]
```

### Platform Support

- **macOS:** Full support (main development platform)
- **Linux:** Full support (CI testing)
- **iOS:** Full support (may need to embed compression libraries)
- **watchOS/tvOS:** Best effort (may have limitations)

### Versioning

Follow Semantic Versioning 2.0.0:
- Major: Breaking API changes
- Minor: New features, backward compatible
- Patch: Bug fixes

---

## Performance Considerations

### Hot Paths

Identify and optimize:
1. Decoding (especially PLAIN and RLE)
2. Level encoding/decoding
3. Dictionary lookups
4. Buffer allocations

### Profiling

Use Instruments (Xcode) to profile:
- Time Profiler
- Allocations
- Leaks

### Benchmarks

Create benchmark suite:
```swift
import XCTest

final class BenchmarkTests: XCTestCase {
    func testReadPerformance() {
        measure {
            // Read a large file
        }
    }
}
```

---

## Next Steps

1. ✅ Create directory structure
2. ✅ Write `Package.swift`
3. ✅ Implement core types (`Types.swift`)
4. Write first tests
5. Implement Thrift support

---

**End of Package Design Document**
