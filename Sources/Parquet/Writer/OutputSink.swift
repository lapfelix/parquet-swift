// OutputSink.swift - Abstraction for writing bytes to various destinations
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Protocol for writing bytes to a destination (file, memory buffer, etc.)
///
/// This abstraction allows the writer to work with different output targets
/// without coupling to specific I/O implementations.
protocol OutputSink {
    /// Write bytes to the output
    /// - Parameter data: The data to write
    /// - Throws: I/O errors if write fails
    func write(_ data: Data) throws

    /// Get current write position (for offset tracking)
    /// - Returns: Current byte offset in the output stream
    /// - Throws: I/O errors if position cannot be determined
    func tell() throws -> Int64

    /// Flush any buffered data to the underlying destination
    /// - Throws: I/O errors if flush fails
    func flush() throws

    /// Close the output sink
    /// - Throws: I/O errors if close fails
    func close() throws
}

// MARK: - File-based Output Sink

/// Output sink that writes to a file on disk
final class FileOutputSink: OutputSink {
    private let fileHandle: FileHandle
    private let url: URL
    private var isClosed: Bool = false

    /// Create a file output sink
    /// - Parameter url: File URL to write to
    /// - Throws: I/O errors if file cannot be created
    init(url: URL) throws {
        self.url = url

        // Create file if it doesn't exist
        if !FileManager.default.fileExists(atPath: url.path) {
            FileManager.default.createFile(atPath: url.path, contents: nil)
        }

        // Open for writing
        self.fileHandle = try FileHandle(forWritingTo: url)

        // Truncate existing content
        try fileHandle.truncate(atOffset: 0)
    }

    func write(_ data: Data) throws {
        guard !isClosed else {
            throw WriterError.sinkClosed
        }

        try fileHandle.write(contentsOf: data)
    }

    func tell() throws -> Int64 {
        guard !isClosed else {
            throw WriterError.sinkClosed
        }

        return Int64(try fileHandle.offset())
    }

    func flush() throws {
        guard !isClosed else {
            throw WriterError.sinkClosed
        }

        try fileHandle.synchronize()
    }

    func close() throws {
        guard !isClosed else {
            return  // Already closed
        }

        try fileHandle.synchronize()
        try fileHandle.close()
        isClosed = true
    }
}

// MARK: - Memory-based Output Sink

/// Output sink that writes to an in-memory buffer
final class MemoryOutputSink: OutputSink {
    private(set) var buffer: Data
    private var isClosed: Bool = false

    init() {
        self.buffer = Data()
    }

    func write(_ data: Data) throws {
        guard !isClosed else {
            throw WriterError.sinkClosed
        }

        buffer.append(data)
    }

    func tell() throws -> Int64 {
        return Int64(buffer.count)
    }

    func flush() throws {
        // No-op for memory sink
    }

    func close() throws {
        isClosed = true
    }
}

// MARK: - Writer Errors

enum WriterError: Error, LocalizedError {
    case sinkClosed
    case schemaNotSet
    case invalidState(String)
    case rowGroupNotOpen
    case rowGroupAlreadyOpen
    case columnIndexOutOfBounds(Int)
    case columnAlreadyWritten(Int)
    case incompatibleType(expected: PhysicalType, actual: String)
    case valueSizeMismatch(expected: Int, actual: Int)
    case compressionFailed(Compression, underlying: Error)
    case encodingFailed(Encoding, underlying: Error)
    case ioError(underlying: Error)
    case invalidSchema(String)
    case thriftSerializationError(String)

    var errorDescription: String? {
        switch self {
        case .sinkClosed:
            return "Output sink is closed"
        case .schemaNotSet:
            return "Schema must be set before creating row groups"
        case .invalidState(let message):
            return "Invalid writer state: \(message)"
        case .rowGroupNotOpen:
            return "No row group is currently open"
        case .rowGroupAlreadyOpen:
            return "A row group is already open"
        case .columnIndexOutOfBounds(let index):
            return "Column index \(index) is out of bounds"
        case .columnAlreadyWritten(let index):
            return "Column \(index) has already been written"
        case .incompatibleType(let expected, let actual):
            return "Incompatible type: expected \(expected), got \(actual)"
        case .valueSizeMismatch(let expected, let actual):
            return "Value size mismatch: expected \(expected), got \(actual)"
        case .compressionFailed(let type, let error):
            return "Compression failed (\(type)): \(error.localizedDescription)"
        case .encodingFailed(let encoding, let error):
            return "Encoding failed (\(encoding)): \(error.localizedDescription)"
        case .ioError(let error):
            return "I/O error: \(error.localizedDescription)"
        case .invalidSchema(let message):
            return "Invalid schema: \(message)"
        case .thriftSerializationError(let message):
            return "Thrift serialization error: \(message)"
        }
    }
}
