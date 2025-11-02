// Buffered I/O for efficient reading
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// A buffered reader that wraps a RandomAccessFile.
///
/// Provides efficient reading by:
/// - Maintaining an internal buffer
/// - Reading ahead to reduce system calls
/// - Supporting both sequential and random access
///
/// Typical usage:
/// ```swift
/// let file = try FileRandomAccessFile(url: fileURL)
/// let reader = BufferedReader(file: file, bufferSize: 8192)
///
/// // Sequential reads
/// let header = try reader.read(count: 4)
///
/// // Random access
/// let footer = try reader.read(at: fileSize - 100, count: 100)
/// ```
public final class BufferedReader {
    private let file: RandomAccessFile
    private let bufferSize: Int

    // Buffer state
    private var buffer: Data
    private var bufferOffset: Int  // File offset where buffer starts
    private var position: Int      // Current read position in file

    /// Default buffer size (8 KB)
    public static let defaultBufferSize = 8 * 1024

    /// Create a buffered reader.
    ///
    /// - Parameters:
    ///   - file: The underlying random access file
    ///   - bufferSize: Size of the internal buffer (default: 8 KB)
    public init(file: RandomAccessFile, bufferSize: Int = defaultBufferSize) {
        self.file = file
        self.bufferSize = bufferSize
        self.buffer = Data()
        self.bufferOffset = 0
        self.position = 0
    }

    /// The current read position in the file.
    public var currentPosition: Int {
        return position
    }

    /// The size of the underlying file.
    public var fileSize: Int {
        get throws {
            return try file.size
        }
    }

    // MARK: - Sequential Reading

    /// Read `count` bytes from the current position.
    ///
    /// Advances the current position by `count` bytes.
    ///
    /// - Parameter count: The number of bytes to read
    /// - Returns: The data read
    /// - Throws: `IOError` if the read fails
    public func read(count: Int) throws -> Data {
        let data = try read(at: position, count: count)
        position += count
        return data
    }

    /// Read a single byte from the current position.
    ///
    /// - Returns: The byte value
    /// - Throws: `IOError` if the read fails
    public func readByte() throws -> UInt8 {
        let data = try read(count: 1)
        return data[0]
    }

    // MARK: - Random Access Reading

    /// Read `count` bytes starting at `offset`.
    ///
    /// Does not change the current position.
    ///
    /// - Parameters:
    ///   - offset: The byte offset from the start of the file
    ///   - count: The number of bytes to read
    /// - Returns: The data read
    /// - Throws: `IOError` if the read fails
    public func read(at offset: Int, count: Int) throws -> Data {
        guard count > 0 else {
            return Data()
        }

        let fileSize = try file.size

        // Validate range
        guard offset >= 0 && offset + count <= fileSize else {
            throw IOError.invalidOffset(offset, fileSize: fileSize)
        }

        // Check if requested data is in buffer
        if isInBuffer(offset: offset, count: count) {
            let bufferStart = offset - bufferOffset
            let bufferEnd = bufferStart + count
            return buffer.subdata(in: bufferStart..<bufferEnd)
        }

        // For small reads, use buffering
        if count <= bufferSize {
            try fillBuffer(at: offset)
            let bufferStart = offset - bufferOffset
            let bufferEnd = bufferStart + count
            return buffer.subdata(in: bufferStart..<bufferEnd)
        }

        // For large reads, bypass buffer
        return try file.read(at: offset, count: count)
    }

    /// Read all remaining data from the current position to end of file.
    ///
    /// - Returns: The data read
    /// - Throws: `IOError` if the read fails
    public func readToEnd() throws -> Data {
        let fileSize = try file.size
        let remaining = fileSize - position
        return try read(count: remaining)
    }

    // MARK: - Position Management

    /// Seek to a specific position in the file.
    ///
    /// - Parameter offset: The byte offset from the start of the file
    /// - Throws: `IOError` if the offset is invalid
    public func seek(to offset: Int) throws {
        let fileSize = try file.size
        guard offset >= 0 && offset <= fileSize else {
            throw IOError.invalidOffset(offset, fileSize: fileSize)
        }
        position = offset
    }

    /// Seek relative to the current position.
    ///
    /// - Parameter delta: The number of bytes to move (positive = forward, negative = backward)
    /// - Throws: `IOError` if the resulting offset is invalid
    public func seek(by delta: Int) throws {
        try seek(to: position + delta)
    }

    // MARK: - Buffer Management

    /// Check if the requested range is fully contained in the current buffer.
    private func isInBuffer(offset: Int, count: Int) -> Bool {
        guard !buffer.isEmpty else {
            return false
        }

        let rangeStart = offset
        let rangeEnd = offset + count
        let bufferEnd = bufferOffset + buffer.count

        return rangeStart >= bufferOffset && rangeEnd <= bufferEnd
    }

    /// Fill the buffer starting at the given offset.
    private func fillBuffer(at offset: Int) throws {
        let fileSize = try file.size
        let readSize = min(bufferSize, fileSize - offset)

        buffer = try file.read(at: offset, count: readSize)
        bufferOffset = offset
    }

    /// Clear the internal buffer.
    public func clearBuffer() {
        buffer = Data()
        bufferOffset = 0
    }

    // MARK: - Resource Management

    /// Close the underlying file.
    public func close() throws {
        try file.close()
    }

    deinit {
        try? close()
    }
}

// MARK: - Convenience Extensions

extension BufferedReader {
    /// Read a 32-bit little-endian unsigned integer.
    public func readUInt32LE() throws -> UInt32 {
        let data = try read(count: 4)
        return data.withUnsafeBytes { $0.load(as: UInt32.self).littleEndian }
    }

    /// Read a 32-bit little-endian unsigned integer at a specific offset.
    public func readUInt32LE(at offset: Int) throws -> UInt32 {
        let data = try read(at: offset, count: 4)
        return data.withUnsafeBytes { $0.load(as: UInt32.self).littleEndian }
    }

    /// Read a 64-bit little-endian unsigned integer.
    public func readUInt64LE() throws -> UInt64 {
        let data = try read(count: 8)
        return data.withUnsafeBytes { $0.load(as: UInt64.self).littleEndian }
    }

    /// Read a 64-bit little-endian unsigned integer at a specific offset.
    public func readUInt64LE(at offset: Int) throws -> UInt64 {
        let data = try read(at: offset, count: 8)
        return data.withUnsafeBytes { $0.load(as: UInt64.self).littleEndian }
    }
}
