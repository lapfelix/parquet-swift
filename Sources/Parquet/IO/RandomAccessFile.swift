// RandomAccessFile protocol for Parquet I/O
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Protocol for random access file reading.
///
/// Provides an abstraction over file I/O that supports:
/// - Reading arbitrary byte ranges
/// - Seeking to specific positions
/// - Getting file size
///
/// This abstraction enables:
/// - Testing with in-memory buffers
/// - Network-backed storage
/// - Custom I/O implementations
public protocol RandomAccessFile {
    /// The total size of the file in bytes.
    var size: Int { get throws }

    /// Read exactly `count` bytes starting at `offset`.
    ///
    /// - Parameters:
    ///   - offset: The byte offset from the start of the file
    ///   - count: The number of bytes to read
    /// - Returns: The data read from the file
    /// - Throws: `IOError` if the read fails or reaches end of file
    func read(at offset: Int, count: Int) throws -> Data

    /// Read data from `offset` to the end of the file.
    ///
    /// - Parameter offset: The byte offset from the start of the file
    /// - Returns: The data read from offset to end of file
    /// - Throws: `IOError` if the read fails
    func readToEnd(from offset: Int) throws -> Data

    /// Close the file and release any resources.
    func close() throws
}

/// Errors that can occur during I/O operations.
public enum IOError: Error, CustomStringConvertible {
    case fileNotFound(String)
    case readFailed(String)
    case seekFailed(String)
    case endOfFile
    case invalidOffset(Int, fileSize: Int)
    case closeFailed(String)

    public var description: String {
        switch self {
        case .fileNotFound(let path):
            return "File not found: \(path)"
        case .readFailed(let msg):
            return "Read failed: \(msg)"
        case .seekFailed(let msg):
            return "Seek failed: \(msg)"
        case .endOfFile:
            return "Unexpected end of file"
        case .invalidOffset(let offset, let fileSize):
            return "Invalid offset \(offset) for file of size \(fileSize)"
        case .closeFailed(let msg):
            return "Close failed: \(msg)"
        }
    }
}

// MARK: - FileHandle-based Implementation

/// Random access file backed by a `FileHandle`.
public final class FileRandomAccessFile: RandomAccessFile {
    private let fileHandle: FileHandle
    private let fileURL: URL
    private let _size: Int

    /// Create a random access file from a URL.
    ///
    /// - Parameter url: The URL of the file to open
    /// - Throws: `IOError.fileNotFound` if the file doesn't exist
    public init(url: URL) throws {
        guard FileManager.default.fileExists(atPath: url.path) else {
            throw IOError.fileNotFound(url.path)
        }

        self.fileURL = url

        do {
            self.fileHandle = try FileHandle(forReadingFrom: url)

            // Get file size
            let attributes = try FileManager.default.attributesOfItem(atPath: url.path)
            guard let fileSize = attributes[.size] as? Int else {
                throw IOError.readFailed("Could not determine file size")
            }
            self._size = fileSize
        } catch {
            throw IOError.readFailed("Failed to open file: \(error.localizedDescription)")
        }
    }

    public var size: Int {
        get throws {
            return _size
        }
    }

    public func read(at offset: Int, count: Int) throws -> Data {
        // Validate offset and count
        guard offset >= 0 else {
            throw IOError.invalidOffset(offset, fileSize: _size)
        }

        guard offset + count <= _size else {
            throw IOError.invalidOffset(offset + count, fileSize: _size)
        }

        guard count > 0 else {
            return Data()
        }

        do {
            // Seek to offset
            if #available(macOS 10.15.4, iOS 13.4, *) {
                try fileHandle.seek(toOffset: UInt64(offset))
            } else {
                fileHandle.seek(toFileOffset: UInt64(offset))
            }

            // Read data
            let data: Data
            if #available(macOS 10.15.4, iOS 13.4, *) {
                guard let readData = try fileHandle.read(upToCount: count) else {
                    throw IOError.endOfFile
                }
                data = readData
            } else {
                data = fileHandle.readData(ofLength: count)
            }

            guard data.count == count else {
                throw IOError.endOfFile
            }

            return data
        } catch let error as IOError {
            throw error
        } catch {
            throw IOError.readFailed("Failed to read at offset \(offset): \(error.localizedDescription)")
        }
    }

    public func readToEnd(from offset: Int) throws -> Data {
        let remaining = _size - offset
        return try read(at: offset, count: remaining)
    }

    public func close() throws {
        do {
            if #available(macOS 10.15, iOS 13.0, *) {
                try fileHandle.close()
            } else {
                fileHandle.closeFile()
            }
        } catch {
            throw IOError.closeFailed(error.localizedDescription)
        }
    }

    deinit {
        try? close()
    }
}

// MARK: - In-Memory Implementation for Testing

/// Random access file backed by in-memory data.
///
/// Useful for testing without filesystem I/O.
public final class MemoryRandomAccessFile: RandomAccessFile {
    private let data: Data

    /// Create a random access file from in-memory data.
    ///
    /// - Parameter data: The data to wrap
    public init(data: Data) {
        self.data = data
    }

    public var size: Int {
        get throws {
            return data.count
        }
    }

    public func read(at offset: Int, count: Int) throws -> Data {
        guard offset >= 0 else {
            throw IOError.invalidOffset(offset, fileSize: data.count)
        }

        guard offset + count <= data.count else {
            throw IOError.invalidOffset(offset + count, fileSize: data.count)
        }

        guard count > 0 else {
            return Data()
        }

        let range = offset..<(offset + count)
        return data.subdata(in: range)
    }

    public func readToEnd(from offset: Int) throws -> Data {
        let remaining = data.count - offset
        return try read(at: offset, count: remaining)
    }

    public func close() throws {
        // No-op for in-memory file
    }
}
