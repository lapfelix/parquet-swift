// Codec protocol for compression/decompression
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Protocol for compression/decompression codecs.
///
/// Codecs handle compression and decompression of Parquet page data.
/// Each compression algorithm implements this protocol.
///
/// # Usage
///
/// ```swift
/// let codec = try CodecFactory.codec(for: .gzip)
/// let decompressed = try codec.decompress(compressedData, uncompressedSize: 1024)
/// ```
///
/// # Thread Safety
///
/// Codecs are stateless and thread-safe. Multiple threads can safely
/// use the same codec instance concurrently.
public protocol Codec {
    /// The compression type this codec implements
    var compressionType: Compression { get }

    /// Decompress data
    ///
    /// - Parameters:
    ///   - data: The compressed data
    ///   - uncompressedSize: Expected size of uncompressed data (for validation)
    /// - Returns: The decompressed data
    /// - Throws: `CodecError` if decompression fails
    func decompress(_ data: Data, uncompressedSize: Int) throws -> Data

    /// Compress data
    ///
    /// - Parameter data: The data to compress
    /// - Returns: The compressed data
    /// - Throws: `CodecError` if compression fails
    func compress(_ data: Data) throws -> Data
}

/// Errors that can occur during compression/decompression
public enum CodecError: Error, Equatable {
    /// Codec not available (missing library or unsupported platform)
    case unavailable(String)

    /// Decompression failed
    case decompressionFailed(String)

    /// Compression failed
    case compressionFailed(String)

    /// Size mismatch after decompression
    case sizeMismatch(expected: Int, actual: Int)

    /// Invalid input data
    case invalidData(String)
}

extension CodecError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .unavailable(let msg):
            return "Codec unavailable: \(msg)"
        case .decompressionFailed(let msg):
            return "Decompression failed: \(msg)"
        case .compressionFailed(let msg):
            return "Compression failed: \(msg)"
        case .sizeMismatch(let expected, let actual):
            return "Size mismatch: expected \(expected) bytes, got \(actual) bytes"
        case .invalidData(let msg):
            return "Invalid data: \(msg)"
        }
    }
}

// MARK: - Codec Factory

/// Factory for creating codec instances
public struct CodecFactory {
    /// Get a codec for the specified compression type
    ///
    /// - Parameter compression: The compression type
    /// - Returns: A codec instance
    /// - Throws: `CodecError.unavailable` if codec is not available
    public static func codec(for compression: Compression) throws -> Codec {
        switch compression {
        case .uncompressed:
            return UncompressedCodec()

        case .gzip:
            return GzipCodec()

        case .snappy:
            // Pure Swift Snappy implementation (always available)
            return SnappyCodec()

        case .lz4, .lz4Raw, .zstd, .brotli, .lzo:
            throw CodecError.unavailable("\(compression.rawValue) codec not yet implemented (Phase 2+)")
        }
    }

    /// Check if a codec is available
    ///
    /// - Parameter compression: The compression type
    /// - Returns: true if the codec can be instantiated
    public static func isAvailable(_ compression: Compression) -> Bool {
        do {
            _ = try codec(for: compression)
            return true
        } catch {
            return false
        }
    }
}

// MARK: - Uncompressed Codec

/// Codec for uncompressed data (no-op)
struct UncompressedCodec: Codec {
    var compressionType: Compression { .uncompressed }

    func decompress(_ data: Data, uncompressedSize: Int) throws -> Data {
        guard data.count == uncompressedSize else {
            throw CodecError.sizeMismatch(expected: uncompressedSize, actual: data.count)
        }
        return data
    }

    func compress(_ data: Data) throws -> Data {
        return data
    }
}
