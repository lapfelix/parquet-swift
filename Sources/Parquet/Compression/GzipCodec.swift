// GZIP compression codec
//
// Licensed under the Apache License, Version 2.0

import Foundation
#if canImport(Compression)
import Compression
#endif

/// GZIP/DEFLATE compression codec
///
/// Uses Foundation's `Compression` framework for GZIP decompression and compression.
/// Available on macOS 10.15+, iOS 13+, and other Apple platforms.
///
/// # Format
///
/// GZIP uses the DEFLATE algorithm with additional header/footer information.
/// Parquet uses standard GZIP format (RFC 1952).
///
/// # Performance
///
/// - Compression ratio: Good (30-50% of original)
/// - Speed: Moderate (slower than Snappy/LZ4)
/// - CPU usage: Moderate
///
/// # Usage
///
/// ```swift
/// let codec = GzipCodec()
/// let decompressed = try codec.decompress(compressed, uncompressedSize: 1024)
/// ```
struct GzipCodec: Codec {
    var compressionType: Compression { .gzip }

    func decompress(_ data: Data, uncompressedSize: Int) throws -> Data {
        #if canImport(Compression)
        // Handle empty data
        if uncompressedSize == 0 {
            return Data()
        }

        // Use Foundation's compression API
        let decompressed = try data.withUnsafeBytes { inputPtr -> Data in
            guard let inputBaseAddress = inputPtr.baseAddress else {
                throw CodecError.invalidData("Empty input buffer")
            }

            // Allocate output buffer
            var outputBuffer = Data(count: uncompressedSize)

            let decompressedSize = outputBuffer.withUnsafeMutableBytes { outputPtr -> Int in
                guard let outputBaseAddress = outputPtr.baseAddress else {
                    return 0
                }

                return compression_decode_buffer(
                    outputBaseAddress,
                    uncompressedSize,
                    inputBaseAddress,
                    data.count,
                    nil,
                    COMPRESSION_ZLIB // GZIP uses ZLIB/DEFLATE
                )
            }

            guard decompressedSize > 0 else {
                throw CodecError.decompressionFailed("compression_decode_buffer returned \(decompressedSize)")
            }

            guard decompressedSize == uncompressedSize else {
                throw CodecError.sizeMismatch(expected: uncompressedSize, actual: decompressedSize)
            }

            return outputBuffer
        }

        return decompressed

        #else
        throw CodecError.unavailable("Compression framework not available on this platform")
        #endif
    }

    func compress(_ data: Data) throws -> Data {
        #if canImport(Compression)
        // Handle empty data
        if data.isEmpty {
            return Data()
        }

        // Use Foundation's compression API
        let compressed = try data.withUnsafeBytes { inputPtr -> Data in
            guard let inputBaseAddress = inputPtr.baseAddress else {
                throw CodecError.invalidData("Empty input buffer")
            }

            // Estimate output size (worst case: slightly larger than input)
            let maxCompressedSize = data.count + (data.count / 10) + 32

            // Allocate output buffer
            var outputBuffer = Data(count: maxCompressedSize)

            let compressedSize = outputBuffer.withUnsafeMutableBytes { outputPtr -> Int in
                guard let outputBaseAddress = outputPtr.baseAddress else {
                    return 0
                }

                return compression_encode_buffer(
                    outputBaseAddress,
                    maxCompressedSize,
                    inputBaseAddress,
                    data.count,
                    nil,
                    COMPRESSION_ZLIB
                )
            }

            guard compressedSize > 0 else {
                throw CodecError.compressionFailed("compression_encode_buffer returned \(compressedSize)")
            }

            // Trim to actual size
            outputBuffer.count = compressedSize
            return outputBuffer
        }

        return compressed

        #else
        throw CodecError.unavailable("Compression framework not available on this platform")
        #endif
    }
}
