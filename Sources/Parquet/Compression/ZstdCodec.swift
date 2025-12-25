// ZSTD compression codec
//
// Licensed under the Apache License, Version 2.0

import Foundation
import libzstd

/// Zstandard (ZSTD) compression codec
///
/// ZSTD provides excellent compression ratio with good speed.
/// It's increasingly popular in production Parquet files.
///
/// # Implementation
///
/// Uses the official [Facebook ZSTD](https://github.com/facebook/zstd) C library.
///
/// # Performance
///
/// - Compression ratio: Good (30-50% of original)
/// - Compression speed: Fast
/// - Decompression speed: Very fast
/// - CPU usage: Moderate
///
/// # Usage
///
/// ```swift
/// let codec = ZstdCodec()
/// let decompressed = try codec.decompress(compressed, uncompressedSize: 1024)
/// ```
struct ZstdCodec: Codec {
    var compressionType: Compression { .zstd }

    func decompress(_ data: Data, uncompressedSize: Int) throws -> Data {
        // Handle empty data case
        guard !data.isEmpty else {
            if uncompressedSize == 0 {
                return Data()
            }
            throw CodecError.invalidData("Empty compressed data")
        }

        // Use ZSTD_decompress directly with the known uncompressedSize from Parquet metadata.
        // This is necessary because Parquet provides the size separately, and some writers
        // may not include it in the ZSTD frame header (which decompressFrame relies on).
        var output = Data(count: uncompressedSize)

        let result = data.withUnsafeBytes { compressedPtr -> Int in
            output.withUnsafeMutableBytes { outputPtr -> Int in
                guard let src = compressedPtr.baseAddress,
                      let dst = outputPtr.baseAddress else { return -1 }
                return ZSTD_decompress(dst, uncompressedSize, src, data.count)
            }
        }

        if ZSTD_isError(result) != 0 {
            if let errPtr = ZSTD_getErrorName(result) {
                throw CodecError.decompressionFailed("ZSTD: \(String(cString: errPtr))")
            }
            throw CodecError.decompressionFailed("ZSTD decompression failed")
        }

        guard result == uncompressedSize else {
            throw CodecError.sizeMismatch(expected: uncompressedSize, actual: result)
        }

        return output
    }

    func compress(_ data: Data) throws -> Data {
        // Handle empty data case
        guard !data.isEmpty else {
            return Data()
        }

        let maxCompressedSize = ZSTD_compressBound(data.count)
        var output = Data(count: maxCompressedSize)

        // Use default compression level (3) - good balance of speed and ratio
        let compressionLevel: Int32 = 3

        let result = data.withUnsafeBytes { srcPtr -> Int in
            output.withUnsafeMutableBytes { dstPtr -> Int in
                guard let src = srcPtr.baseAddress,
                      let dst = dstPtr.baseAddress else { return -1 }
                return ZSTD_compress(dst, maxCompressedSize, src, data.count, compressionLevel)
            }
        }

        if ZSTD_isError(result) != 0 {
            if let errPtr = ZSTD_getErrorName(result) {
                throw CodecError.compressionFailed("ZSTD: \(String(cString: errPtr))")
            }
            throw CodecError.compressionFailed("ZSTD compression failed")
        }

        // Trim to actual compressed size
        return output.prefix(result)
    }
}
