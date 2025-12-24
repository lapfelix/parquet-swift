// ZSTD compression codec
//
// Licensed under the Apache License, Version 2.0

import Foundation
import SwiftZSTD

/// Zstandard (ZSTD) compression codec
///
/// ZSTD provides excellent compression ratio with good speed.
/// It's increasingly popular in production Parquet files.
///
/// # Implementation
///
/// Uses [SwiftZSTD](https://github.com/aperedera/SwiftZSTD), which wraps
/// the official Facebook ZSTD C library.
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

        do {
            let processor = ZSTDProcessor()
            let decompressed = try processor.decompressFrame(data)

            // Verify size matches expectation
            guard decompressed.count == uncompressedSize else {
                throw CodecError.sizeMismatch(expected: uncompressedSize, actual: decompressed.count)
            }

            return decompressed
        } catch let error as CodecError {
            throw error
        } catch {
            throw CodecError.decompressionFailed("ZSTD decompression failed: \(error)")
        }
    }

    func compress(_ data: Data) throws -> Data {
        // Handle empty data case
        guard !data.isEmpty else {
            return Data()
        }

        do {
            let processor = ZSTDProcessor()
            // Use default compression level (3) - good balance of speed and ratio
            return try processor.compressBuffer(data, compressionLevel: 3)
        } catch {
            throw CodecError.compressionFailed("ZSTD compression failed: \(error)")
        }
    }
}
