// Tests for Codec implementations
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet
import libzstd

final class CodecTests: XCTestCase {
    // MARK: - Codec Factory Tests

    func testCodecFactoryUncompressed() throws {
        let codec = try CodecFactory.codec(for: .uncompressed)
        XCTAssertEqual(codec.compressionType, .uncompressed)
    }

    func testCodecFactoryGzip() throws {
        let codec = try CodecFactory.codec(for: .gzip)
        XCTAssertEqual(codec.compressionType, .gzip)
    }

    func testCodecFactorySnappy() throws {
        // Snappy is now available in Phase 2
        let codec = try CodecFactory.codec(for: .snappy)
        XCTAssertEqual(codec.compressionType, .snappy)
    }

    func testCodecFactoryZstd() throws {
        let codec = try CodecFactory.codec(for: .zstd)
        XCTAssertEqual(codec.compressionType, .zstd)
    }

    func testCodecFactoryUnsupportedCodecs() {
        let unsupported: [Compression] = [.lz4, .lz4Raw, .brotli, .lzo]

        for compression in unsupported {
            XCTAssertThrowsError(try CodecFactory.codec(for: compression)) { error in
                guard case CodecError.unavailable = error else {
                    XCTFail("Expected unavailable error for \(compression)")
                    return
                }
            }
        }
    }

    func testCodecFactoryIsAvailable() {
        XCTAssertTrue(CodecFactory.isAvailable(.uncompressed))
        XCTAssertTrue(CodecFactory.isAvailable(.gzip))
        XCTAssertTrue(CodecFactory.isAvailable(.snappy))
        XCTAssertTrue(CodecFactory.isAvailable(.zstd))
        XCTAssertFalse(CodecFactory.isAvailable(.lz4))
    }

    // MARK: - Uncompressed Codec Tests

    func testUncompressedDecompress() throws {
        let codec = try CodecFactory.codec(for: .uncompressed)
        let data = Data("Hello, World!".utf8)

        let decompressed = try codec.decompress(data, uncompressedSize: data.count)
        XCTAssertEqual(decompressed, data)
    }

    func testUncompressedCompress() throws {
        let codec = try CodecFactory.codec(for: .uncompressed)
        let data = Data("Hello, World!".utf8)

        let compressed = try codec.compress(data)
        XCTAssertEqual(compressed, data)
    }

    func testUncompressedSizeMismatch() throws {
        let codec = try CodecFactory.codec(for: .uncompressed)
        let data = Data("Hello, World!".utf8)

        // Wrong uncompressed size should fail
        XCTAssertThrowsError(try codec.decompress(data, uncompressedSize: 100)) { error in
            guard case CodecError.sizeMismatch(let expected, let actual) = error else {
                XCTFail("Expected sizeMismatch error")
                return
            }
            XCTAssertEqual(expected, 100)
            XCTAssertEqual(actual, data.count)
        }
    }

    // MARK: - GZIP Codec Tests

    func testGzipRoundTrip() throws {
        let codec = try CodecFactory.codec(for: .gzip)
        let original = Data("Hello, World! This is a test string for compression.".utf8)

        // Compress
        let compressed = try codec.compress(original)

        // Should be smaller (or at least different)
        print("Original: \(original.count) bytes, Compressed: \(compressed.count) bytes")

        // Decompress
        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)

        // Should match original
        XCTAssertEqual(decompressed, original)
    }

    func testGzipLargeData() throws {
        let codec = try CodecFactory.codec(for: .gzip)

        // Create 10KB of repeated data (compresses well)
        let pattern = Data("ABCDEFGHIJ".utf8)
        var original = Data()
        for _ in 0..<1000 {
            original.append(pattern)
        }

        // Compress
        let compressed = try codec.compress(original)

        // Should achieve good compression
        let ratio = Double(compressed.count) / Double(original.count)
        print("Compression ratio: \(ratio)")
        XCTAssertLessThan(compressed.count, original.count, "Compressed should be smaller")

        // Decompress
        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)
        XCTAssertEqual(decompressed, original)
    }

    func testGzipEmptyData() throws {
        let codec = try CodecFactory.codec(for: .gzip)
        let empty = Data()

        let compressed = try codec.compress(empty)
        let decompressed = try codec.decompress(compressed, uncompressedSize: 0)

        XCTAssertEqual(decompressed, empty)
    }

    func testGzipBinaryData() throws {
        let codec = try CodecFactory.codec(for: .gzip)

        // Create binary data (0-255 repeated)
        var original = Data()
        for i in 0..<256 {
            original.append(UInt8(i))
        }
        original.append(contentsOf: original) // 512 bytes

        let compressed = try codec.compress(original)
        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)

        XCTAssertEqual(decompressed, original)
    }

    func testGzipInvalidCompressedData() throws {
        let codec = try CodecFactory.codec(for: .gzip)

        // Invalid GZIP data
        let invalid = Data([0xFF, 0xFF, 0xFF, 0xFF])

        XCTAssertThrowsError(try codec.decompress(invalid, uncompressedSize: 100)) { error in
            guard case CodecError.decompressionFailed = error else {
                XCTFail("Expected decompressionFailed error")
                return
            }
        }
    }

    func testGzipSizeMismatch() throws {
        let codec = try CodecFactory.codec(for: .gzip)
        let original = Data("Hello, World!".utf8)

        let compressed = try codec.compress(original)

        // Wrong uncompressed size
        XCTAssertThrowsError(try codec.decompress(compressed, uncompressedSize: 1000)) { error in
            guard case CodecError.sizeMismatch = error else {
                XCTFail("Expected sizeMismatch error")
                return
            }
        }
    }

    // MARK: - Snappy Codec Tests

    func testSnappyRoundTrip() throws {
        let codec = try CodecFactory.codec(for: .snappy)
        let original = Data("Hello, World! This is a test string for Snappy compression.".utf8)

        // Compress
        let compressed = try codec.compress(original)

        // Should be smaller (or at least different)
        print("Snappy Original: \(original.count) bytes, Compressed: \(compressed.count) bytes")

        // Decompress
        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)

        // Should match original
        XCTAssertEqual(decompressed, original)
    }

    func testSnappyLargeData() throws {
        let codec = try CodecFactory.codec(for: .snappy)

        // Create 10KB of repeated data (compresses well)
        let pattern = Data("ABCDEFGHIJ".utf8)
        var original = Data()
        for _ in 0..<1000 {
            original.append(pattern)
        }

        // Compress
        let compressed = try codec.compress(original)

        // Should achieve compression
        let ratio = Double(compressed.count) / Double(original.count)
        print("Snappy compression ratio: \(ratio)")
        XCTAssertLessThan(compressed.count, original.count, "Compressed should be smaller")

        // Decompress
        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)
        XCTAssertEqual(decompressed, original)
    }

    func testSnappyEmptyData() throws {
        let codec = try CodecFactory.codec(for: .snappy)
        let empty = Data()

        let compressed = try codec.compress(empty)
        let decompressed = try codec.decompress(compressed, uncompressedSize: 0)

        XCTAssertEqual(decompressed, empty)
    }

    func testSnappyBinaryData() throws {
        let codec = try CodecFactory.codec(for: .snappy)

        // Create binary data (0-255 repeated)
        var original = Data()
        for i in 0..<256 {
            original.append(UInt8(i))
        }
        original.append(contentsOf: original) // 512 bytes

        let compressed = try codec.compress(original)
        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)

        XCTAssertEqual(decompressed, original)
    }

    func testSnappyRepeatingPattern() throws {
        let codec = try CodecFactory.codec(for: .snappy)

        // Highly compressible data
        let original = Data(repeating: 0x42, count: 1000)

        let compressed = try codec.compress(original)

        // Should compress very well
        let ratio = Double(compressed.count) / Double(original.count)
        print("Snappy repeating pattern compression ratio: \(ratio)")
        XCTAssertLessThan(ratio, 0.5, "Repeating data should compress significantly")

        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)
        XCTAssertEqual(decompressed, original)
    }

    func testSnappyTextData() throws {
        let codec = try CodecFactory.codec(for: .snappy)

        // Realistic text data
        let text = """
        Lorem ipsum dolor sit amet, consectetur adipiscing elit.
        Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
        Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.
        Lorem ipsum dolor sit amet, consectetur adipiscing elit.
        Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
        """

        let original = Data(text.utf8)
        let compressed = try codec.compress(original)

        let ratio = Double(compressed.count) / Double(original.count)
        print("Snappy text data compression ratio: \(ratio)")
        XCTAssertLessThan(ratio, 1.0, "Text should compress")

        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)
        XCTAssertEqual(decompressed, original)
    }

    // MARK: - ZSTD Codec Tests

    func testZstdRoundTrip() throws {
        let codec = try CodecFactory.codec(for: .zstd)
        let original = Data("Hello, World! This is a test string for ZSTD compression.".utf8)

        // Compress
        let compressed = try codec.compress(original)

        // Should be smaller (or at least different)
        print("ZSTD Original: \(original.count) bytes, Compressed: \(compressed.count) bytes")

        // Decompress
        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)

        // Should match original
        XCTAssertEqual(decompressed, original)
    }

    func testZstdLargeData() throws {
        let codec = try CodecFactory.codec(for: .zstd)

        // Create 10KB of repeated data (compresses well)
        let pattern = Data("ABCDEFGHIJ".utf8)
        var original = Data()
        for _ in 0..<1000 {
            original.append(pattern)
        }

        // Compress
        let compressed = try codec.compress(original)

        // Should achieve compression
        let ratio = Double(compressed.count) / Double(original.count)
        print("ZSTD compression ratio: \(ratio)")
        XCTAssertLessThan(compressed.count, original.count, "Compressed should be smaller")

        // Decompress
        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)
        XCTAssertEqual(decompressed, original)
    }

    func testZstdEmptyData() throws {
        let codec = try CodecFactory.codec(for: .zstd)
        let empty = Data()

        let compressed = try codec.compress(empty)
        let decompressed = try codec.decompress(compressed, uncompressedSize: 0)

        XCTAssertEqual(decompressed, empty)
    }

    func testZstdBinaryData() throws {
        let codec = try CodecFactory.codec(for: .zstd)

        // Create binary data (0-255 repeated)
        var original = Data()
        for i in 0..<256 {
            original.append(UInt8(i))
        }
        original.append(contentsOf: original) // 512 bytes

        let compressed = try codec.compress(original)
        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)

        XCTAssertEqual(decompressed, original)
    }

    func testZstdRepeatingPattern() throws {
        let codec = try CodecFactory.codec(for: .zstd)

        // Highly compressible data
        let original = Data(repeating: 0x42, count: 1000)

        let compressed = try codec.compress(original)

        // Should compress very well
        let ratio = Double(compressed.count) / Double(original.count)
        print("ZSTD repeating pattern compression ratio: \(ratio)")
        XCTAssertLessThan(ratio, 0.5, "Repeating data should compress significantly")

        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)
        XCTAssertEqual(decompressed, original)
    }

    func testZstdTextData() throws {
        let codec = try CodecFactory.codec(for: .zstd)

        // Realistic text data
        let text = """
        Lorem ipsum dolor sit amet, consectetur adipiscing elit.
        Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
        Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.
        Lorem ipsum dolor sit amet, consectetur adipiscing elit.
        Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
        """

        let original = Data(text.utf8)
        let compressed = try codec.compress(original)

        let ratio = Double(compressed.count) / Double(original.count)
        print("ZSTD text data compression ratio: \(ratio)")
        XCTAssertLessThan(ratio, 1.0, "Text should compress")

        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)
        XCTAssertEqual(decompressed, original)
    }

    func testZstdSizeMismatch() throws {
        let codec = try CodecFactory.codec(for: .zstd)
        let original = Data("Hello, World!".utf8)

        let compressed = try codec.compress(original)

        // Wrong uncompressed size
        XCTAssertThrowsError(try codec.decompress(compressed, uncompressedSize: 1000)) { error in
            guard case CodecError.sizeMismatch = error else {
                XCTFail("Expected sizeMismatch error")
                return
            }
        }
    }

    func testZstdDecompressWithKnownSize() throws {
        // This test verifies that ZSTD decompression works when we provide
        // the uncompressed size externally (as Parquet does via page metadata),
        // rather than relying on the size being embedded in the ZSTD frame header.
        // Some Parquet writers don't include the size in the frame header.
        let codec = try CodecFactory.codec(for: .zstd)

        // Test with various data sizes to ensure the known-size decompression works
        let testCases: [(String, Data)] = [
            ("small", Data("Hello".utf8)),
            ("medium", Data(repeating: 0xAB, count: 1000)),
            ("large", Data((0..<10000).map { UInt8($0 % 256) })),
        ]

        for (name, original) in testCases {
            let compressed = try codec.compress(original)

            // Decompress using the known size (simulating Parquet's approach)
            let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)

            XCTAssertEqual(decompressed, original, "\(name) data should round-trip correctly")
            XCTAssertEqual(decompressed.count, original.count, "\(name) size should match exactly")
        }
    }

    func testZstdDecompressWithoutFrameContentSize() throws {
        // This test creates ZSTD-compressed data WITHOUT the content size in the frame header.
        // This would have failed with SwiftZSTD's decompressFrame (which uses ZSTD_getDecompressedSize),
        // but works with our implementation that uses ZSTD_decompress with the known size from Parquet.
        let original = Data("Test data for ZSTD without content size in frame header.".utf8)

        // Compress using ZSTD C API with content size disabled in frame header
        let cctx = ZSTD_createCCtx()
        defer { ZSTD_freeCCtx(cctx) }

        // Disable content size in frame header (simulates some Parquet writers)
        ZSTD_CCtx_setParameter(cctx, ZSTD_c_contentSizeFlag, 0)

        let maxCompressedSize = ZSTD_compressBound(original.count)
        var compressed = Data(count: maxCompressedSize)

        let compressedSize = original.withUnsafeBytes { srcPtr -> Int in
            compressed.withUnsafeMutableBytes { dstPtr -> Int in
                guard let src = srcPtr.baseAddress,
                      let dst = dstPtr.baseAddress else { return 0 }
                return ZSTD_compress2(cctx, dst, maxCompressedSize, src, original.count)
            }
        }

        XCTAssertFalse(ZSTD_isError(compressedSize) != 0, "Compression should succeed")
        compressed = compressed.prefix(compressedSize)

        // Verify the frame does NOT contain the content size
        let frameContentSize = compressed.withUnsafeBytes { ptr -> UInt64 in
            guard let base = ptr.baseAddress else { return 0 }
            return ZSTD_getFrameContentSize(base, compressed.count)
        }
        // ZSTD_CONTENTSIZE_UNKNOWN = UInt64.max (0xFFFFFFFFFFFFFFFF)
        XCTAssertEqual(frameContentSize, UInt64.max, "Frame should not contain content size")

        // Our codec should still decompress successfully using the known size
        let codec = try CodecFactory.codec(for: .zstd)
        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)

        XCTAssertEqual(decompressed, original, "Should decompress correctly with externally-provided size")
    }

    // MARK: - Real-World Data Tests

    func testGzipRepeatingPattern() throws {
        let codec = try CodecFactory.codec(for: .gzip)

        // Highly compressible data
        let original = Data(repeating: 0x42, count: 1000)

        let compressed = try codec.compress(original)

        // Should compress very well
        let ratio = Double(compressed.count) / Double(original.count)
        print("Repeating pattern compression ratio: \(ratio)")
        XCTAssertLessThan(ratio, 0.1, "Repeating data should compress to < 10%")

        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)
        XCTAssertEqual(decompressed, original)
    }

    func testGzipRandomData() throws {
        let codec = try CodecFactory.codec(for: .gzip)

        // Random data (doesn't compress well)
        var original = Data(count: 1000)
        for i in 0..<1000 {
            original[i] = UInt8.random(in: 0...255)
        }

        let compressed = try codec.compress(original)

        // May not compress much (random data)
        let ratio = Double(compressed.count) / Double(original.count)
        print("Random data compression ratio: \(ratio)")

        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)
        XCTAssertEqual(decompressed, original)
    }

    func testGzipTextData() throws {
        let codec = try CodecFactory.codec(for: .gzip)

        // Realistic text data
        let text = """
        Lorem ipsum dolor sit amet, consectetur adipiscing elit.
        Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
        Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.
        Lorem ipsum dolor sit amet, consectetur adipiscing elit.
        Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
        """

        let original = Data(text.utf8)
        let compressed = try codec.compress(original)

        let ratio = Double(compressed.count) / Double(original.count)
        print("Text data compression ratio: \(ratio)")
        XCTAssertLessThan(ratio, 0.8, "Text should compress reasonably")

        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)
        XCTAssertEqual(decompressed, original)
    }

    // MARK: - Error Description Tests

    func testCodecErrorDescriptions() {
        let unavailable = CodecError.unavailable("Test message")
        XCTAssertTrue(unavailable.description.contains("unavailable"))

        let decompFailed = CodecError.decompressionFailed("Test")
        XCTAssertTrue(decompFailed.description.contains("Decompression failed"))

        let compFailed = CodecError.compressionFailed("Test")
        XCTAssertTrue(compFailed.description.contains("Compression failed"))

        let sizeMismatch = CodecError.sizeMismatch(expected: 100, actual: 50)
        XCTAssertTrue(sizeMismatch.description.contains("100"))
        XCTAssertTrue(sizeMismatch.description.contains("50"))

        let invalid = CodecError.invalidData("Test")
        XCTAssertTrue(invalid.description.contains("Invalid data"))
    }
}
