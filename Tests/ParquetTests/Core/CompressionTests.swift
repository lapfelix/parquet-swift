// Tests for Compression
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class CompressionTests: XCTestCase {
    // MARK: - Phase 1 Codecs

    func testUncompressedCodec() {
        let codec = Compression.uncompressed
        XCTAssertTrue(codec.isPhase1Supported)
        XCTAssertTrue(codec.isImplemented)
        XCTAssertFalse(codec.requiresExternalLibrary)
        XCTAssertEqual(codec.typicalRatio, 1.0...1.0)
        XCTAssertEqual(codec.relativeSpeed, 0)
        XCTAssertEqual(codec.rawValue, "UNCOMPRESSED")
    }

    func testGZIPCodec() {
        let codec = Compression.gzip
        XCTAssertTrue(codec.isPhase1Supported)
        XCTAssertTrue(codec.isImplemented)
        XCTAssertFalse(codec.requiresExternalLibrary, "GZIP uses Foundation")
        XCTAssertEqual(codec.typicalRatio, 0.3...0.5)
        XCTAssertEqual(codec.relativeSpeed, 3)
        XCTAssertEqual(codec.rawValue, "GZIP")
    }

    func testSnappyCodec() {
        let codec = Compression.snappy
        XCTAssertTrue(codec.isPhase1Supported, "Snappy is best-effort in Phase 1")
        XCTAssertFalse(codec.isImplemented, "Not yet implemented")
        XCTAssertTrue(codec.requiresExternalLibrary)
        XCTAssertEqual(codec.typicalRatio, 0.5...0.7)
        XCTAssertEqual(codec.relativeSpeed, 1)
        XCTAssertEqual(codec.rawValue, "SNAPPY")
    }

    // MARK: - Phase 2+ Codecs

    func testLZ4Codec() {
        let codec = Compression.lz4
        XCTAssertFalse(codec.isPhase1Supported)
        XCTAssertFalse(codec.isImplemented)
        XCTAssertTrue(codec.requiresExternalLibrary)
        XCTAssertEqual(codec.relativeSpeed, 1)
    }

    func testLZ4RawCodec() {
        let codec = Compression.lz4Raw
        XCTAssertFalse(codec.isPhase1Supported)
        XCTAssertTrue(codec.requiresExternalLibrary)
    }

    func testZSTDCodec() {
        let codec = Compression.zstd
        XCTAssertFalse(codec.isPhase1Supported)
        XCTAssertTrue(codec.requiresExternalLibrary)
        XCTAssertEqual(codec.typicalRatio, 0.3...0.5)
        XCTAssertEqual(codec.relativeSpeed, 2)
    }

    func testBrotliCodec() {
        let codec = Compression.brotli
        XCTAssertFalse(codec.isPhase1Supported)
        XCTAssertTrue(codec.requiresExternalLibrary)
        XCTAssertEqual(codec.typicalRatio, 0.2...0.4)
        XCTAssertEqual(codec.relativeSpeed, 4)
    }

    func testLZOCodec() {
        let codec = Compression.lzo
        XCTAssertFalse(codec.isPhase1Supported)
        XCTAssertTrue(codec.requiresExternalLibrary)
    }

    // MARK: - All Cases

    func testAllCases() {
        let allCases = Compression.allCases
        XCTAssertEqual(allCases.count, 8)
        XCTAssertTrue(allCases.contains(.uncompressed))
        XCTAssertTrue(allCases.contains(.gzip))
        XCTAssertTrue(allCases.contains(.snappy))
        XCTAssertTrue(allCases.contains(.lz4))
        XCTAssertTrue(allCases.contains(.zstd))
    }

    // MARK: - Relative Speed

    func testRelativeSpeedOrdering() {
        // Faster codecs should have lower speed values
        XCTAssertLessThan(Compression.snappy.relativeSpeed, Compression.gzip.relativeSpeed)
        XCTAssertLessThan(Compression.gzip.relativeSpeed, Compression.brotli.relativeSpeed)
    }

    // MARK: - Compression Ratio

    func testCompressionRatioOrdering() {
        // Better compression should have lower ratios
        let snappyMax = Compression.snappy.typicalRatio.upperBound
        let brotliMin = Compression.brotli.typicalRatio.lowerBound
        XCTAssertGreaterThan(snappyMax, brotliMin, "Snappy compresses less than Brotli")
    }

    // MARK: - Raw Value Init

    func testRawValueInit() {
        XCTAssertEqual(Compression(rawValue: "UNCOMPRESSED"), .uncompressed)
        XCTAssertEqual(Compression(rawValue: "GZIP"), .gzip)
        XCTAssertEqual(Compression(rawValue: "SNAPPY"), .snappy)
        XCTAssertNil(Compression(rawValue: "INVALID"))
    }

    // MARK: - Description

    func testDescription() {
        XCTAssertEqual(String(describing: Compression.uncompressed), "UNCOMPRESSED")
        XCTAssertEqual(String(describing: Compression.gzip), "GZIP")
        XCTAssertEqual(String(describing: Compression.snappy), "SNAPPY")
    }
}
