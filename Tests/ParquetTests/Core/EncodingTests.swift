// Tests for Encoding
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class EncodingTests: XCTestCase {
    // MARK: - Phase 1 Encodings

    func testPlainEncoding() {
        let encoding = Encoding.plain
        XCTAssertTrue(encoding.isPhase1Supported)
        XCTAssertFalse(encoding.isDictionary)
        XCTAssertFalse(encoding.isDelta)
        XCTAssertFalse(encoding.isDeprecated)
        XCTAssertEqual(encoding.rawValue, "PLAIN")
    }

    func testRLEDictionaryEncoding() {
        let encoding = Encoding.rleDictionary
        XCTAssertTrue(encoding.isPhase1Supported)
        XCTAssertTrue(encoding.isDictionary)
        XCTAssertFalse(encoding.isDelta)
        XCTAssertFalse(encoding.isDeprecated)
        XCTAssertEqual(encoding.rawValue, "RLE_DICTIONARY")
    }

    func testRLEEncoding() {
        let encoding = Encoding.rle
        XCTAssertTrue(encoding.isPhase1Supported)
        XCTAssertFalse(encoding.isDictionary)
        XCTAssertFalse(encoding.isDelta)
        XCTAssertEqual(encoding.rawValue, "RLE")
    }

    // MARK: - Delta Encodings

    func testDeltaBinaryPackedEncoding() {
        let encoding = Encoding.deltaBinaryPacked
        XCTAssertFalse(encoding.isPhase1Supported)
        XCTAssertFalse(encoding.isDictionary)
        XCTAssertTrue(encoding.isDelta)
        XCTAssertFalse(encoding.isDeprecated)
    }

    func testDeltaLengthByteArrayEncoding() {
        let encoding = Encoding.deltaLengthByteArray
        XCTAssertFalse(encoding.isPhase1Supported)
        XCTAssertTrue(encoding.isDelta)
    }

    func testDeltaByteArrayEncoding() {
        let encoding = Encoding.deltaByteArray
        XCTAssertFalse(encoding.isPhase1Supported)
        XCTAssertTrue(encoding.isDelta)
    }

    // MARK: - Advanced Encodings

    func testByteStreamSplitEncoding() {
        let encoding = Encoding.byteStreamSplit
        XCTAssertFalse(encoding.isPhase1Supported)
        XCTAssertFalse(encoding.isDictionary)
        XCTAssertFalse(encoding.isDelta)
    }

    // MARK: - Deprecated Encodings

    func testPlainDictionaryEncoding() {
        let encoding = Encoding.plainDictionary
        XCTAssertFalse(encoding.isPhase1Supported)
        XCTAssertTrue(encoding.isDictionary)
        XCTAssertTrue(encoding.isDeprecated)
    }

    func testBitPackedEncoding() {
        let encoding = Encoding.bitPacked
        XCTAssertFalse(encoding.isPhase1Supported)
        XCTAssertTrue(encoding.isDeprecated)
    }

    // MARK: - Raw Value Init

    func testRawValueInit() {
        XCTAssertEqual(Encoding(rawValue: "PLAIN"), .plain)
        XCTAssertEqual(Encoding(rawValue: "RLE_DICTIONARY"), .rleDictionary)
        XCTAssertEqual(Encoding(rawValue: "DELTA_BINARY_PACKED"), .deltaBinaryPacked)
        XCTAssertNil(Encoding(rawValue: "INVALID"))
    }

    // MARK: - Description

    func testDescription() {
        XCTAssertEqual(String(describing: Encoding.plain), "PLAIN")
        XCTAssertEqual(String(describing: Encoding.rleDictionary), "RLE_DICTIONARY")
        XCTAssertEqual(String(describing: Encoding.deltaBinaryPacked), "DELTA_BINARY_PACKED")
    }
}
