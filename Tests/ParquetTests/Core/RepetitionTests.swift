// Tests for Repetition
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class RepetitionTests: XCTestCase {
    // MARK: - Basic Properties

    func testRequiredRepetition() {
        let repetition = Repetition.required

        XCTAssertFalse(repetition.isNullable)
        XCTAssertFalse(repetition.isList)
        XCTAssertTrue(repetition.isRequired)
        XCTAssertEqual(repetition.maxDefinitionLevel, 0)
        XCTAssertEqual(repetition.maxRepetitionLevel, 0)
        XCTAssertEqual(repetition.rawValue, "REQUIRED")
    }

    func testOptionalRepetition() {
        let repetition = Repetition.optional

        XCTAssertTrue(repetition.isNullable)
        XCTAssertFalse(repetition.isList)
        XCTAssertFalse(repetition.isRequired)
        XCTAssertEqual(repetition.maxDefinitionLevel, 1)
        XCTAssertEqual(repetition.maxRepetitionLevel, 0)
        XCTAssertEqual(repetition.rawValue, "OPTIONAL")
    }

    func testRepeatedRepetition() {
        let repetition = Repetition.repeated

        XCTAssertFalse(repetition.isNullable)
        XCTAssertTrue(repetition.isList)
        XCTAssertFalse(repetition.isRequired)
        XCTAssertEqual(repetition.maxDefinitionLevel, 1)
        XCTAssertEqual(repetition.maxRepetitionLevel, 1)
        XCTAssertEqual(repetition.rawValue, "REPEATED")
    }

    // MARK: - All Cases

    func testAllCases() {
        let allCases = Repetition.allCases
        XCTAssertEqual(allCases.count, 3)
        XCTAssertTrue(allCases.contains(.required))
        XCTAssertTrue(allCases.contains(.optional))
        XCTAssertTrue(allCases.contains(.repeated))
    }

    // MARK: - Raw Value Init

    func testRawValueInit() {
        XCTAssertEqual(Repetition(rawValue: "REQUIRED"), .required)
        XCTAssertEqual(Repetition(rawValue: "OPTIONAL"), .optional)
        XCTAssertEqual(Repetition(rawValue: "REPEATED"), .repeated)
        XCTAssertNil(Repetition(rawValue: "INVALID"))
    }

    // MARK: - Description

    func testDescription() {
        XCTAssertEqual(String(describing: Repetition.required), "REQUIRED")
        XCTAssertEqual(String(describing: Repetition.optional), "OPTIONAL")
        XCTAssertEqual(String(describing: Repetition.repeated), "REPEATED")
    }
}
