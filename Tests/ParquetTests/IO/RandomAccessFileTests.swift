// Tests for RandomAccessFile implementations
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class RandomAccessFileTests: XCTestCase {
    // MARK: - MemoryRandomAccessFile Tests

    func testMemoryFileSize() throws {
        let data = Data([1, 2, 3, 4, 5])
        let file = MemoryRandomAccessFile(data: data)

        XCTAssertEqual(try file.size, 5)
    }

    func testMemoryFileRead() throws {
        let data = Data([1, 2, 3, 4, 5])
        let file = MemoryRandomAccessFile(data: data)

        let result = try file.read(at: 0, count: 3)
        XCTAssertEqual(result, Data([1, 2, 3]))
    }

    func testMemoryFileReadAtOffset() throws {
        let data = Data([1, 2, 3, 4, 5])
        let file = MemoryRandomAccessFile(data: data)

        let result = try file.read(at: 2, count: 3)
        XCTAssertEqual(result, Data([3, 4, 5]))
    }

    func testMemoryFileReadZeroBytes() throws {
        let data = Data([1, 2, 3, 4, 5])
        let file = MemoryRandomAccessFile(data: data)

        let result = try file.read(at: 0, count: 0)
        XCTAssertEqual(result, Data())
    }

    func testMemoryFileReadToEnd() throws {
        let data = Data([1, 2, 3, 4, 5])
        let file = MemoryRandomAccessFile(data: data)

        let result = try file.readToEnd(from: 2)
        XCTAssertEqual(result, Data([3, 4, 5]))
    }

    func testMemoryFileInvalidOffset() {
        let data = Data([1, 2, 3, 4, 5])
        let file = MemoryRandomAccessFile(data: data)

        XCTAssertThrowsError(try file.read(at: 10, count: 1)) { error in
            guard case IOError.invalidOffset = error else {
                XCTFail("Expected invalidOffset error")
                return
            }
        }
    }

    func testMemoryFileInvalidRange() {
        let data = Data([1, 2, 3, 4, 5])
        let file = MemoryRandomAccessFile(data: data)

        XCTAssertThrowsError(try file.read(at: 3, count: 5)) { error in
            guard case IOError.invalidOffset = error else {
                XCTFail("Expected invalidOffset error")
                return
            }
        }
    }

    func testMemoryFileNegativeOffset() {
        let data = Data([1, 2, 3, 4, 5])
        let file = MemoryRandomAccessFile(data: data)

        XCTAssertThrowsError(try file.read(at: -1, count: 1)) { error in
            guard case IOError.invalidOffset = error else {
                XCTFail("Expected invalidOffset error")
                return
            }
        }
    }

    func testMemoryFileClose() throws {
        let data = Data([1, 2, 3, 4, 5])
        let file = MemoryRandomAccessFile(data: data)

        // Close should not throw for memory file
        try file.close()
    }

    // MARK: - FileRandomAccessFile Tests

    func testFileAccessWithRealFile() throws {
        let fixturesURL = URL(fileURLWithPath: #file)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Fixtures")
        let fileURL = fixturesURL.appendingPathComponent("datapage_v1-snappy-compressed-checksum.parquet")

        let file = try FileRandomAccessFile(url: fileURL)

        // Check file size
        let size = try file.size
        XCTAssertGreaterThan(size, 0)

        // Read header (should be "PAR1")
        let header = try file.read(at: 0, count: 4)
        XCTAssertEqual(header, Data([0x50, 0x41, 0x52, 0x31])) // "PAR1"

        // Read footer magic (should also be "PAR1")
        let footer = try file.read(at: size - 4, count: 4)
        XCTAssertEqual(footer, Data([0x50, 0x41, 0x52, 0x31]))

        try file.close()
    }

    func testFileAccessReadToEnd() throws {
        let fixturesURL = URL(fileURLWithPath: #file)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Fixtures")
        let fileURL = fixturesURL.appendingPathComponent("datapage_v1-snappy-compressed-checksum.parquet")

        let file = try FileRandomAccessFile(url: fileURL)
        let size = try file.size

        // Read last 8 bytes (footer length + magic)
        let trailer = try file.readToEnd(from: size - 8)
        XCTAssertEqual(trailer.count, 8)

        // Check magic in trailer
        let magic = trailer.suffix(4)
        XCTAssertEqual(magic, Data([0x50, 0x41, 0x52, 0x31]))

        try file.close()
    }

    func testFileAccessNonExistentFile() {
        let nonExistentURL = URL(fileURLWithPath: "/tmp/nonexistent_\(UUID().uuidString).parquet")

        XCTAssertThrowsError(try FileRandomAccessFile(url: nonExistentURL)) { error in
            guard case IOError.fileNotFound = error else {
                XCTFail("Expected fileNotFound error, got \(error)")
                return
            }
        }
    }

    func testFileAccessInvalidRange() throws {
        let fixturesURL = URL(fileURLWithPath: #file)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Fixtures")
        let fileURL = fixturesURL.appendingPathComponent("datapage_v1-snappy-compressed-checksum.parquet")

        let file = try FileRandomAccessFile(url: fileURL)
        let size = try file.size

        XCTAssertThrowsError(try file.read(at: size, count: 1)) { error in
            guard case IOError.invalidOffset = error else {
                XCTFail("Expected invalidOffset error")
                return
            }
        }

        try file.close()
    }
}
