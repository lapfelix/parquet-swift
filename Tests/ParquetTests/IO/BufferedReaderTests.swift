// Tests for BufferedReader
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class BufferedReaderTests: XCTestCase {
    // MARK: - Basic Reading Tests

    func testBufferedReadSequential() throws {
        let data = Data([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        let file = MemoryRandomAccessFile(data: data)
        let reader = BufferedReader(file: file, bufferSize: 4)

        let chunk1 = try reader.read(count: 3)
        XCTAssertEqual(chunk1, Data([1, 2, 3]))

        let chunk2 = try reader.read(count: 3)
        XCTAssertEqual(chunk2, Data([4, 5, 6]))

        let chunk3 = try reader.read(count: 4)
        XCTAssertEqual(chunk3, Data([7, 8, 9, 10]))
    }

    func testBufferedReadRandomAccess() throws {
        let data = Data([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        let file = MemoryRandomAccessFile(data: data)
        let reader = BufferedReader(file: file, bufferSize: 4)

        // Read from different offsets without changing position
        let chunk1 = try reader.read(at: 5, count: 3)
        XCTAssertEqual(chunk1, Data([6, 7, 8]))
        XCTAssertEqual(reader.currentPosition, 0) // Position unchanged

        let chunk2 = try reader.read(at: 0, count: 2)
        XCTAssertEqual(chunk2, Data([1, 2]))
        XCTAssertEqual(reader.currentPosition, 0)
    }

    func testBufferedReadByte() throws {
        let data = Data([1, 2, 3, 4, 5])
        let file = MemoryRandomAccessFile(data: data)
        let reader = BufferedReader(file: file)

        XCTAssertEqual(try reader.readByte(), 1)
        XCTAssertEqual(try reader.readByte(), 2)
        XCTAssertEqual(try reader.readByte(), 3)
        XCTAssertEqual(reader.currentPosition, 3)
    }

    func testBufferedReadToEnd() throws {
        let data = Data([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        let file = MemoryRandomAccessFile(data: data)
        let reader = BufferedReader(file: file)

        // Read first 3 bytes
        _ = try reader.read(count: 3)

        // Read to end
        let remaining = try reader.readToEnd()
        XCTAssertEqual(remaining, Data([4, 5, 6, 7, 8, 9, 10]))
    }

    func testBufferedReadZeroBytes() throws {
        let data = Data([1, 2, 3, 4, 5])
        let file = MemoryRandomAccessFile(data: data)
        let reader = BufferedReader(file: file)

        let result = try reader.read(count: 0)
        XCTAssertEqual(result, Data())
        XCTAssertEqual(reader.currentPosition, 0)
    }

    // MARK: - Seeking Tests

    func testBufferedSeekAbsolute() throws {
        let data = Data([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        let file = MemoryRandomAccessFile(data: data)
        let reader = BufferedReader(file: file)

        try reader.seek(to: 5)
        XCTAssertEqual(reader.currentPosition, 5)

        let chunk = try reader.read(count: 3)
        XCTAssertEqual(chunk, Data([6, 7, 8]))
    }

    func testBufferedSeekRelative() throws {
        let data = Data([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        let file = MemoryRandomAccessFile(data: data)
        let reader = BufferedReader(file: file)

        // Read 3 bytes (position = 3)
        _ = try reader.read(count: 3)

        // Seek forward by 2
        try reader.seek(by: 2)
        XCTAssertEqual(reader.currentPosition, 5)

        let chunk = try reader.read(count: 2)
        XCTAssertEqual(chunk, Data([6, 7]))
    }

    func testBufferedSeekBackward() throws {
        let data = Data([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        let file = MemoryRandomAccessFile(data: data)
        let reader = BufferedReader(file: file)

        // Move to position 7
        try reader.seek(to: 7)

        // Seek backward by 3
        try reader.seek(by: -3)
        XCTAssertEqual(reader.currentPosition, 4)

        let chunk = try reader.read(count: 2)
        XCTAssertEqual(chunk, Data([5, 6]))
    }

    func testBufferedSeekInvalidOffset() throws {
        let data = Data([1, 2, 3, 4, 5])
        let file = MemoryRandomAccessFile(data: data)
        let reader = BufferedReader(file: file)

        XCTAssertThrowsError(try reader.seek(to: 10)) { error in
            guard case IOError.invalidOffset = error else {
                XCTFail("Expected invalidOffset error")
                return
            }
        }

        XCTAssertThrowsError(try reader.seek(to: -1)) { error in
            guard case IOError.invalidOffset = error else {
                XCTFail("Expected invalidOffset error")
                return
            }
        }
    }

    // MARK: - Buffering Tests

    func testBufferedReadWithinBuffer() throws {
        let data = Data([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        let file = MemoryRandomAccessFile(data: data)
        let reader = BufferedReader(file: file, bufferSize: 6)

        // First read fills buffer
        let chunk1 = try reader.read(count: 3)
        XCTAssertEqual(chunk1, Data([1, 2, 3]))

        // Second read should use buffer (no file access)
        let chunk2 = try reader.read(count: 2)
        XCTAssertEqual(chunk2, Data([4, 5]))
    }

    func testBufferedReadAcrossBufferBoundary() throws {
        let data = Data([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        let file = MemoryRandomAccessFile(data: data)
        let reader = BufferedReader(file: file, bufferSize: 4)

        // Read that crosses buffer boundary
        _ = try reader.read(count: 2)  // Buffer: [1,2,3,4], position=2
        let chunk = try reader.read(count: 4)  // Needs refill
        XCTAssertEqual(chunk, Data([3, 4, 5, 6]))
    }

    func testBufferedReadLargerThanBuffer() throws {
        let data = Data(repeating: 0xAA, count: 100)
        let file = MemoryRandomAccessFile(data: data)
        let reader = BufferedReader(file: file, bufferSize: 10)

        // Read larger than buffer should bypass buffering
        let chunk = try reader.read(count: 50)
        XCTAssertEqual(chunk.count, 50)
        XCTAssertTrue(chunk.allSatisfy { $0 == 0xAA })
    }

    func testBufferedRandomAccessUsesBuffer() throws {
        let data = Data([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        let file = MemoryRandomAccessFile(data: data)
        let reader = BufferedReader(file: file, bufferSize: 6)

        // Random read fills buffer
        let chunk1 = try reader.read(at: 2, count: 3)
        XCTAssertEqual(chunk1, Data([3, 4, 5]))

        // Another random read in same buffer range
        let chunk2 = try reader.read(at: 3, count: 2)
        XCTAssertEqual(chunk2, Data([4, 5]))
    }

    // MARK: - Convenience Methods Tests

    func testBufferedReadUInt32LE() throws {
        let data = Data([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08])
        let file = MemoryRandomAccessFile(data: data)
        let reader = BufferedReader(file: file)

        let value = try reader.readUInt32LE()
        XCTAssertEqual(value, 0x04030201) // Little-endian
        XCTAssertEqual(reader.currentPosition, 4)
    }

    func testBufferedReadUInt32LEAt() throws {
        let data = Data([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08])
        let file = MemoryRandomAccessFile(data: data)
        let reader = BufferedReader(file: file)

        let value = try reader.readUInt32LE(at: 4)
        XCTAssertEqual(value, 0x08070605)
        XCTAssertEqual(reader.currentPosition, 0) // Position unchanged
    }

    func testBufferedReadUInt64LE() throws {
        let data = Data([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08])
        let file = MemoryRandomAccessFile(data: data)
        let reader = BufferedReader(file: file)

        let value = try reader.readUInt64LE()
        XCTAssertEqual(value, 0x0807060504030201)
        XCTAssertEqual(reader.currentPosition, 8)
    }

    func testBufferedReadUInt64LEAt() throws {
        let data = Data([0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08])
        let file = MemoryRandomAccessFile(data: data)
        let reader = BufferedReader(file: file)

        let value = try reader.readUInt64LE(at: 2)
        XCTAssertEqual(value, 0x0807060504030201)
        XCTAssertEqual(reader.currentPosition, 0)
    }

    // MARK: - Real File Tests

    func testBufferedReaderWithRealFile() throws {
        let fixturesURL = URL(fileURLWithPath: #file)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Fixtures")
        let fileURL = fixturesURL.appendingPathComponent("datapage_v1-snappy-compressed-checksum.parquet")

        let file = try FileRandomAccessFile(url: fileURL)
        let reader = BufferedReader(file: file, bufferSize: 1024)

        // Read header
        let header = try reader.read(count: 4)
        XCTAssertEqual(header, Data([0x50, 0x41, 0x52, 0x31]))

        // Seek to end and read footer
        let fileSize = try reader.fileSize
        try reader.seek(to: fileSize - 4)
        let footer = try reader.read(count: 4)
        XCTAssertEqual(footer, Data([0x50, 0x41, 0x52, 0x31]))

        try reader.close()
    }

    func testBufferedReaderReadFooterLength() throws {
        let fixturesURL = URL(fileURLWithPath: #file)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Fixtures")
        let fileURL = fixturesURL.appendingPathComponent("datapage_v1-snappy-compressed-checksum.parquet")

        let file = try FileRandomAccessFile(url: fileURL)
        let reader = BufferedReader(file: file)

        let fileSize = try reader.fileSize

        // Read footer length (4 bytes before magic)
        let footerLength = try reader.readUInt32LE(at: fileSize - 8)
        XCTAssertGreaterThan(footerLength, 0)

        try reader.close()
    }
}
