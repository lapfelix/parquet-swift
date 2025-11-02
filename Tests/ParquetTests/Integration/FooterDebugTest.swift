// Debug test for footer reading
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class FooterDebugTest: XCTestCase {
    func testFooterReading() throws {
        let fixturesURL = URL(fileURLWithPath: #file)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Fixtures")
        let fileURL = fixturesURL.appendingPathComponent("datapage_v1-snappy-compressed-checksum.parquet")

        let data = try Data(contentsOf: fileURL)

        print("File size: \(data.count) bytes")

        // Check header
        let header = data.prefix(4)
        print("Header: \(header.map { String(format: "%02X", $0) }.joined())")

        // Check trailer
        let trailer = data.suffix(8)
        print("Trailer (last 8 bytes): \(trailer.map { String(format: "%02X", $0) }.joined())")

        // Read footer length
        let footerLengthOffset = data.count - 8
        let footerLengthBytes = data.subdata(in: footerLengthOffset..<(footerLengthOffset + 4))
        let footerLength = footerLengthBytes.withUnsafeBytes { $0.load(as: UInt32.self).littleEndian }

        print("Footer length: \(footerLength) bytes")

        // Calculate metadata offset
        let metadataOffset = data.count - 8 - Int(footerLength)
        print("Metadata offset: \(metadataOffset)")

        // Extract first few bytes of metadata
        let metadataBytes = data.subdata(in: metadataOffset..<footerLengthOffset)
        let prefix = metadataBytes.prefix(20)
        print("Metadata prefix: \(prefix.map { String(format: "%02X", $0) }.joined(separator: " "))")

        // Try to read full metadata
        let reader = ThriftReader(data: metadataBytes)
        do {
            let metadata = try reader.readFileMetaData()
            print("\nMetadata read successfully!")
            print("  Version: \(metadata.version)")
            print("  Rows: \(metadata.numRows)")
            print("  Schema elements: \(metadata.schema.count)")
            print("  Row groups: \(metadata.rowGroups.count)")

            // Print first few schema elements
            for (i, elem) in metadata.schema.prefix(3).enumerated() {
                print("\n  Schema[\(i)]:")
                print("    Name: \(elem.name)")
                print("    Type: \(elem.type?.rawValue ?? -1)")
                print("    Num children: \(elem.numChildren ?? 0)")
            }
        } catch {
            print("\nError reading metadata: \(error)")
        }
    }
}
