#!/usr/bin/env swift

import Foundation

// Quick script to verify the plain_types.parquet fixture
// Run with: swift verify_plain_fixture.swift

let currentDir = FileManager.default.currentDirectoryPath
let fixturePath = "\(currentDir)/Tests/ParquetTests/Fixtures/plain_types.parquet"

print("Checking fixture: \(fixturePath)")
print("File exists: \(FileManager.default.fileExists(atPath: fixturePath))")

if let attr = try? FileManager.default.attributesOfItem(atPath: fixturePath) {
    if let size = attr[.size] as? Int {
        print("File size: \(size) bytes")
    }
}
