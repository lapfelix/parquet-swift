// Fixture Analysis Tests - Analyze test fixtures for nullable columns
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class FixtureAnalysisTests: XCTestCase {
    var fixturesURL: URL {
        let sourceFile = URL(fileURLWithPath: #file)
        let testsDir = sourceFile.deletingLastPathComponent().deletingLastPathComponent()
        return testsDir.appendingPathComponent("Fixtures")
    }

    struct ColumnInfo {
        let name: String
        let path: String
        let physicalType: String
        let logicalType: String?
        let repetitionType: String
        let nullable: Bool
        let maxDefinitionLevel: Int
        let maxRepetitionLevel: Int
    }

    struct FileAnalysis {
        let filename: String
        let createdBy: String?
        let version: Int
        let numRows: Int64
        let numRowGroups: Int
        let columns: [ColumnInfo]
        let compressionCodecs: Set<String>
        let encodings: Set<String>
    }

    func analyzeFile(filename: String) throws -> FileAnalysis {
        let url = fixturesURL.appendingPathComponent(filename)
        let metadata = try ParquetFileReader.readMetadata(from: url)
        let schema = metadata.schema

        // Collect column information
        var columnInfos: [ColumnInfo] = []
        for column in schema.columns {
            let info = ColumnInfo(
                name: column.name,
                path: column.path.joined(separator: "."),
                physicalType: column.physicalType.name,
                logicalType: column.logicalType?.name,
                repetitionType: column.repetitionType.rawValue,
                nullable: column.isOptional,
                maxDefinitionLevel: column.maxDefinitionLevel,
                maxRepetitionLevel: column.maxRepetitionLevel
            )
            columnInfos.append(info)
        }

        // Collect compression codecs and encodings
        var compressionCodecs = Set<String>()
        var encodings = Set<String>()

        for rowGroup in metadata.rowGroups {
            for columnChunk in rowGroup.columns {
                if let meta = columnChunk.metadata {
                    compressionCodecs.insert(meta.codec.description)
                    for encoding in meta.encodings {
                        encodings.insert(encoding.description)
                    }
                }
            }
        }

        return FileAnalysis(
            filename: filename,
            createdBy: metadata.createdBy,
            version: metadata.version,
            numRows: metadata.numRows,
            numRowGroups: metadata.numRowGroups,
            columns: columnInfos,
            compressionCodecs: compressionCodecs,
            encodings: encodings
        )
    }

    func printAnalysis(_ analysis: FileAnalysis) {
        print("\n" + String(repeating: "=", count: 80))
        print("FILE: \(analysis.filename)")
        print(String(repeating: "=", count: 80))
        print("Created By: \(analysis.createdBy ?? "unknown")")
        print("Version: \(analysis.version)")
        print("Rows: \(analysis.numRows)")
        print("Row Groups: \(analysis.numRowGroups)")
        print("Compression: \(analysis.compressionCodecs.sorted().joined(separator: ", "))")
        print("Encodings: \(analysis.encodings.sorted().joined(separator: ", "))")

        print("\nCOLUMNS:")
        for (index, col) in analysis.columns.enumerated() {
            let nullableStr = col.nullable ? "NULLABLE" : "REQUIRED"
            let logicalStr = col.logicalType.map { " (\($0))" } ?? ""
            print("  [\(index)] \(col.path)")
            print("      Type: \(col.physicalType)\(logicalStr)")
            print("      Repetition: \(col.repetitionType) - \(nullableStr)")
            print("      Max Def Level: \(col.maxDefinitionLevel)")
            print("      Max Rep Level: \(col.maxRepetitionLevel)")
        }

        let nullableColumns = analysis.columns.filter { $0.nullable }
        let requiredColumns = analysis.columns.filter { !$0.nullable }
        print("\nSUMMARY:")
        print("  Total columns: \(analysis.columns.count)")
        print("  Required: \(requiredColumns.count)")
        print("  Nullable: \(nullableColumns.count)")
        print("  Suitable for nullable testing: \(nullableColumns.isEmpty ? "NO" : "YES")")
    }

    func generatorType(_ createdBy: String?) -> String {
        guard let createdBy = createdBy else { return "unknown" }

        if createdBy.contains("parquet-mr") {
            return "parquet-mr (Java)"
        } else if createdBy.contains("parquet-cpp") || createdBy.contains("Arrow") {
            return "PyArrow/C++"
        } else {
            return "other"
        }
    }

    func testAnalyzeAllFixtures() throws {
        let files = [
            "datapage_v1-snappy-compressed-checksum.parquet",
            "nation.plain.parquet",
            "plain_types.parquet",
            "alltypes_plain.parquet"
        ]

        var analyses: [FileAnalysis] = []

        print("\n" + String(repeating: "=", count: 80))
        print("PARQUET FIXTURE ANALYSIS")
        print(String(repeating: "=", count: 80))

        for filename in files {
            do {
                let analysis = try analyzeFile(filename: filename)
                printAnalysis(analysis)
                analyses.append(analysis)
            } catch {
                print("\nERROR analyzing \(filename): \(error)")
            }
        }

        // Print summary table
        print("\n" + String(repeating: "=", count: 80))
        print("SUMMARY TABLE")
        print(String(repeating: "=", count: 80))

        for analysis in analyses {
            let nullableColumns = analysis.columns.filter { $0.nullable }
            let requiredColumns = analysis.columns.filter { !$0.nullable }

            print("\nFile: \(analysis.filename)")
            print("  Generator: \(generatorType(analysis.createdBy))")
            print("  Created By: \(analysis.createdBy ?? "unknown")")
            print("  Compression: \(analysis.compressionCodecs.sorted().joined(separator: ", "))")
            print("  Encodings: \(analysis.encodings.sorted().joined(separator: ", "))")
            print("  Rows: \(analysis.numRows)")
            print("  Columns: \(analysis.columns.count) (\(requiredColumns.count) required, \(nullableColumns.count) nullable)")

            if !nullableColumns.isEmpty {
                print("  Nullable columns:")
                for col in nullableColumns {
                    let logicalStr = col.logicalType.map { " (\($0))" } ?? ""
                    print("    - \(col.path): \(col.physicalType)\(logicalStr) [maxDef=\(col.maxDefinitionLevel)]")
                }
            }
        }

        // Print recommendations
        print("\n" + String(repeating: "=", count: 80))
        print("RECOMMENDATIONS")
        print(String(repeating: "=", count: 80))

        let withNullable = analyses.filter { !$0.columns.filter { $0.nullable }.isEmpty }
        let withoutNullable = analyses.filter { $0.columns.filter { $0.nullable }.isEmpty }

        if withNullable.isEmpty {
            print("\nNone of the test fixtures have nullable columns!")
            print("Need to create or find test files with nullable columns for testing.")
        } else {
            print("\nFiles with nullable columns (suitable for testing):")
            for analysis in withNullable {
                let nullCount = analysis.columns.filter { $0.nullable }.count
                let plainCols = analysis.columns.filter { col in
                    col.nullable && analysis.encodings.contains("PLAIN")
                }
                print("  - \(analysis.filename)")
                print("      \(nullCount) nullable columns")
                print("      Generator: \(generatorType(analysis.createdBy))")
                print("      Compression: \(analysis.compressionCodecs.sorted().joined(separator: ", "))")
            }
        }

        if !withoutNullable.isEmpty {
            print("\nFiles WITHOUT nullable columns:")
            for analysis in withoutNullable {
                print("  - \(analysis.filename)")
            }
        }

        print("\n" + String(repeating: "=", count: 80))
    }
}
