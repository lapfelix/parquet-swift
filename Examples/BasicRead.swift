// BasicRead.swift - Example of reading a Parquet file
//
// This example demonstrates:
// - Opening a Parquet file
// - Reading metadata
// - Reading primitive columns
// - Reading nested columns (lists, maps, structs)

import Foundation
import Parquet

func basicReadExample() throws {
    // Replace with your actual file path
    let fileURL = URL(fileURLWithPath: "path/to/your/file.parquet")

    // Open the Parquet file
    let reader = try ParquetFileReader(url: fileURL)
    defer { try? reader.close() }

    // Print file metadata
    print("=== File Metadata ===")
    print("Number of rows: \(reader.metadata.numRows)")
    print("Number of columns: \(reader.metadata.schema.columnCount)")
    print("Number of row groups: \(reader.metadata.numRowGroups)")
    print()

    // Print schema information
    print("=== Schema ===")
    for (index, column) in reader.metadata.schema.columns.enumerated() {
        print("Column \(index): \(column.name)")
        print("  Type: \(column.physicalType)")
        print("  Repetition: \(column.repetitionType)")
        if let logicalType = column.logicalType {
            print("  Logical Type: \(logicalType)")
        }
    }
    print()

    // Access the first row group
    let rowGroup = try reader.rowGroup(at: 0)

    // Example: Read an Int32 column
    print("=== Reading Int32 Column ===")
    let idColumn = try rowGroup.int32Column(at: 0)
    let ids = try idColumn.readAll()
    print("First 10 IDs: \(ids.prefix(10))")
    print()

    // Example: Read a String column
    print("=== Reading String Column ===")
    let nameColumn = try rowGroup.stringColumn(at: 1)
    let names = try nameColumn.readAll()
    print("First 10 names: \(names.prefix(10))")
    print()

    // Example: Read a list column
    // Assuming column at index 2 is a list<int32>
    print("=== Reading List Column ===")
    do {
        let listData = try rowGroup.readList(at: ["list_column"])
        print("Number of lists: \(listData.count)")
        if let firstList = listData.first as? [Any?] {
            print("First list: \(firstList)")
        }
    } catch {
        print("No list column found or error: \(error)")
    }
    print()

    // Example: Read a map column
    // Assuming you have a map<string, int32> column
    print("=== Reading Map Column ===")
    do {
        let mapData = try rowGroup.readMap(at: ["map_column"])
        print("Number of maps: \(mapData.count)")
        if let firstMap = mapData.first {
            print("First map: \(firstMap ?? [:])")
        }
    } catch {
        print("No map column found or error: \(error)")
    }
    print()

    // Example: Read a struct column
    // Assuming you have a struct column
    print("=== Reading Struct Column ===")
    do {
        let structData = try rowGroup.readStruct(at: ["struct_column"])
        print("Number of structs: \(structData.count)")
        if let firstStruct = structData.first {
            print("First struct: \(firstStruct ?? [:])")
        }
    } catch {
        print("No struct column found or error: \(error)")
    }
    print()

    // Example: Batch reading for large files
    print("=== Batch Reading ===")
    let batchSize = 1000
    var totalRead = 0

    while totalRead < ids.count {
        let batch = try idColumn.readBatch(count: batchSize)
        totalRead += batch.count
        print("Read batch of \(batch.count) values, total: \(totalRead)")

        if batch.isEmpty {
            break
        }
    }
}

// Run the example
do {
    try basicReadExample()
} catch {
    print("Error: \(error)")
}
