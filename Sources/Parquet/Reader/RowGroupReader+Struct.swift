// RowGroupReader+Struct - Struct reading support
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Extension for reading struct columns
extension RowGroupReader {
    /// Read all rows of a struct column
    ///
    /// - Parameter path: Path to the struct (e.g., ["user"])
    /// - Returns: Array where:
    ///   - `nil` = NULL struct (struct instance not present)
    ///   - `StructValue` = struct present (may have null fields)
    ///
    /// # Example
    ///
    /// ```swift
    /// let users = try rowGroup.readStruct(at: ["user"])
    /// for (i, user) in users.enumerated() {
    ///     if let user = user {
    ///         let name = user.get("name", as: String.self)
    ///         let age = user.get("age", as: Int32.self)
    ///         print("Row \(i): \(name ?? "NULL"), age \(age ?? 0)")
    ///     } else {
    ///         print("Row \(i): NULL struct")
    ///     }
    /// }
    /// ```
    ///
    /// # NULL Semantics
    ///
    /// For schema: `optional group user { optional string name; optional int32 age; }`
    ///
    /// - `defLevel(all fields) = 0` → struct is NULL → return `nil`
    /// - `defLevel(field) = 1` → struct present, field NULL → `StructValue` with `field = nil`
    /// - `defLevel(field) = maxDef` → field has value → `StructValue` with `field = value`
    ///
    /// - Throws: `RowGroupReaderError` if path doesn't point to a struct
    public func readStruct(at path: [String]) throws -> [StructValue?] {
        // Validate path points to a struct
        guard let element = schema.element(at: path), element.isStruct else {
            throw RowGroupReaderError.unsupportedType(
                "Path \(path.joined(separator: ".")) does not point to a struct"
            )
        }

        // Get all field columns for this struct
        guard let fieldColumns = schema.structFields(at: path), !fieldColumns.isEmpty else {
            throw RowGroupReaderError.unsupportedType(
                "Struct at \(path.joined(separator: ".")) has no fields"
            )
        }

        // Read all field columns with their data and definition levels
        let fieldReaders = try readFieldColumns(fieldColumns)

        // Determine number of rows from first field
        let numRows = fieldReaders[0].definitionLevels.count

        // Reconstruct struct values row by row
        var result: [StructValue?] = []
        result.reserveCapacity(numRows)

        for rowIndex in 0..<numRows {
            let structValue = try reconstructStructValue(
                at: rowIndex,
                element: element,
                fieldReaders: fieldReaders
            )
            result.append(structValue)
        }

        return result
    }

    /// Read all field columns and their data
    private func readFieldColumns(_ columns: [Column]) throws -> [FieldReader] {
        var readers: [FieldReader] = []

        for column in columns {
            let reader = try readFieldColumn(column)
            readers.append(reader)
        }

        return readers
    }

    /// Read a single field column
    private func readFieldColumn(_ column: Column) throws -> FieldReader {
        let columnIndex = column.index

        // Read based on physical type
        switch column.physicalType {
        case .int32:
            let reader = try int32Column(at: columnIndex)
            let (values, defLevels) = try reader.readAllWithLevels()
            return FieldReader(
                name: column.name,
                values: values.map { $0 as Any? },
                definitionLevels: defLevels,
                maxDefinitionLevel: column.maxDefinitionLevel
            )

        case .int64:
            let reader = try int64Column(at: columnIndex)
            let (values, defLevels) = try reader.readAllWithLevels()
            return FieldReader(
                name: column.name,
                values: values.map { $0 as Any? },
                definitionLevels: defLevels,
                maxDefinitionLevel: column.maxDefinitionLevel
            )

        case .float:
            let reader = try floatColumn(at: columnIndex)
            let (values, defLevels) = try reader.readAllWithLevels()
            return FieldReader(
                name: column.name,
                values: values.map { $0 as Any? },
                definitionLevels: defLevels,
                maxDefinitionLevel: column.maxDefinitionLevel
            )

        case .double:
            let reader = try doubleColumn(at: columnIndex)
            let (values, defLevels) = try reader.readAllWithLevels()
            return FieldReader(
                name: column.name,
                values: values.map { $0 as Any? },
                definitionLevels: defLevels,
                maxDefinitionLevel: column.maxDefinitionLevel
            )

        case .byteArray:
            let reader = try stringColumn(at: columnIndex)
            let (values, defLevels) = try reader.readAllWithLevels()
            return FieldReader(
                name: column.name,
                values: values.map { $0 as Any? },
                definitionLevels: defLevels,
                maxDefinitionLevel: column.maxDefinitionLevel
            )

        default:
            throw RowGroupReaderError.unsupportedType(
                "Field \(column.name) has unsupported type \(column.physicalType.name)"
            )
        }
    }

    /// Reconstruct a single struct value at a given row
    private func reconstructStructValue(
        at rowIndex: Int,
        element: SchemaElement,
        fieldReaders: [FieldReader]
    ) throws -> StructValue? {
        // Check if struct is NULL: ALL fields must have defLevel = 0
        let allFieldsNull = fieldReaders.allSatisfy { reader in
            reader.definitionLevels[rowIndex] == 0
        }

        if allFieldsNull {
            // Struct is NULL
            return nil
        }

        // Struct is present - build field data
        var fieldData: [String: Any?] = [:]

        for reader in fieldReaders {
            // The values array from readAllWithLevels() already has nil for NULLs
            // So we can directly index by row
            fieldData[reader.name] = reader.values[rowIndex]
        }

        return StructValue(element: element, fieldData: fieldData)
    }
}

// MARK: - Helper Types

/// Holds column data for struct reconstruction
private struct FieldReader {
    let name: String
    let values: [Any?]  // Values with nil for NULLs (indexed by row)
    let definitionLevels: [UInt16]
    let maxDefinitionLevel: Int
}
