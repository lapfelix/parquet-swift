// RowGroupReader+Struct - Struct reading support
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Extension for reading struct columns
extension RowGroupReader {
    /// Check if a schema element or any of its descendants contains repeated/map/list nodes
    ///
    /// This recursively traverses the schema tree to detect:
    /// - Repeated fields (at any depth)
    /// - Maps (at any depth)
    /// - Lists (at any depth)
    ///
    /// Used to determine if a struct/map can be safely read without multi-level reconstruction.
    /// Accessible across RowGroupReader extensions (Struct, Map).
    internal func hasRepeatedOrComplexDescendants(_ element: SchemaElement) -> Bool {
        // Check immediate node
        if element.repetitionType == .repeated {
            return true
        }
        if element.isMap || element.isList {
            return true
        }

        // Recursively check all children
        for child in element.children {
            if hasRepeatedOrComplexDescendants(child) {
                return true
            }
        }

        return false
    }

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

        // Check if this struct contains any repeated or map/list fields (at any depth)
        // We recursively check all descendants to catch cases like:
        // - struct { repeated int32 }
        // - struct { map }
        // - struct { list }
        // - struct { struct { map } }  ← nested case requiring recursion
        //
        // Nested structs with ONLY scalar fields are allowed and can be read.
        let hasUnsupportedChildren = element.children.contains { child in
            hasRepeatedOrComplexDescendants(child)
        }

        if hasUnsupportedChildren {
            throw RowGroupReaderError.unsupportedType(
                "Structs containing repeated or map/list fields are not yet supported.\n" +
                "\n" +
                "This struct has fields that are:\n" +
                "- Repeated (e.g., repeated int32 tags)\n" +
                "- Maps (map<K,V>)\n" +
                "- Lists (list<T>)\n" +
                "\n" +
                "Note: Nested structs with only scalar fields ARE supported.\n" +
                "\n" +
                "Workarounds:\n" +
                "1. For repeated fields: Use the primitive column reader\n" +
                "   - Access schema: let schema = reader.metadata.schema\n" +
                "   - Find column: let col = schema.columns.first(where: { $0.path == [\"struct\", \"field\"] })!\n" +
                "   - Read via: rowGroup.int32Column(at: col.index).readAllRepeated()\n" +
                "2. For maps: Use readMap(at: path)\n" +
                "3. For lists: Use appropriate list reading method\n" +
                "\n" +
                "This limitation will be removed once proper multi-level reconstruction is implemented.\n" +
                "See docs/limitations.md for details."
            )
        }

        // For simple structs (primitives only), iterate through definition levels
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


    /// Read all rows of a repeated struct column (list of structs)
    ///
    /// - Parameter path: Path to the repeated struct (e.g., ["items", "list", "element"])
    /// - Returns: Array of arrays where:
    ///   - Outer nil = NULL list (list not present)
    ///   - Inner nil = NULL struct (struct not present)
    ///   - Empty array [] = empty list (list present, zero elements)
    ///   - StructValue = present struct (may have null fields)
    ///
    /// # Example
    ///
    /// ```swift
    /// let listOfStructs = try rowGroup.readRepeatedStruct(at: ["my_map", "key_value"])
    /// for (i, list) in listOfStructs.enumerated() {
    ///     if let list = list {
    ///         print("Row \(i): \(list.count) structs")
    ///         for struct in list {
    ///             if let s = struct {
    ///                 // Access struct fields
    ///             }
    ///         }
    ///     }
    /// }
    /// ```
    ///
    /// - Throws: `RowGroupReaderError` if path doesn't point to a repeated struct
    public func readRepeatedStruct(at path: [String]) throws -> [[StructValue?]?] {
        // Validate path points to a struct
        guard let element = schema.element(at: path), element.isStruct else {
            throw RowGroupReaderError.unsupportedType(
                "Path \(path.joined(separator: ".")) does not point to a struct"
            )
        }

        // Check if this struct contains any repeated or map/list fields (at any depth)
        // We recursively check all descendants to catch cases like:
        // - struct { repeated int32 }
        // - struct { map }
        // - struct { list }
        // - struct { struct { map } }  ← nested case requiring recursion
        //
        // Nested structs with ONLY scalar fields are allowed and can be read.
        let hasUnsupportedChildren = element.children.contains { child in
            hasRepeatedOrComplexDescendants(child)
        }

        if hasUnsupportedChildren {
            throw RowGroupReaderError.unsupportedType(
                "Structs in lists containing repeated or map/list fields are not yet supported.\n" +
                "\n" +
                "The struct in this list has fields that are:\n" +
                "- Repeated (e.g., repeated int32 tags)\n" +
                "- Maps (map<K,V>)\n" +
                "- Lists (list<T>)\n" +
                "\n" +
                "Note: Nested structs with only scalar fields ARE supported.\n" +
                "\n" +
                "Workarounds:\n" +
                "1. For repeated fields: Use the primitive column reader\n" +
                "   - Access schema: let schema = reader.metadata.schema\n" +
                "   - Find column: let col = schema.columns.first(where: { $0.path == [..., \"field\"] })!\n" +
                "   - Read via: rowGroup.int32Column(at: col.index).readAllRepeated()\n" +
                "2. For maps: Use readMap(at: path)\n" +
                "3. For lists: Use appropriate list reading method\n" +
                "\n" +
                "This limitation will be removed once proper multi-level reconstruction is implemented.\n" +
                "See docs/limitations.md for details."
            )
        }

        // Get all field columns for this struct
        guard let fieldColumns = schema.structFields(at: path), !fieldColumns.isEmpty else {
            throw RowGroupReaderError.unsupportedType(
                "Struct at \(path.joined(separator: ".")) has no fields"
            )
        }

        // Verify this is a repeated struct
        let firstColumn = fieldColumns[0]
        guard firstColumn.maxRepetitionLevel > 0 else {
            throw RowGroupReaderError.unsupportedType(
                "Struct at \(path.joined(separator: ".")) is not repeated. Use readStruct() instead."
            )
        }

        guard let repeatedAncestorDefLevel = firstColumn.repeatedAncestorDefLevel else {
            throw RowGroupReaderError.internalError(
                "Cannot compute repeatedAncestorDefLevel for repeated struct"
            )
        }

        // Read all field columns with their data and levels
        let fieldReaders = try readFieldColumnsWithLevels(fieldColumns)

        // Use first field's levels (all fields in a struct share the same levels)
        let defLevels = fieldReaders[0].definitionLevels
        let repLevels = fieldReaders[0].repetitionLevels
        let maxRepLevel = firstColumn.maxRepetitionLevel

        // Reconstruct structs grouped by repetition levels
        return try reconstructRepeatedStructs(
            element: element,
            fieldReaders: fieldReaders,
            definitionLevels: defLevels,
            repetitionLevels: repLevels,
            maxRepetitionLevel: maxRepLevel,
            repeatedAncestorDefLevel: repeatedAncestorDefLevel
        )
    }

    /// Read field columns with definition and repetition levels
    private func readFieldColumnsWithLevels(_ columns: [Column]) throws -> [FieldReaderWithLevels] {
        var readers: [FieldReaderWithLevels] = []

        for column in columns {
            let reader = try readFieldColumnWithLevels(column)
            readers.append(reader)
        }

        return readers
    }

    /// Read a single field column with levels
    private func readFieldColumnWithLevels(_ column: Column) throws -> FieldReaderWithLevels {
        let columnIndex = column.index

        // Read based on physical type
        switch column.physicalType {
        case .int32:
            let reader = try int32Column(at: columnIndex)
            let (values, defLevels, repLevels) = try reader.readAllWithAllLevels()
            return FieldReaderWithLevels(
                name: column.name,
                values: values.map { $0 as Any? },
                definitionLevels: defLevels,
                repetitionLevels: repLevels,
                maxDefinitionLevel: column.maxDefinitionLevel
            )

        case .int64:
            let reader = try int64Column(at: columnIndex)
            let (values, defLevels, repLevels) = try reader.readAllWithAllLevels()
            return FieldReaderWithLevels(
                name: column.name,
                values: values.map { $0 as Any? },
                definitionLevels: defLevels,
                repetitionLevels: repLevels,
                maxDefinitionLevel: column.maxDefinitionLevel
            )

        case .float:
            let reader = try floatColumn(at: columnIndex)
            let (values, defLevels, repLevels) = try reader.readAllWithAllLevels()
            return FieldReaderWithLevels(
                name: column.name,
                values: values.map { $0 as Any? },
                definitionLevels: defLevels,
                repetitionLevels: repLevels,
                maxDefinitionLevel: column.maxDefinitionLevel
            )

        case .double:
            let reader = try doubleColumn(at: columnIndex)
            let (values, defLevels, repLevels) = try reader.readAllWithAllLevels()
            return FieldReaderWithLevels(
                name: column.name,
                values: values.map { $0 as Any? },
                definitionLevels: defLevels,
                repetitionLevels: repLevels,
                maxDefinitionLevel: column.maxDefinitionLevel
            )

        case .byteArray:
            let reader = try stringColumn(at: columnIndex)
            let (values, defLevels, repLevels) = try reader.readAllWithAllLevels()
            return FieldReaderWithLevels(
                name: column.name,
                values: values.map { $0 as Any? },
                definitionLevels: defLevels,
                repetitionLevels: repLevels,
                maxDefinitionLevel: column.maxDefinitionLevel
            )

        default:
            throw RowGroupReaderError.unsupportedType(
                "Field \(column.name) has unsupported type \(column.physicalType.name)"
            )
        }
    }

    /// Reconstruct repeated structs using rep/def levels (similar to ArrayReconstructor)
    ///
    /// This implementation groups structs by ROW (repLevel = 0), not by the innermost
    /// repetition level. This matches Arrow C++'s behavior and correctly handles
    /// nested repetitions like list<map> or list<struct>.
    ///
    /// For multi-level repetitions:
    /// - repLevel = 0: new row (start new result element)
    /// - repLevel > 0: same row (add to current list, possibly new inner element)
    ///
    /// The innermost repetition level (maxRepetitionLevel) is used only for
    /// detecting empty vs NULL lists via definition levels.
    private func reconstructRepeatedStructs(
        element: SchemaElement,
        fieldReaders: [FieldReaderWithLevels],
        definitionLevels: [UInt16],
        repetitionLevels: [UInt16],
        maxRepetitionLevel: Int,
        repeatedAncestorDefLevel: Int
    ) throws -> [[StructValue?]?] {
        var result: [[StructValue?]?] = []
        var currentList: [StructValue?] = []
        var needsAppend = false

        for i in 0..<definitionLevels.count {
            let defLevel = Int(definitionLevels[i])
            let repLevel = Int(repetitionLevels[i])

            // NEW LOGIC: Group by ROWS (repLevel = 0), not by innermost repetition
            // This fixes the bug where list<map> was split into multiple rows
            if repLevel == 0 {
                // Start of new ROW
                if needsAppend {
                    result.append(currentList)
                }

                currentList = []

                // Check if this is a NULL list
                if defLevel < repeatedAncestorDefLevel {
                    result.append(nil)
                    needsAppend = false
                    continue
                }

                // Check if this is an EMPTY list (list present but no elements)
                if defLevel == repeatedAncestorDefLevel {
                    // Empty list - don't add any elements
                    needsAppend = true
                    continue
                }

                // Add first struct based on definition level
                let structValue = try reconstructStructValueAt(
                    i,
                    element: element,
                    fieldReaders: fieldReaders,
                    repeatedAncestorDefLevel: repeatedAncestorDefLevel
                )
                currentList.append(structValue)
                needsAppend = true
            } else {
                // Continuation of current ROW (repLevel > 0)
                // This could be a new inner element (e.g., new map in list<map>)
                // or continuation of current element (e.g., map entry)
                let structValue = try reconstructStructValueAt(
                    i,
                    element: element,
                    fieldReaders: fieldReaders,
                    repeatedAncestorDefLevel: repeatedAncestorDefLevel
                )
                currentList.append(structValue)
            }
        }

        // Append final list
        if needsAppend {
            result.append(currentList)
        }

        return result
    }

    /// Reconstruct a struct value at a specific position in the flattened data
    private func reconstructStructValueAt(
        _ index: Int,
        element: SchemaElement,
        fieldReaders: [FieldReaderWithLevels],
        repeatedAncestorDefLevel: Int
    ) throws -> StructValue? {
        // Check if struct is NULL
        //
        // CORRECT NULL DETECTION:
        // The struct is NULL when ALL fields have definition levels at or below
        // the level where the PARENT repeated group (list) is present.
        //
        // For list<struct { optional string nickname; required int32 id }>:
        // - If nickname defLevel = 1 (NULL) but id defLevel = 2 (present)
        //   → struct IS present (at least one field indicates presence)
        // - If ALL fields have defLevel <= 1
        //   → struct is NULL (no fields present)
        //
        // Example: list<struct { optional string name }>
        //   defLevel 0: list NULL
        //   defLevel 1: list present, no elements (repeatedAncestorDefLevel = 1)
        //   defLevel 2: struct present, name NULL
        //   defLevel 3: struct present, name has value
        //
        // IMPORTANT: Must check ALL fields, not just first field!

        let allFieldsIndicateNoStruct = fieldReaders.allSatisfy { reader in
            Int(reader.definitionLevels[index]) <= repeatedAncestorDefLevel
        }

        if allFieldsIndicateNoStruct {
            return nil  // Struct is NULL
        }

        // Struct is present - build field data
        // Fields may individually be NULL (their values array already has nil for NULLs)
        //
        // ⚠️ WARNING: KNOWN LIMITATION - Repeated children are truncated here!
        // For fields that are repeated (maps/lists), reader.values[index] only contains
        // the FIRST entry. Multi-entry structures are INCOMPLETE.
        //
        // Example: If this struct has a map field with {a:1, b:2, c:3},
        // only {key:"a", value:1} will be present in the StructValue.
        //
        // TODO: Implement proper multi-level reconstruction to gather all entries
        // TODO: See issue tracking LevelInfo port for full solution
        var fieldData: [String: Any?] = [:]

        for reader in fieldReaders {
            // WARNING: For repeated fields, this only captures first entry!
            fieldData[reader.name] = reader.values[index]
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

/// Holds column data with repetition levels for repeated struct reconstruction
private struct FieldReaderWithLevels {
    let name: String
    let values: [Any?]  // Values with nil for NULLs (indexed by position)
    let definitionLevels: [UInt16]
    let repetitionLevels: [UInt16]
    let maxDefinitionLevel: Int
}
