// RowGroupReader+Struct - Struct reading support
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Extension for reading struct columns
///
/// # Phase 4.4: Struct Validity Computation for Repeated Children
///
/// Phase 4.4 implements INFRASTRUCTURE ONLY for structs with repeated children
/// (maps, lists, or repeated fields). This is NOT full support.
///
/// **What works:**
/// - Detection of structs needing complex reconstruction
/// - Struct validity (NULL vs present) computed from child def/rep levels using DefRepLevelsToBitmap
/// - No crashes when reading struct { map } or struct { list }
///
/// **Critical limitation:**
/// - Repeated child VALUES are NOT reconstructed
/// - Map/list fields are intentionally OMITTED from StructValue
/// - Only scalar sibling fields are accessible
/// - Accessing structValue.get("mapField", ...) returns nil even when map is present
///
/// **Example:**
/// ```swift
/// // Schema: struct { int32 id; map<string, int64> attributes; }
/// let structs = try rowGroup.readStruct(at: ["user"])
///
/// for struct in structs {
///     if let s = struct {
///         // ✅ Struct validity works: correctly identifies present vs NULL
///         // ✅ Scalar fields work: id is accessible
///         let id = s.get("id", as: Int32.self)
///         // ❌ Map fields DON'T work: attributes is omitted (returns nil)
///         let attrs = s.get("attributes", as: MapType.self)  // Always nil!
///     } else {
///         // ✅ NULL structs correctly identified
///     }
/// }
/// ```
///
/// Full map/list reconstruction is deferred to Phase 4.5+.
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

    /// Check if struct needs complex reconstruction using DefRepLevelsToBitmap
    ///
    /// Matches Arrow C++ StructReader::IsOrHasRepeatedChild() semantics:
    /// Returns TRUE if ANY child has repeated descendants (maps, lists, or repeated fields).
    ///
    /// When true, struct validity must be computed from child def/rep levels using DefRepLevelsToBitmap.
    /// When false, simple definition-level-only reconstruction suffices.
    internal func needsComplexReconstruction(_ element: SchemaElement) -> Bool {
        // Check if ANY child has repeated descendants
        for child in element.children {
            if hasRepeatedOrComplexDescendants(child) {
                // Found a repeated/complex child - need complex reconstruction
                return true
            }
        }
        // No children have repeated descendants - can use simple reconstruction
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

        // Branch between simple and complex reconstruction
        if needsComplexReconstruction(element) {
            // Complex case: struct has repeated children (maps, lists, or repeated fields)
            // Must use DefRepLevelsToBitmap to compute validity from child levels
            return try readStructWithRepeatedChildren(at: path, element: element, fieldColumns: fieldColumns)
        } else {
            // Simple case: struct has only non-repeated children
            // Can use definition-level-only reconstruction
            return try readStructSimple(element: element, fieldColumns: fieldColumns)
        }
    }

    /// Read struct using simple definition-level-only reconstruction
    /// Used when all children are non-repeated (scalars or simple nested structs)
    private func readStructSimple(
        element: SchemaElement,
        fieldColumns: [Column]
    ) throws -> [StructValue?] {
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

    /// Read struct with repeated children using DefRepLevelsToBitmap
    ///
    /// When a struct contains repeated children (maps, lists, or repeated fields),
    /// we must compute struct validity from the child column's def/rep levels.
    ///
    /// Example: struct { int32 id; list<string> tags; }
    /// - The 'tags' child has multiple values per row (repeated)
    /// - Struct validity comes from tags column's levels, projected to struct level
    /// - DefRepLevelsToBitmap handles the projection correctly
    ///
    /// IMPORTANT LIMITATION (Phase 4.4):
    /// This implementation computes struct validity correctly but does NOT reconstruct
    /// repeated child values. Only non-repeated scalar fields are included in StructValue.
    /// Repeated children (maps, lists) are omitted to avoid data corruption from misaligned indexing.
    private func readStructWithRepeatedChildren(
        at path: [String],
        element: SchemaElement,
        fieldColumns: [Column]
    ) throws -> [StructValue?] {
        // PHASE 4.4: STRUCT VALIDITY VIA DEFREPLEVELSTOBITMAP
        //
        // This phase focuses ONLY on computing correct struct validity (NULL vs present).
        // Repeated child reconstruction is deferred to Phase 4.5+.
        //
        // Implementation:
        // 1. Find representative child for validity computation
        // 2. Read its def/rep levels
        // 3. Compute struct validity using DefRepLevelsToBitmap
        // 4. Read ONLY non-repeated scalar children
        // 5. Build StructValues with validity + scalar fields only

        // Find first repeated child at struct level (not leaf level)
        // Maps/lists are group nodes, so we check struct's direct children
        guard let repeatedChild = element.children.first(where: { child in
            hasRepeatedOrComplexDescendants(child)
        }) else {
            throw RowGroupReaderError.internalError(
                "needsComplexReconstruction returned true but no repeated child found in struct children"
            )
        }

        // Find a leaf column belonging to this repeated child
        // Column paths include parent names, e.g., ["user", "attributes", "key_value", "key"]
        // We need to check if the column's schema path passes through the repeated child node
        //
        // IMPORTANT: Use schema node matching, not substring matching!
        // A struct with ["foo_map", "map_metadata"] should not match both for "map"
        guard let representativeColumn = fieldColumns.first(where: { column in
            // Walk up from column's leaf element to root, checking if we pass through repeatedChild
            var current: SchemaElement? = column.element
            while let node = current {
                if node === repeatedChild {
                    return true  // Found the repeated child in this column's ancestry
                }
                current = node.parent
            }
            return false
        }) else {
            throw RowGroupReaderError.internalError(
                "Could not find leaf column for repeated child '\(repeatedChild.name)'"
            )
        }

        // Read representative column's levels
        let (defLevels, repLevels) = try readRepresentativeColumnLevels(representativeColumn)

        // Compute struct's LevelInfo from representative column
        // Use the column's LevelInfo and project to struct level
        let structLevelInfo = computeStructLevelInfo(
            from: representativeColumn,
            structElement: element,
            repeatedChild: repeatedChild
        )

        var validityOutput = ArrayReconstructor.ValidityBitmapOutput()
        try ArrayReconstructor.defRepLevelsToBitmap(
            definitionLevels: defLevels,
            repetitionLevels: repLevels,
            levelInfo: structLevelInfo,
            output: &validityOutput
        )

        // Filter to ONLY non-repeated scalar children to avoid misaligned indexing
        let scalarColumns = fieldColumns.filter { column in
            // Find which struct child this column belongs to by walking up the schema tree
            var current: SchemaElement? = column.element
            var foundChild: SchemaElement?

            // Walk up to find the direct child of the struct
            while let node = current {
                if node.parent === element {
                    // This node is a direct child of the struct
                    foundChild = node
                    break
                }
                current = node.parent
            }

            // Include column only if its parent child is non-repeated
            if let child = foundChild {
                return !hasRepeatedOrComplexDescendants(child)
            }
            return false
        }

        // Read only scalar field columns (one value per row, safe to index by rowIndex)
        let scalarReaders = try readFieldColumns(scalarColumns)

        // Reconstruct structs using validity bitmap + scalar fields only
        var result: [StructValue?] = []
        result.reserveCapacity(validityOutput.valuesRead)

        for rowIndex in 0..<validityOutput.valuesRead {
            if validityOutput.validBits[rowIndex] {
                // Struct is present - build field data from scalar fields only
                // Repeated children are intentionally omitted (Phase 4.4 limitation)
                var fieldData: [String: Any?] = [:]
                for reader in scalarReaders {
                    fieldData[reader.name] = reader.values[rowIndex]
                }
                result.append(StructValue(element: element, fieldData: fieldData))
            } else {
                // Struct is NULL
                result.append(nil)
            }
        }

        return result
    }

    /// Compute struct's LevelInfo by projecting from representative column
    ///
    /// Uses the representative column's LevelInfo (from descriptor) and subtracts
    /// the levels contributed by the repeated child to get struct's levels.
    ///
    /// This matches Arrow's approach: start with column descriptor levels and
    /// project backwards to parent node, rather than guessing by walking the tree.
    private func computeStructLevelInfo(
        from representativeColumn: Column,
        structElement: SchemaElement,
        repeatedChild: SchemaElement
    ) -> LevelInfo {
        // The representative column has exact levels from its descriptor.
        // Example: struct { map<string, int64> attributes }
        //   Column "key": defLevel=4, repLevel=1, repeatedAncestorDefLevel=1
        //     - Root optional struct: +1 def
        //     - Map present: +1 def
        //     - key_value repeated: +1 def, +1 rep
        //     - key required: +0 def
        //   Struct: defLevel=1, repLevel=0, repeatedAncestorDefLevel=0
        //
        // We need to subtract the levels contributed by the repeated child (map)
        // and everything below it.

        // Count levels contributed by repeated child and its descendants
        let childDefLevels = countDefinitionLevels(from: repeatedChild)
        let childRepLevels = countRepetitionLevels(from: repeatedChild)

        // Struct's levels = column's levels - child's contribution
        let structDefLevel = representativeColumn.maxDefinitionLevel - childDefLevels
        let structRepLevel = representativeColumn.maxRepetitionLevel - childRepLevels

        // Repeated ancestor def level
        let structRepeatedAncestorDefLevel: Int
        if structRepLevel > 0 {
            // Struct is inside a repeated group
            // The column's repeatedAncestorDefLevel points to the innermost repeated ancestor
            // If that ancestor is ABOVE the struct, use it; otherwise struct has no repeated ancestor
            if let columnRepeatedAncestorDefLevel = representativeColumn.repeatedAncestorDefLevel {
                // Check if the repeated ancestor is above the struct level
                if columnRepeatedAncestorDefLevel <= structDefLevel {
                    structRepeatedAncestorDefLevel = columnRepeatedAncestorDefLevel
                } else {
                    // Repeated ancestor is below struct (shouldn't happen for valid schemas)
                    structRepeatedAncestorDefLevel = 0
                }
            } else {
                structRepeatedAncestorDefLevel = 0
            }
        } else {
            structRepeatedAncestorDefLevel = 0
        }

        return LevelInfo(
            defLevel: structDefLevel,
            repLevel: structRepLevel,
            repeatedAncestorDefLevel: structRepeatedAncestorDefLevel
        )
    }

    /// Count definition levels contributed by a node and its descendants
    private func countDefinitionLevels(from element: SchemaElement) -> Int {
        var count = 0

        // Add this node's contribution
        if let repetition = element.repetitionType {
            count += repetition.maxDefinitionLevel
        }

        // Recursively add children's contributions
        for child in element.children {
            count += countDefinitionLevels(from: child)
        }

        return count
    }

    /// Count repetition levels contributed by a node and its descendants
    private func countRepetitionLevels(from element: SchemaElement) -> Int {
        var count = 0

        // Add this node's contribution
        if let repetition = element.repetitionType {
            count += repetition.maxRepetitionLevel
        }

        // Recursively add children's contributions
        for child in element.children {
            count += countRepetitionLevels(from: child)
        }

        return count
    }

    /// Read def/rep levels from a representative column for struct validity computation
    private func readRepresentativeColumnLevels(_ column: Column) throws -> (defLevels: [UInt16], repLevels: [UInt16]) {
        let columnIndex = column.index

        // Read levels based on physical type
        switch column.physicalType {
        case .int32:
            let reader = try int32Column(at: columnIndex)
            let (_, defLevels, repLevels) = try reader.readAllWithAllLevels()
            return (defLevels, repLevels)

        case .int64:
            let reader = try int64Column(at: columnIndex)
            let (_, defLevels, repLevels) = try reader.readAllWithAllLevels()
            return (defLevels, repLevels)

        case .float:
            let reader = try floatColumn(at: columnIndex)
            let (_, defLevels, repLevels) = try reader.readAllWithAllLevels()
            return (defLevels, repLevels)

        case .double:
            let reader = try doubleColumn(at: columnIndex)
            let (_, defLevels, repLevels) = try reader.readAllWithAllLevels()
            return (defLevels, repLevels)

        case .byteArray:
            let reader = try stringColumn(at: columnIndex)
            let (_, defLevels, repLevels) = try reader.readAllWithAllLevels()
            return (defLevels, repLevels)

        default:
            throw RowGroupReaderError.unsupportedType(
                "Field \(column.name) has unsupported type \(column.physicalType.name)"
            )
        }
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
