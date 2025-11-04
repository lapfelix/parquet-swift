// RowGroupReader+Map - Map reading support
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Extension for reading map columns
///
/// Maps in Parquet are encoded as `list<struct<key,value>>`, where:
/// - The outer list represents the map container (repeated group)
/// - Each struct element is a key-value pair
///
/// This implementation treats maps exactly like Arrow C++ does:
/// 1. Read the key_value struct as a repeated struct
/// 2. Convert StructValue pairs to MapEntry objects
/// 3. All rep/def level logic is handled by ArrayReconstructor
extension RowGroupReader {
    /// Read all rows of a map column
    ///
    /// - Parameter path: Path to the map (e.g., ["attributes"])
    /// - Returns: Array where:
    ///   - `nil` = NULL map (map instance not present)
    ///   - `[MapEntry]` = map present (may be empty or have NULL values)
    ///
    /// # Example
    ///
    /// ```swift
    /// let maps = try rowGroup.readMap(at: ["attributes"])
    /// for (i, map) in maps.enumerated() {
    ///     if let map = map {
    ///         print("Row \(i): \(map.count) entries")
    ///         for entry in map {
    ///             print("  \(entry.key): \(entry.value ?? "NULL")")
    ///         }
    ///     } else {
    ///         print("Row \(i): NULL map")
    ///     }
    /// }
    /// ```
    ///
    /// # NULL Semantics
    ///
    /// For schema: `optional group attributes (MAP) { repeated group key_value { key, value } }`
    ///
    /// - `defLevel < repeatedAncestorDefLevel` → NULL map → return `nil`
    /// - `defLevel == repeatedAncestorDefLevel` → Empty map → return `[]`
    /// - `repLevel < maxRepLevel` → New map
    /// - `repLevel == maxRepLevel` → Continuation of current map
    ///
    /// # Nested Maps
    ///
    /// This implementation correctly handles:
    /// - Maps inside optional structs
    /// - Maps inside lists (`list<map<...>>`)
    /// - Lists inside maps (`map<k, list<v>>`)
    ///
    /// All rep/def level logic is delegated to `ArrayReconstructor`, which matches
    /// the Arrow C++ implementation.
    ///
    /// - Throws: `RowGroupReaderError` if path doesn't point to a map
    public func readMap(at path: [String]) throws -> [[MapEntry]?] {
        // Validate path points to a map
        guard let element = schema.element(at: path), element.isMap else {
            throw RowGroupReaderError.unsupportedType(
                "Path \(path.joined(separator: ".")) does not point to a map"
            )
        }

        // Get the path to the repeated key_value struct
        guard let kvPath = schema.mapKeyValuePath(at: path) else {
            throw RowGroupReaderError.unsupportedType(
                "Could not find key_value struct for map at \(path.joined(separator: "."))"
            )
        }

        // Read as repeated struct (list<struct<key,value>>)
        // Catch errors from readRepeatedStruct and provide map-specific message
        let listOfStructs: [[StructValue?]?]
        do {
            listOfStructs = try readRepeatedStruct(at: kvPath)
        } catch RowGroupReaderError.unsupportedType {
            // The key_value struct has repeated or complex children
            // Determine which side (key or value) is problematic
            guard let kvElement = schema.element(at: kvPath) else {
                throw RowGroupReaderError.unsupportedType(
                    "Maps with complex keys or values are not yet supported.\n" +
                    "\n" +
                    "This limitation will be removed once proper multi-level reconstruction is implemented.\n" +
                    "See docs/limitations.md for details."
                )
            }

            // Check key and value children recursively
            // This catches cases like map<struct { repeated int32 }, string> where
            // the key is a struct containing repeated fields
            let keyChild = kvElement.children.first { $0.name == "key" }
            let valueChild = kvElement.children.first { $0.name == "value" }

            // Use recursive check to detect complexity at any depth
            let keyIsComplex = keyChild.map { hasRepeatedOrComplexDescendants($0) } ?? false
            let valueIsComplex = valueChild.map { hasRepeatedOrComplexDescendants($0) } ?? false

            let problematicSide: String
            if keyIsComplex && valueIsComplex {
                problematicSide = "both keys and values"
            } else if keyIsComplex {
                problematicSide = "keys"
            } else if valueIsComplex {
                problematicSide = "values"
            } else {
                problematicSide = "keys or values"  // Shouldn't happen, but be safe
            }

            throw RowGroupReaderError.unsupportedType(
                "Maps with complex \(problematicSide) are not yet supported.\n" +
                "\n" +
                "This map has \(problematicSide) of type:\n" +
                "- list<T>\n" +
                "- map<K,V> (nested map)\n" +
                "- repeated primitives\n" +
                "\n" +
                "Workaround:\n" +
                "Maps with complex \(problematicSide) cannot currently be read. Consider:\n" +
                "1. Reading the underlying columns directly if you understand the Parquet encoding\n" +
                "2. Redesigning the schema to use simpler types\n" +
                "\n" +
                "This limitation will be removed once proper multi-level reconstruction is implemented.\n" +
                "See docs/limitations.md for details."
            )
        }

        // Convert [[StructValue?]?] to [[MapEntry]?]
        return try listOfStructs.map { list in
            // list is [StructValue?]? (one map)
            guard let list = list else {
                return nil  // NULL map
            }

            // Convert each StructValue to MapEntry
            return try list.compactMap { structValue -> MapEntry? in
                guard let kv = structValue else {
                    // NULL struct in list - this shouldn't happen for map key_value
                    // but handle gracefully
                    throw RowGroupReaderError.internalError(
                        "Encountered NULL key_value struct in map at \(path.joined(separator: "."))"
                    )
                }

                // Extract key and value from struct
                // Map keys are required in Parquet, so key should never be nil
                guard let key = kv["key"] else {
                    throw RowGroupReaderError.internalError(
                        "Map key is NULL at \(path.joined(separator: "."))"
                    )
                }

                // Value can be nil (NULL value)
                let value = kv["value"]

                return MapEntry(key: key, value: value)
            }
        }
    }
}
