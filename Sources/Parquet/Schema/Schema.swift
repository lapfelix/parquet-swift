// Schema - Parquet file schema representation
//
// Licensed under the Apache License, Version 2.0

/// The schema of a Parquet file.
///
/// Parquet schemas are trees where:
/// - The root is always named "schema" (a group node)
/// - Primitive nodes are leaves (actual data columns)
/// - Group nodes represent nested structures (structs, lists, maps)
///
/// The Thrift format stores the schema as a flat list in depth-first order.
/// This class reconstructs the tree structure and provides navigation APIs.
public final class Schema {
    /// The root node of the schema tree
    public let root: SchemaElement

    /// All leaf columns (primitive types) in the schema
    public let columns: [Column]

    /// Number of columns in the schema
    public var columnCount: Int {
        return columns.count
    }

    /// Creates a schema from the root element.
    public init(root: SchemaElement) {
        self.root = root
        self.columns = Self.collectColumns(from: root)
    }

    /// Finds a column by its path.
    ///
    /// - Parameter path: The path components (e.g., ["user", "address", "city"])
    /// - Returns: The column if found, nil otherwise
    public func column(at path: [String]) -> Column? {
        return columns.first { $0.path == path }
    }

    /// Finds a column by its index (0-based).
    ///
    /// - Parameter index: The column index
    /// - Returns: The column if index is valid, nil otherwise
    public func column(at index: Int) -> Column? {
        guard index >= 0 && index < columns.count else {
            return nil
        }
        return columns[index]
    }

    /// Collects all leaf columns from the schema tree.
    private static func collectColumns(from node: SchemaElement, index: inout Int) -> [Column] {
        var result: [Column] = []

        if node.isLeaf, !node.isRoot {
            // Skip the root node itself
            let column = Column(
                index: index,
                element: node
            )
            result.append(column)
            index += 1
        }

        for child in node.children {
            result.append(contentsOf: collectColumns(from: child, index: &index))
        }

        return result
    }

    private static func collectColumns(from node: SchemaElement) -> [Column] {
        var index = 0
        return collectColumns(from: node, index: &index)
    }
}

/// A leaf column in the Parquet schema.
///
/// Represents a primitive-type field that stores actual data.
/// Group nodes are not columns.
public struct Column {
    /// The index of this column (0-based, in depth-first order)
    public let index: Int

    /// The schema element for this column
    public let element: SchemaElement

    /// The name of the column
    public var name: String {
        return element.name
    }

    /// The full path to this column (excluding root "schema")
    public var path: [String] {
        // Remove "schema" root from path
        return Array(element.path.dropFirst())
    }

    /// The physical type of this column
    public var physicalType: PhysicalType {
        guard let type = element.physicalType else {
            fatalError("Column must have a physical type")
        }
        return type
    }

    /// The logical type of this column (if any)
    public var logicalType: LogicalType? {
        return element.logicalType
    }

    /// The repetition type of this column
    public var repetitionType: Repetition {
        guard let repetition = element.repetitionType else {
            fatalError("Column must have a repetition type")
        }
        return repetition
    }

    /// Whether this column is required (cannot be null)
    public var isRequired: Bool {
        return repetitionType.isRequired
    }

    /// Whether this column is optional (can be null)
    public var isOptional: Bool {
        return repetitionType.isNullable
    }

    /// Whether this column is repeated (list/array)
    public var isRepeated: Bool {
        return repetitionType.isList
    }

    /// Maximum definition level for this column
    ///
    /// Computed by walking the full path from root to leaf and summing
    /// definition level contributions from all nodes (including optional ancestors).
    ///
    /// Example:
    /// ```
    /// optional group foo {
    ///   required int32 bar;
    /// }
    /// ```
    /// Column "bar" has maxDefinitionLevel = 1 (from optional group "foo")
    public var maxDefinitionLevel: Int {
        var level = 0
        var current: SchemaElement? = element

        // Walk up the tree summing definition level contributions
        while let node = current {
            if let repetition = node.repetitionType {
                level += repetition.maxDefinitionLevel
            }
            current = node.parent
        }

        return level
    }

    /// Maximum repetition level for this column
    ///
    /// Computed by walking the full path from root to leaf and summing
    /// repetition level contributions from all nodes (including repeated ancestors).
    ///
    /// Example:
    /// ```
    /// repeated group items {
    ///   required int32 id;
    /// }
    /// ```
    /// Column "id" has maxRepetitionLevel = 1 (from repeated group "items")
    public var maxRepetitionLevel: Int {
        var level = 0
        var current: SchemaElement? = element

        // Walk up the tree summing repetition level contributions
        while let node = current {
            if let repetition = node.repetitionType {
                level += repetition.maxRepetitionLevel
            }
            current = node.parent
        }

        return level
    }

    /// Definition level at which the nearest repeated ancestor is "present but empty".
    ///
    /// Returns nil if the column is not repeated (`maxRepetitionLevel == 0`).
    ///
    /// This is used for array reconstruction to distinguish between:
    /// - NULL list (def < repeatedAncestorDefLevel) → append nil to result
    /// - EMPTY list (def == repeatedAncestorDefLevel) → append [] to result
    /// - List with NULL element (repeatedAncestorDefLevel < def < maxDefinitionLevel) → append [nil]
    /// - List with value (def == maxDefinitionLevel) → append [value]
    ///
    /// Example:
    /// ```
    /// optional group items (List) {
    ///   repeated group list {
    ///     optional int32 element;
    ///   }
    /// }
    /// ```
    /// - maxDefinitionLevel = 3 (optional +1, repeated +1, optional +1)
    /// - maxRepetitionLevel = 1 (from repeated group)
    /// - repeatedAncestorDefLevel = 1 (def level when optional parent is present)
    ///
    /// Matches Arrow's `definition_level_of_list` logic.
    public var repeatedAncestorDefLevel: Int? {
        guard maxRepetitionLevel > 0 else { return nil }

        // Build the path from root → leaf
        var nodes: [SchemaElement] = []
        var current: SchemaElement? = element
        while let node = current {
            nodes.append(node)
            current = node.parent
        }
        nodes.reverse()  // root first, leaf last
        guard nodes.count >= 2 else { return nil }

        var defLevel = 0

        // Walk the path (skip root at index 0)
        for node in nodes[1...] {
            guard let repetition = node.repetitionType else { continue }

            switch repetition {
            case .repeated:
                // Return the definition level at which this repeated ancestor is "present"
                // This is the current defLevel, NOT defLevel + 1
                // The repeated group is considered "present but empty" when def equals
                // the sum of optional/repeated ancestors BEFORE the repeated group itself
                return defLevel
            case .optional:
                defLevel += 1
            case .required:
                break
            }
        }

        return nil  // No repeated ancestor found
    }

    /// Array of definition levels at which each repeated ancestor is "present but empty".
    ///
    /// Returns nil if the column is not repeated (`maxRepetitionLevel == 0`).
    ///
    /// For multi-level nested types (maxRepetitionLevel > 1), this returns an array
    /// where index i contains the definition level for the repeated ancestor at repetition level i+1.
    ///
    /// Example:
    /// ```
    /// optional group outer (List) {
    ///   repeated group outer_list {        // rep level 1
    ///     optional group inner (List) {
    ///       repeated group inner_list {    // rep level 2
    ///         optional int32 element;
    ///       }
    ///     }
    ///   }
    /// }
    /// ```
    /// - maxRepetitionLevel = 2
    /// - repeatedAncestorDefLevels = [1, 3]
    ///   - Index 0 (rep level 1): outer list present at def=1
    ///   - Index 1 (rep level 2): inner list present at def=3
    public var repeatedAncestorDefLevels: [Int]? {
        guard maxRepetitionLevel > 0 else { return nil }

        var result: [Int] = []
        var nodes: [SchemaElement] = []
        var current: SchemaElement? = element

        // Build path from root to leaf
        while let node = current {
            nodes.append(node)
            current = node.parent
        }
        nodes.reverse()  // root first

        var defLevel = 0
        var repeatedCount = 0

        // Walk the path (skip root at index 0)
        guard nodes.count >= 2 else {
            return []
        }

        for node in nodes[1...] {
            guard let repetition = node.repetitionType else { continue }

            switch repetition {
            case .repeated:
                // This repeated node's def level is the current accumulated def
                result.append(defLevel)
                repeatedCount += 1
                // Repeated nodes also contribute 1 to definition level
                defLevel += 1
            case .optional:
                defLevel += 1
            case .required:
                break
            }
        }

        return result
    }
}

// MARK: - Struct and Map Support

extension Schema {
    /// Finds a schema element by path (excluding root "schema")
    ///
    /// - Parameter path: Path components (e.g., ["user"], ["user", "address"])
    /// - Returns: The schema element if found, nil otherwise
    ///
    /// # Example
    ///
    /// ```swift
    /// let userElement = schema.element(at: ["user"])
    /// let cityElement = schema.element(at: ["user", "address", "city"])
    /// ```
    public func element(at path: [String]) -> SchemaElement? {
        return root.descendant(at: path)
    }

    /// Returns all field columns of a struct
    ///
    /// - Parameter path: Path to the struct (e.g., ["user"])
    /// - Returns: Array of columns that are fields of this struct, or nil if not a struct
    ///
    /// # Example
    ///
    /// ```swift
    /// // For struct: user { name: string, age: int32 }
    /// let fields = schema.structFields(at: ["user"])
    /// // Returns: [Column(path: ["user", "name"]), Column(path: ["user", "age"])]
    /// ```
    public func structFields(at path: [String]) -> [Column]? {
        guard let element = element(at: path), element.isStruct else {
            return nil
        }

        // Find all leaf columns that are descendants of this struct
        let fullPath = ["schema"] + path
        return columns.filter { column in
            let columnPath = ["schema"] + column.path
            // Check if this column is a direct descendant of the struct
            return columnPath.starts(with: fullPath) && columnPath.count > fullPath.count
        }
    }
}

extension Schema: CustomStringConvertible {
    public var description: String {
        var result = "Parquet Schema:\n"
        result += describeNode(root)
        result += "\nColumns: \(columnCount)"
        return result
    }

    private func describeNode(_ node: SchemaElement) -> String {
        var result = node.description + "\n"
        for child in node.children {
            result += describeNode(child)
        }
        return result
    }
}
