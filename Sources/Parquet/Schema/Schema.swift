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
