// SchemaElement - Node in the Parquet schema tree
//
// Licensed under the Apache License, Version 2.0

/// A node in the Parquet schema tree.
///
/// The schema is represented as a tree where:
/// - Root node is always named "schema" (group node)
/// - Group nodes have children (nested structures)
/// - Primitive nodes are leaves (actual data columns)
///
/// The tree is stored as a flat list in Thrift (depth-first traversal)
/// and reconstructed here into a proper tree structure.
public final class SchemaElement {
    /// Name of this schema element
    public let name: String

    /// Type of this element
    public let elementType: ElementType

    /// Repetition type (nil for root)
    public let repetitionType: Repetition?

    /// Field ID (optional, for compatibility with other systems)
    public let fieldId: Int32?

    /// Children of this node (empty for primitive types)
    public let children: [SchemaElement]

    /// Parent node (nil for root)
    public weak var parent: SchemaElement?

    /// The depth of this node in the tree (0 for root)
    public let depth: Int

    /// Full path from root to this node (e.g., ["schema", "user", "name"])
    public let path: [String]

    /// Whether this is the root node
    public var isRoot: Bool {
        return parent == nil
    }

    /// Whether this is a leaf node (primitive type)
    public var isLeaf: Bool {
        return children.isEmpty
    }

    /// Whether this is a group node (has children)
    public var isGroup: Bool {
        return !children.isEmpty
    }

    /// The physical type (nil for group nodes)
    public var physicalType: PhysicalType? {
        if case .primitive(physicalType: let type, logicalType: _) = elementType {
            return type
        }
        return nil
    }

    /// The logical type (nil for group nodes or primitives without logical type)
    public var logicalType: LogicalType? {
        if case .primitive(physicalType: _, logicalType: let logical) = elementType {
            return logical
        }
        if case .group(logicalType: let logical) = elementType {
            return logical
        }
        return nil
    }

    public init(
        name: String,
        elementType: ElementType,
        repetitionType: Repetition?,
        fieldId: Int32?,
        children: [SchemaElement],
        parent: SchemaElement?,
        depth: Int
    ) {
        self.name = name
        self.elementType = elementType
        self.repetitionType = repetitionType
        self.fieldId = fieldId
        self.children = children
        self.parent = parent
        self.depth = depth

        // Calculate full path
        if let parent = parent {
            self.path = parent.path + [name]
        } else {
            self.path = [name]
        }

        // Set parent reference for all children
        for child in children {
            child.parent = self
        }
    }
}

/// Type of a schema element (group or primitive)
public enum ElementType: Sendable {
    /// Group node (has children, no physical type)
    case group(logicalType: LogicalType?)

    /// Primitive node (leaf, has physical type)
    case primitive(physicalType: PhysicalType, logicalType: LogicalType?)
}

extension SchemaElement: CustomStringConvertible {
    public var description: String {
        let indent = String(repeating: "  ", count: depth)
        let repStr = repetitionType.map { " \($0.rawValue)" } ?? ""

        switch elementType {
        case .group(let logical):
            let logicalStr = logical.map { " (\($0.name))" } ?? ""
            return "\(indent)\(name)\(repStr) GROUP\(logicalStr)"
        case .primitive(let physical, let logical):
            let logicalStr = logical.map { " (\($0.name))" } ?? ""
            return "\(indent)\(name)\(repStr) \(physical.name)\(logicalStr)"
        }
    }
}
