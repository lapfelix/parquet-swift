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
    public private(set) var children: [SchemaElement]

    /// Parent node (nil for root)
    public weak var parent: SchemaElement?

    /// The depth of this node in the tree (0 for root)
    public private(set) var depth: Int

    /// Full path from root to this node (e.g., ["schema", "user", "name"])
    public private(set) var path: [String]

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

        // Re-home children under this node so parent/path/depth stay consistent.
        for child in children {
            child.updateHierarchy(parent: self, depth: depth + 1)
        }
    }

    /// Add a child to this element.
    ///
    /// Only group elements may contain children.
    ///
    /// - Parameter child: The child element to attach
    /// - Throws: SchemaError if this is a primitive element
    public func addChild(_ child: SchemaElement) throws {
        guard physicalType == nil else {
            throw SchemaError.invalidSchema("Primitive element '\(name)' cannot have children")
        }

        child.detachFromParent()
        child.updateHierarchy(parent: self, depth: depth + 1)
        children.append(child)
    }

    private func detachFromParent() {
        parent?.children.removeAll { $0 === self }
        parent = nil
    }

    private func updateHierarchy(parent: SchemaElement?, depth: Int) {
        self.parent = parent
        self.depth = depth

        if let parent {
            self.path = parent.path + [name]
        } else {
            self.path = [name]
        }

        for child in children {
            child.updateHierarchy(parent: self, depth: depth + 1)
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

// MARK: - Struct and Map Detection

extension SchemaElement {
    /// Whether this element is a struct (group without LIST or MAP logical type)
    public var isStruct: Bool {
        guard isGroup else { return false }
        if case .group(let logical) = elementType {
            return logical != .list && logical != .map
        }
        return false
    }

    /// Whether this element is a list (group with LIST logical type)
    public var isList: Bool {
        guard isGroup else { return false }
        if case .group(let logical) = elementType {
            return logical == .list
        }
        return false
    }

    /// Whether this element is a map (group with MAP logical type)
    public var isMap: Bool {
        guard isGroup else { return false }
        if case .group(let logical) = elementType {
            return logical == .map
        }
        return false
    }

    /// Find a child element by name
    ///
    /// - Parameter name: The child's name
    /// - Returns: The child element if found, nil otherwise
    public func child(named name: String) -> SchemaElement? {
        return children.first { $0.name == name }
    }

    /// Find a descendant element by path
    ///
    /// - Parameter path: Path components from this element
    /// - Returns: The descendant element if found, nil otherwise
    ///
    /// # Example
    ///
    /// ```swift
    /// let address = userElement.descendant(at: ["address"])
    /// let city = userElement.descendant(at: ["address", "city"])
    /// ```
    public func descendant(at path: [String]) -> SchemaElement? {
        guard !path.isEmpty else { return self }

        var current = self
        for component in path {
            guard let child = current.child(named: component) else {
                return nil
            }
            current = child
        }
        return current
    }
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
