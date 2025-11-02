// Thrift FileMetaData - File-level metadata in Parquet format
//
// Licensed under the Apache License, Version 2.0

/// Empty struct to signal the order defined by the physical or logical type.
///
/// Maps to Thrift `TypeDefinedOrder` struct.
public struct ThriftTypeDefinedOrder: Sendable {
    public init() {}
}

/// Union to specify the order used for the min_value and max_value fields for a column.
///
/// If the reader does not support the value of this union, min and max stats
/// for this column should be ignored.
///
/// Maps to Thrift `ColumnOrder` union.
public enum ThriftColumnOrder: Sendable {
    /// The sort orders for logical types are defined by their type
    /// (see Thrift definition for full list)
    case typeOrder

    public var name: String {
        switch self {
        case .typeOrder: return "TYPE_ORDER"
        }
    }
}

/// Description for file metadata.
///
/// Maps to Thrift `FileMetaData` struct.
public struct ThriftFileMetaData: Sendable {
    /// Version of this file
    public let version: Int32

    /// Parquet schema for this file
    /// This schema contains metadata for all the columns.
    /// The schema is represented as a tree with a single root.
    /// The nodes of the tree are flattened to a list by doing a depth-first traversal.
    /// The first element is the root.
    public let schema: [ThriftSchemaElement]

    /// Number of rows in this file
    public let numRows: Int64

    /// Row groups in this file
    public let rowGroups: [ThriftRowGroup]

    /// Optional key/value metadata
    public let keyValueMetadata: [ThriftKeyValue]?

    /// String for application that wrote this file
    /// Should be in the format: <Application> version <App Version> (build <App Build Hash>)
    /// e.g. "impala version 1.0 (build 6cf94d29b2b7115df4de2c06e2ab4326d721eb55)"
    public let createdBy: String?

    /// Sort order used for the min_value and max_value fields in the Statistics
    /// objects and the min_values and max_values fields in the ColumnIndex objects
    /// of each column in this file.
    ///
    /// Sort orders are listed in the order matching the columns in the schema.
    /// Without column_orders, the meaning of the min_value and max_value fields
    /// in the Statistics object and the ColumnIndex object is undefined.
    public let columnOrders: [ThriftColumnOrder]?

    public init(
        version: Int32,
        schema: [ThriftSchemaElement],
        numRows: Int64,
        rowGroups: [ThriftRowGroup],
        keyValueMetadata: [ThriftKeyValue]? = nil,
        createdBy: String? = nil,
        columnOrders: [ThriftColumnOrder]? = nil
    ) {
        self.version = version
        self.schema = schema
        self.numRows = numRows
        self.rowGroups = rowGroups
        self.keyValueMetadata = keyValueMetadata
        self.createdBy = createdBy
        self.columnOrders = columnOrders
    }
}
