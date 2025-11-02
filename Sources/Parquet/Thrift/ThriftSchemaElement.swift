// Thrift SchemaElement - Schema definition in Parquet format
//
// Licensed under the Apache License, Version 2.0

/// Represents an element inside a schema definition.
///
/// - If it is a group (inner node): type is nil and numChildren is defined
/// - If it is a primitive type (leaf): type is defined and numChildren is nil
///
/// The nodes are listed in depth-first traversal order.
///
/// Maps to Thrift `SchemaElement` struct.
public struct ThriftSchemaElement: Sendable {
    /// Data type for this field
    /// Not set if the current element is a non-leaf node
    public let type: ThriftType?

    /// If type is FIXED_LEN_BYTE_ARRAY, this is the byte length of the values
    /// Otherwise, if specified, this is the maximum bit length to store any of the values
    public let typeLength: Int32?

    /// Repetition of the field
    /// The root of the schema does not have a repetition_type
    /// All other nodes must have one
    public let repetitionType: ThriftFieldRepetitionType?

    /// Name of the field in the schema
    public let name: String

    /// Nested fields
    /// Since Thrift does not support nested fields, the nesting is flattened to a
    /// single list by a depth-first traversal. The children count is used to construct
    /// the nested relationship. This field is not set when the element is a primitive type.
    public let numChildren: Int32?

    /// DEPRECATED: When the schema is the result of a conversion from another model
    /// This is superseded by logicalType
    public let convertedType: ThriftConvertedType?

    /// DEPRECATED: Used when this column contains decimal data
    /// This is superseded by using the DecimalType annotation in logicalType
    public let scale: Int32?
    public let precision: Int32?

    /// When the original schema supports field ids, this saves the original field id
    public let fieldId: Int32?

    /// The logical type of this SchemaElement
    /// LogicalType replaces ConvertedType, but ConvertedType is still required
    /// for some logical types to ensure forward-compatibility in format v1
    public let logicalType: ThriftLogicalType?

    public init(
        type: ThriftType? = nil,
        typeLength: Int32? = nil,
        repetitionType: ThriftFieldRepetitionType? = nil,
        name: String,
        numChildren: Int32? = nil,
        convertedType: ThriftConvertedType? = nil,
        scale: Int32? = nil,
        precision: Int32? = nil,
        fieldId: Int32? = nil,
        logicalType: ThriftLogicalType? = nil
    ) {
        self.type = type
        self.typeLength = typeLength
        self.repetitionType = repetitionType
        self.name = name
        self.numChildren = numChildren
        self.convertedType = convertedType
        self.scale = scale
        self.precision = precision
        self.fieldId = fieldId
        self.logicalType = logicalType
    }
}
