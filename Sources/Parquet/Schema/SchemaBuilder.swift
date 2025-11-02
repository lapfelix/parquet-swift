// SchemaBuilder - Builds Schema tree from Thrift metadata
//
// Licensed under the Apache License, Version 2.0

/// Errors that can occur during schema building.
public enum SchemaError: Error {
    case invalidSchema(String)
    case unsupportedFeature(String)
}

/// Builds a Schema tree from a flat list of ThriftSchemaElements.
///
/// The Thrift format stores schemas as a flat list in depth-first traversal order.
/// Each node has a `num_children` field indicating how many children follow it.
///
/// Example flat list:
/// ```
/// [0] schema (GROUP, num_children=2)
/// [1]   user (GROUP, num_children=2)
/// [2]     id (INT64)
/// [3]     name (BYTE_ARRAY, STRING)
/// [4]   timestamp (INT64, TIMESTAMP)
/// ```
///
/// This gets reconstructed into a tree:
/// ```
/// schema
/// ├── user (GROUP)
/// │   ├── id (INT64)
/// │   └── name (BYTE_ARRAY, STRING)
/// └── timestamp (INT64, TIMESTAMP)
/// ```
public struct SchemaBuilder {
    /// Builds a Schema from a list of ThriftSchemaElements.
    ///
    /// - Parameter elements: The flat list of schema elements from FileMetaData
    /// - Returns: The reconstructed schema tree
    /// - Throws: SchemaError if the schema is invalid
    public static func buildSchema(from elements: [ThriftSchemaElement]) throws -> Schema {
        guard !elements.isEmpty else {
            throw SchemaError.invalidSchema("Schema element list is empty")
        }

        // The first element should be the root (a group with children)
        // Note: The name can be anything (commonly "schema", "hive_schema", or single letter like "m")
        guard elements[0].numChildren != nil && elements[0].numChildren! > 0 else {
            throw SchemaError.invalidSchema("First element must be a group with children")
        }

        var index = 0
        let root = try buildNode(from: elements, index: &index, depth: 0, parent: nil)

        return Schema(root: root)
    }

    /// Recursively builds a schema node and its children.
    private static func buildNode(
        from elements: [ThriftSchemaElement],
        index: inout Int,
        depth: Int,
        parent: SchemaElement?
    ) throws -> SchemaElement {
        guard index < elements.count else {
            throw SchemaError.invalidSchema("Unexpected end of schema elements")
        }

        let thriftElement = elements[index]
        index += 1

        // Determine if this is a group or primitive
        let elementType: ElementType
        let numChildren = Int(thriftElement.numChildren ?? 0)

        if let physicalType = thriftElement.type {
            // Primitive type
            guard numChildren == 0 else {
                throw SchemaError.invalidSchema("Primitive type '\(thriftElement.name)' cannot have children")
            }

            let swiftPhysicalType = try convertPhysicalType(physicalType, typeLength: thriftElement.typeLength)
            let logicalType = try convertLogicalType(thriftElement)
            elementType = .primitive(physicalType: swiftPhysicalType, logicalType: logicalType)
        } else {
            // Group type
            let logicalType = try convertLogicalType(thriftElement)
            elementType = .group(logicalType: logicalType)
        }

        // Convert repetition type (root has no repetition)
        let repetitionType: Repetition?
        if depth == 0 {
            repetitionType = nil
        } else {
            guard let thriftRep = thriftElement.repetitionType else {
                throw SchemaError.invalidSchema("Non-root element '\(thriftElement.name)' must have repetition type")
            }
            repetitionType = convertRepetitionType(thriftRep)
        }

        // Create the node (without children first)
        let node = SchemaElement(
            name: thriftElement.name,
            elementType: elementType,
            repetitionType: repetitionType,
            fieldId: thriftElement.fieldId,
            children: [],
            parent: parent,
            depth: depth
        )

        // Build children
        var children: [SchemaElement] = []
        for _ in 0..<numChildren {
            let child = try buildNode(from: elements, index: &index, depth: depth + 1, parent: node)
            children.append(child)
        }

        // Update children (Swift doesn't allow mutation after init for let properties)
        // We need to recreate the node with children
        let finalNode = SchemaElement(
            name: thriftElement.name,
            elementType: elementType,
            repetitionType: repetitionType,
            fieldId: thriftElement.fieldId,
            children: children,
            parent: parent,
            depth: depth
        )

        return finalNode
    }

    // MARK: - Type Conversion

    private static func convertPhysicalType(
        _ thriftType: ThriftType,
        typeLength: Int32?
    ) throws -> PhysicalType {
        switch thriftType {
        case .boolean:
            return .boolean
        case .int32:
            return .int32
        case .int64:
            return .int64
        case .int96:
            return .int96
        case .float:
            return .float
        case .double:
            return .double
        case .byteArray:
            return .byteArray
        case .fixedLenByteArray:
            guard let length = typeLength else {
                throw SchemaError.invalidSchema("FIXED_LEN_BYTE_ARRAY must have type_length")
            }
            return .fixedLenByteArray(length: Int(length))
        }
    }

    private static func convertRepetitionType(_ thriftRep: ThriftFieldRepetitionType) -> Repetition {
        switch thriftRep {
        case .required:
            return .required
        case .optional:
            return .optional
        case .repeated:
            return .repeated
        }
    }

    private static func convertLogicalType(_ element: ThriftSchemaElement) throws -> LogicalType? {
        // Prefer LogicalType over deprecated ConvertedType
        if let thriftLogical = element.logicalType {
            return try convertThriftLogicalType(thriftLogical)
        }

        // Fall back to ConvertedType for older files
        if let convertedType = element.convertedType {
            return convertConvertedType(convertedType, scale: element.scale, precision: element.precision)
        }

        return nil
    }

    private static func convertThriftLogicalType(_ thriftLogical: ThriftLogicalType) throws -> LogicalType {
        switch thriftLogical {
        case .string:
            return .string
        case .map:
            return .map
        case .list:
            return .list
        case .enum:
            return .enum
        case .decimal(let decimal):
            return .decimal(precision: Int(decimal.precision), scale: Int(decimal.scale))
        case .date:
            return .date
        case .time(let time):
            let unit = convertTimeUnit(time.unit)
            return .time(isAdjustedToUTC: time.isAdjustedToUTC, unit: unit)
        case .timestamp(let ts):
            let unit = convertTimeUnit(ts.unit)
            return .timestamp(isAdjustedToUTC: ts.isAdjustedToUTC, unit: unit)
        case .integer(let int):
            return .integer(bitWidth: Int(int.bitWidth), isSigned: int.isSigned)
        case .unknown:
            throw SchemaError.unsupportedFeature("Unknown/Null logical type")
        case .json:
            return .json
        case .bson:
            return .bson
        case .uuid:
            return .uuid
        case .float16:
            throw SchemaError.unsupportedFeature("FLOAT16 logical type not yet supported")
        }
    }

    private static func convertTimeUnit(_ thriftUnit: ThriftTimeUnit) -> TimeUnit {
        switch thriftUnit {
        case .millis:
            return .milliseconds
        case .micros:
            return .microseconds
        case .nanos:
            return .nanoseconds
        }
    }

    private static func convertConvertedType(
        _ convertedType: ThriftConvertedType,
        scale: Int32?,
        precision: Int32?
    ) -> LogicalType? {
        switch convertedType {
        case .utf8:
            return .string
        case .map:
            return .map
        case .mapKeyValue:
            return nil  // Internal marker, not a logical type
        case .list:
            return .list
        case .enum:
            return .enum
        case .decimal:
            guard let scale = scale, let precision = precision else {
                return nil
            }
            return .decimal(precision: Int(precision), scale: Int(scale))
        case .date:
            return .date
        case .timeMillis:
            return .time(isAdjustedToUTC: true, unit: .milliseconds)
        case .timeMicros:
            return .time(isAdjustedToUTC: true, unit: .microseconds)
        case .timestampMillis:
            return .timestamp(isAdjustedToUTC: true, unit: .milliseconds)
        case .timestampMicros:
            return .timestamp(isAdjustedToUTC: true, unit: .microseconds)
        case .uint8:
            return .integer(bitWidth: 8, isSigned: false)
        case .uint16:
            return .integer(bitWidth: 16, isSigned: false)
        case .uint32:
            return .integer(bitWidth: 32, isSigned: false)
        case .uint64:
            return .integer(bitWidth: 64, isSigned: false)
        case .int8:
            return .integer(bitWidth: 8, isSigned: true)
        case .int16:
            return .integer(bitWidth: 16, isSigned: true)
        case .int32:
            return .integer(bitWidth: 32, isSigned: true)
        case .int64:
            return .integer(bitWidth: 64, isSigned: true)
        case .json:
            return .json
        case .bson:
            return .bson
        case .interval:
            return nil  // Not supported yet
        }
    }
}
