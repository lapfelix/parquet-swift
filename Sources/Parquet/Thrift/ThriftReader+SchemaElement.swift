// ThriftReader extensions for SchemaElement deserialization
//
// Licensed under the Apache License, Version 2.0

import Foundation

extension ThriftReader {
    /// Reads a SchemaElement from the current position.
    func readSchemaElement() throws -> ThriftSchemaElement {
        var type: ThriftType?
        var typeLength: Int32?
        var repetitionType: ThriftFieldRepetitionType?
        var name: String?
        var numChildren: Int32?
        var convertedType: ThriftConvertedType?
        var scale: Int32?
        var precision: Int32?
        var fieldId: Int32?
        var logicalType: ThriftLogicalType?

        var lastFieldId: Int16 = 0

        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            switch field.fieldId {
            case 1: // type
                let value = try readVarint32()
                type = ThriftType(rawValue: value)
            case 2: // type_length
                typeLength = try readVarint32()
            case 3: // repetition_type
                let value = try readVarint32()
                repetitionType = ThriftFieldRepetitionType(rawValue: value)
            case 4: // name
                name = try readString()
            case 5: // num_children
                numChildren = try readVarint32()
            case 6: // converted_type
                let value = try readVarint32()
                convertedType = ThriftConvertedType(rawValue: value)
            case 7: // scale
                scale = try readVarint32()
            case 8: // precision
                precision = try readVarint32()
            case 9: // field_id
                fieldId = try readVarint32()
            case 10: // logical_type
                logicalType = try readLogicalType()
            default:
                try skipField(type: field.type)
            }
        }

        guard let name = name else {
            throw ThriftError.invalidData("Missing required field 'name' in SchemaElement")
        }

        return ThriftSchemaElement(
            type: type,
            typeLength: typeLength,
            repetitionType: repetitionType,
            name: name,
            numChildren: numChildren,
            convertedType: convertedType,
            scale: scale,
            precision: precision,
            fieldId: fieldId,
            logicalType: logicalType
        )
    }

    // MARK: - LogicalType Reading

    func readLogicalType() throws -> ThriftLogicalType? {
        var lastFieldId: Int16 = 0

        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            switch field.fieldId {
            case 1: // STRING
                _ = try readEmptyStruct()
                return .string
            case 2: // MAP
                _ = try readEmptyStruct()
                return .map
            case 3: // LIST
                _ = try readEmptyStruct()
                return .list
            case 4: // ENUM
                _ = try readEmptyStruct()
                return .enum
            case 5: // DECIMAL
                let decimal = try readDecimalType()
                return .decimal(decimal)
            case 6: // DATE
                _ = try readEmptyStruct()
                return .date
            case 7: // TIME
                let time = try readTimeType()
                return .time(time)
            case 8: // TIMESTAMP
                let timestamp = try readTimestampType()
                return .timestamp(timestamp)
            case 10: // INTEGER
                let integer = try readIntType()
                return .integer(integer)
            case 11: // UNKNOWN (NULL)
                _ = try readEmptyStruct()
                return .unknown
            case 12: // JSON
                _ = try readEmptyStruct()
                return .json
            case 13: // BSON
                _ = try readEmptyStruct()
                return .bson
            case 14: // UUID
                _ = try readEmptyStruct()
                return .uuid
            case 15: // FLOAT16
                _ = try readEmptyStruct()
                return .float16
            default:
                // Skip unknown logical types
                try skipField(type: field.type)
            }
        }

        return nil
    }

    func readEmptyStruct() throws {
        var lastFieldId: Int16 = 0
        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            try skipField(type: field.type)
        }
    }

    func readDecimalType() throws -> ThriftDecimalType {
        var scale: Int32?
        var precision: Int32?
        var lastFieldId: Int16 = 0

        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            switch field.fieldId {
            case 1: // scale
                scale = try readVarint32()
            case 2: // precision
                precision = try readVarint32()
            default:
                try skipField(type: field.type)
            }
        }

        guard let scale = scale, let precision = precision else {
            throw ThriftError.invalidData("Missing required fields in DecimalType")
        }

        return ThriftDecimalType(scale: scale, precision: precision)
    }

    func readTimeType() throws -> ThriftTimeType {
        var isAdjustedToUTC: Bool?
        var unit: ThriftTimeUnit?
        var lastFieldId: Int16 = 0

        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            switch field.fieldId {
            case 1: // isAdjustedToUTC
                isAdjustedToUTC = field.type == .boolTrue
            case 2: // unit
                unit = try readTimeUnit()
            default:
                try skipField(type: field.type)
            }
        }

        guard let isAdjustedToUTC = isAdjustedToUTC, let unit = unit else {
            throw ThriftError.invalidData("Missing required fields in TimeType")
        }

        return ThriftTimeType(isAdjustedToUTC: isAdjustedToUTC, unit: unit)
    }

    func readTimestampType() throws -> ThriftTimestampType {
        var isAdjustedToUTC: Bool?
        var unit: ThriftTimeUnit?
        var lastFieldId: Int16 = 0

        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            switch field.fieldId {
            case 1: // isAdjustedToUTC
                isAdjustedToUTC = field.type == .boolTrue
            case 2: // unit
                unit = try readTimeUnit()
            default:
                try skipField(type: field.type)
            }
        }

        guard let isAdjustedToUTC = isAdjustedToUTC, let unit = unit else {
            throw ThriftError.invalidData("Missing required fields in TimestampType")
        }

        return ThriftTimestampType(isAdjustedToUTC: isAdjustedToUTC, unit: unit)
    }

    func readIntType() throws -> ThriftIntType {
        var bitWidth: Int8?
        var isSigned: Bool?
        var lastFieldId: Int16 = 0

        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            switch field.fieldId {
            case 1: // bitWidth
                bitWidth = try readI8()
            case 2: // isSigned
                isSigned = field.type == .boolTrue
            default:
                try skipField(type: field.type)
            }
        }

        guard let bitWidth = bitWidth, let isSigned = isSigned else {
            throw ThriftError.invalidData("Missing required fields in IntType")
        }

        return ThriftIntType(bitWidth: bitWidth, isSigned: isSigned)
    }

    func readTimeUnit() throws -> ThriftTimeUnit? {
        var lastFieldId: Int16 = 0

        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            switch field.fieldId {
            case 1: // MILLIS
                _ = try readEmptyStruct()
                return .millis
            case 2: // MICROS
                _ = try readEmptyStruct()
                return .micros
            case 3: // NANOS
                _ = try readEmptyStruct()
                return .nanos
            default:
                try skipField(type: field.type)
            }
        }

        return nil
    }
}
