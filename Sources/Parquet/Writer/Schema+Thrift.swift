// Schema+Thrift.swift - Extensions for serializing Schema to Thrift format
//
// Licensed under the Apache License, Version 2.0

import Foundation

// MARK: - Schema Serialization

extension Schema {
    /// Convert schema to Thrift format for writing to file
    /// - Returns: Array of Thrift SchemaElement in depth-first order
    /// - Throws: WriterError if serialization fails
    func toThrift() throws -> [ThriftSchemaElement] {
        // Convert schema tree to flat list in depth-first order
        var elements: [ThriftSchemaElement] = []
        try flattenSchema(node: root, into: &elements)
        return elements
    }

    private func flattenSchema(node: SchemaElement, into elements: inout [ThriftSchemaElement]) throws {
        // Convert node to Thrift
        let thriftElement = try node.toThrift()
        elements.append(thriftElement)

        // Recurse into children
        for child in node.children {
            try flattenSchema(node: child, into: &elements)
        }
    }
}

// MARK: - SchemaElement Serialization

extension SchemaElement {
    /// Convert schema element to Thrift format
    /// - Returns: Thrift SchemaElement
    /// - Throws: WriterError if serialization fails
    func toThrift() throws -> ThriftSchemaElement {
        let thriftType = physicalType?.toThrift()
        let typeLengthValue: Int32?
        if case .fixedLenByteArray(let length) = physicalType {
            typeLengthValue = Int32(length)
        } else {
            typeLengthValue = nil
        }
        let thriftRepetition = repetitionType?.toThrift()
        let numChildren = isLeaf ? nil : Int32(children.count)

        var convertedType: ThriftConvertedType?
        var logicalTypeAnnotation: ThriftLogicalType?
        var precisionValue: Int32?
        var scaleValue: Int32?

        if let logicalType = logicalType {
            convertedType = logicalType.toConvertedType()
            logicalTypeAnnotation = logicalType.toThriftLogicalType()

            if case .decimal(let precision, let scale) = logicalType {
                precisionValue = Int32(precision)
                scaleValue = Int32(scale)
            }
        }

        return ThriftSchemaElement(
            type: thriftType,
            typeLength: typeLengthValue,
            repetitionType: thriftRepetition,
            name: name,
            numChildren: numChildren,
            convertedType: convertedType,
            scale: scaleValue,
            precision: precisionValue,
            fieldId: fieldId,
            logicalType: logicalTypeAnnotation
        )
    }
}

// MARK: - PhysicalType Serialization

extension PhysicalType {
    func toThrift() -> ThriftType {
        switch self {
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
            return .fixedLenByteArray
        }
    }
}

// MARK: - Repetition Serialization

extension Repetition {
    func toThrift() -> ThriftFieldRepetitionType {
        switch self {
        case .required:
            return .required
        case .optional:
            return .optional
        case .repeated:
            return .repeated
        }
    }
}

// MARK: - LogicalType Serialization

extension LogicalType {
    /// Convert to legacy ConvertedType for compatibility
    func toConvertedType() -> ThriftConvertedType? {
        switch self {
        case .string:
            return .utf8
        case .enum:
            return .enum
        case .decimal:
            return .decimal
        case .date:
            return .date
        case .time(_, let unit):
            switch unit {
            case .milliseconds:
                return .timeMillis
            case .microseconds:
                return .timeMicros
            case .nanoseconds:
                return nil
            }
        case .timestamp(_, let unit):
            switch unit {
            case .milliseconds:
                return .timestampMillis
            case .microseconds:
                return .timestampMicros
            case .nanoseconds:
                return nil
            }
        case .integer(let bitWidth, let isSigned):
            switch (bitWidth, isSigned) {
            case (8, true): return .int8
            case (16, true): return .int16
            case (32, true): return .int32
            case (64, true): return .int64
            case (8, false): return .uint8
            case (16, false): return .uint16
            case (32, false): return .uint32
            case (64, false): return .uint64
            default: return nil
            }
        case .json:
            return .json
        case .bson:
            return .bson
        case .uuid:
            return nil
        case .list:
            return .list
        case .map:
            return .map
        }
    }

    /// Convert to modern LogicalType format
    func toThriftLogicalType() -> ThriftLogicalType? {
        switch self {
        case .string:
            return .string
        case .enum:
            return .enum
        case .decimal(let precision, let scale):
            return .decimal(
                ThriftDecimalType(
                    scale: Int32(scale),
                    precision: Int32(precision)
                )
            )
        case .date:
            return .date
        case .time(let isAdjustedToUTC, let unit):
            return .time(
                ThriftTimeType(
                    isAdjustedToUTC: isAdjustedToUTC,
                    unit: unit.toThrift()
                )
            )
        case .timestamp(let isAdjustedToUTC, let unit):
            return .timestamp(
                ThriftTimestampType(
                    isAdjustedToUTC: isAdjustedToUTC,
                    unit: unit.toThrift()
                )
            )
        case .integer(let bitWidth, let isSigned):
            return .integer(
                ThriftIntType(bitWidth: Int8(bitWidth), isSigned: isSigned)
            )
        case .json:
            return .json
        case .bson:
            return .bson
        case .uuid:
            return .uuid
        case .list:
            return .list
        case .map:
            return .map
        }
    }
}

// MARK: - TimeUnit Serialization

extension TimeUnit {
    func toThrift() -> ThriftTimeUnit {
        switch self {
        case .milliseconds:
            return .millis
        case .microseconds:
            return .micros
        case .nanoseconds:
            return .nanos
        }
    }
}

// MARK: - Encoding Serialization

extension Encoding {
    func toThrift() -> ThriftEncoding {
        switch self {
        case .plain:
            return .plain
        case .plainDictionary:
            return .plainDictionary
        case .rle:
            return .rle
        case .bitPacked:
            return .bitPacked
        case .rleDictionary:
            return .rleDictionary
        case .deltaBinaryPacked:
            return .deltaBinaryPacked
        case .deltaLengthByteArray:
            return .deltaLengthByteArray
        case .deltaByteArray:
            return .deltaByteArray
        case .byteStreamSplit:
            return .byteStreamSplit
        }
    }
}

// MARK: - Compression Serialization

extension Compression {
    func toThrift() -> ThriftCompressionCodec {
        switch self {
        case .uncompressed:
            return .uncompressed
        case .snappy:
            return .snappy
        case .gzip:
            return .gzip
        case .lzo:
            return .lzo
        case .brotli:
            return .brotli
        case .lz4:
            return .lz4
        case .zstd:
            return .zstd
        case .lz4Raw:
            return .lz4Raw
        }
    }
}
