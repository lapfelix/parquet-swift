// ThriftReader extensions for FileMetaData deserialization
//
// Licensed under the Apache License, Version 2.0

import Foundation

extension ThriftReader {
    /// Reads FileMetaData from the current position.
    public func readFileMetaData() throws -> ThriftFileMetaData {
        var version: Int32?
        var schema: [ThriftSchemaElement]?
        var numRows: Int64?
        var rowGroups: [ThriftRowGroup]?
        var keyValueMetadata: [ThriftKeyValue]?
        var createdBy: String?
        var columnOrders: [ThriftColumnOrder]?

        var lastFieldId: Int16 = 0

        while true {
            do {
                guard let field = try readFieldHeader(lastFieldId: &lastFieldId) else {
                    break // STOP field
                }

                switch field.fieldId {
                case 1: // version
                    version = try readVarint32()
                case 2: // schema
                    let (elementType, count) = try readListHeader()
                    guard elementType == .struct else {
                        throw ThriftError.protocolError("Expected struct list for schema")
                    }
                    var elements: [ThriftSchemaElement] = []
                    for _ in 0..<count {
                        elements.append(try readSchemaElement())
                    }
                    schema = elements
                case 3: // num_rows
                    numRows = try readVarint()
                case 4: // row_groups
                    let (elementType, count) = try readListHeader()
                    guard elementType == .struct else {
                        throw ThriftError.protocolError("Expected struct list for row_groups")
                    }
                    var groups: [ThriftRowGroup] = []
                    for _ in 0..<count {
                        groups.append(try readRowGroup())
                    }
                    rowGroups = groups
                case 5: // key_value_metadata
                    let (elementType, count) = try readListHeader()
                    guard elementType == .struct else {
                        throw ThriftError.protocolError("Expected struct list for key_value_metadata")
                    }
                    var kvs: [ThriftKeyValue] = []
                    for _ in 0..<count {
                        kvs.append(try readKeyValue())
                    }
                    keyValueMetadata = kvs
                case 6: // created_by
                    createdBy = try readString()
                case 7: // column_orders
                    let (elementType, count) = try readListHeader()
                    guard elementType == .struct else {
                        throw ThriftError.protocolError("Expected struct list for column_orders")
                    }
                    var orders: [ThriftColumnOrder] = []
                    for _ in 0..<count {
                        _ = try readColumnOrder()
                        orders.append(.typeOrder) // Only TypeOrder is currently defined
                    }
                    columnOrders = orders
                default:
                    // Skip unknown fields
                    try skipField(type: field.type)
                }
            } catch ThriftError.unsupportedType {
                // Skip fields with unsupported types (forward compatibility)
                // This can happen with newer Parquet versions
                continue
            }
        }

        // Validate required fields
        guard let version = version,
              let schema = schema,
              let numRows = numRows,
              let rowGroups = rowGroups else {
            throw ThriftError.invalidData("Missing required fields in FileMetaData")
        }

        return ThriftFileMetaData(
            version: version,
            schema: schema,
            numRows: numRows,
            rowGroups: rowGroups,
            keyValueMetadata: keyValueMetadata,
            createdBy: createdBy,
            columnOrders: columnOrders
        )
    }

    // MARK: - Supporting Struct Readers

    func readKeyValue() throws -> ThriftKeyValue {
        var key: String?
        var value: String?
        var lastFieldId: Int16 = 0

        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            switch field.fieldId {
            case 1: // key
                key = try readString()
            case 2: // value
                value = try readString()
            default:
                try skipField(type: field.type)
            }
        }

        guard let key = key else {
            throw ThriftError.invalidData("Missing required field 'key' in KeyValue")
        }

        return ThriftKeyValue(key: key, value: value)
    }

    func readColumnOrder() throws {
        var lastFieldId: Int16 = 0
        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            // Currently only TypeDefinedOrder exists, which is an empty struct
            try skipField(type: field.type)
        }
    }
}
