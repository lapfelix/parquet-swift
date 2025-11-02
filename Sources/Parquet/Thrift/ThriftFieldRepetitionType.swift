// Thrift FieldRepetitionType enum - Field repetition types in Parquet format
//
// Licensed under the Apache License, Version 2.0

/// Representation of field repetition in schema.
///
/// Maps directly to the Thrift `FieldRepetitionType` enum.
public enum ThriftFieldRepetitionType: Int32, Sendable {
    /// This field is required (cannot be null) and each row has exactly 1 value
    case required = 0

    /// The field is optional (can be null) and each row has 0 or 1 values
    case optional = 1

    /// The field is repeated and can contain 0 or more values
    case repeated = 2

    public var name: String {
        switch self {
        case .required: return "REQUIRED"
        case .optional: return "OPTIONAL"
        case .repeated: return "REPEATED"
        }
    }
}
