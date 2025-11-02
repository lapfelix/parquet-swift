// Field repetition types
//
// Licensed under the Apache License, Version 2.0

/// Field repetition type in Parquet schema
///
/// Defines whether a field is required, optional, or repeated.
/// Corresponds to `FieldRepetitionType` in the Parquet Thrift spec.
///
/// # Repetition Types
///
/// - `required`: Field must have exactly one value (no nulls)
/// - `optional`: Field may have zero or one value (nullable)
/// - `repeated`: Field may have zero or more values (list/array)
///
/// # Definition and Repetition Levels
///
/// These types affect the definition and repetition levels in the data:
///
/// - **Required field**: Definition level = 0 (no nulls possible)
/// - **Optional field**: Definition level = 1 (can be null)
/// - **Repeated field**: Repetition level tracks list boundaries
///
/// # Usage
///
/// ```swift
/// let repetition = Repetition.optional
/// print(repetition.isNullable) // true
/// ```
public enum Repetition: String, Equatable, Hashable, Sendable, CaseIterable {
    /// Field must have exactly one value
    ///
    /// No nulls are allowed. Definition level = 0.
    case required = "REQUIRED"

    /// Field may have zero or one value
    ///
    /// Nulls are allowed. Definition level = 1 (or higher in nested structures).
    case optional = "OPTIONAL"

    /// Field may have zero or more values
    ///
    /// Represents arrays/lists. Uses repetition levels to track boundaries.
    case repeated = "REPEATED"

    /// Whether this field can be null
    public var isNullable: Bool {
        self == .optional
    }

    /// Whether this field represents a list/array
    public var isList: Bool {
        self == .repeated
    }

    /// Whether this field must have a value
    public var isRequired: Bool {
        self == .required
    }

    /// Maximum definition level contributed by this repetition type
    ///
    /// - Required: 0 (no null tracking needed)
    /// - Optional: 1 (track null vs. non-null)
    /// - Repeated: 1 (track empty vs. non-empty list)
    public var maxDefinitionLevel: Int {
        switch self {
        case .required:
            return 0
        case .optional, .repeated:
            return 1
        }
    }

    /// Maximum repetition level contributed by this repetition type
    ///
    /// - Required/Optional: 0
    /// - Repeated: 1 (track list boundaries)
    public var maxRepetitionLevel: Int {
        switch self {
        case .required, .optional:
            return 0
        case .repeated:
            return 1
        }
    }
}

extension Repetition: CustomStringConvertible {
    public var description: String {
        rawValue
    }
}

extension Repetition: CustomDebugStringConvertible {
    public var debugDescription: String {
        rawValue
    }
}
