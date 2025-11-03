// StructValue - Lightweight view over struct data in Parquet
//
// Licensed under the Apache License, Version 2.0

/// Represents a struct value as a lightweight view over columnar data.
///
/// NULL Semantics (matches array pattern):
/// - `StructValue?` (outer optional) = NULL struct instance (struct not present)
/// - `fieldData["name"]` (inner optional) = NULL field value (field present but null)
///
/// # Example
///
/// ```swift
/// // Read struct column
/// let users = try rowGroup.readStruct(at: ["user"])
///
/// for (i, user) in users.enumerated() {
///     if let user = user {
///         // Struct is present (may have NULL fields)
///         let name = user.get("name", as: String.self)
///         let age = user.get("age", as: Int32.self)
///         print("Row \(i): \(name ?? "NULL"), age \(age ?? 0)")
///     } else {
///         // Struct is NULL
///         print("Row \(i): NULL struct")
///     }
/// }
/// ```
///
/// # NULL Cases
///
/// For schema: `optional group user { optional string name; optional int32 age; }`
///
/// - `defLevel = 0` → struct is NULL → array contains `nil`
/// - `defLevel = 1` → struct present, field NULL → `StructValue` with `field = nil`
/// - `defLevel = 2` → field has value → `StructValue` with `field = value`
public struct StructValue {
    /// Schema element for this struct (for introspection)
    public let element: SchemaElement

    /// Field data indexed by field name
    /// - Values are boxed as `Any?` to support heterogeneous types
    /// - `nil` = field is NULL (field present but value is null)
    /// - Missing key = field doesn't exist in schema (shouldn't happen)
    private let fieldData: [String: Any?]

    /// Create a struct value
    ///
    /// - Parameters:
    ///   - element: Schema element for this struct
    ///   - fieldData: Field name → value mapping
    internal init(element: SchemaElement, fieldData: [String: Any?]) {
        self.element = element
        self.fieldData = fieldData
    }

    /// Access field by name
    ///
    /// Returns `nil` if:
    /// - Field doesn't exist in schema
    /// - Field value is NULL
    ///
    /// # Example
    ///
    /// ```swift
    /// let name = user["name"] as? String
    /// let age = user["age"] as? Int32
    /// ```
    public subscript(field: String) -> Any? {
        // Return nil if field doesn't exist or is explicitly nil
        fieldData[field] ?? nil
    }

    /// Get typed field value
    ///
    /// - Parameters:
    ///   - field: Field name
    ///   - type: Expected type
    /// - Returns: Typed value if present and type matches, nil otherwise
    ///
    /// # Example
    ///
    /// ```swift
    /// if let name = user.get("name", as: String.self) {
    ///     print("Name: \(name)")
    /// } else {
    ///     print("Name is NULL or not a string")
    /// }
    /// ```
    public func get<T>(_ field: String, as type: T.Type) -> T? {
        guard let value = fieldData[field] else {
            return nil  // Field doesn't exist
        }

        // Handle nil values (NULL field)
        guard let nonNilValue = value else {
            return nil  // Field is NULL
        }

        return nonNilValue as? T
    }

    /// All field names in this struct (from schema)
    ///
    /// Useful for iterating over fields without prior knowledge of schema.
    ///
    /// # Example
    ///
    /// ```swift
    /// for field in user.fields {
    ///     print("\(field): \(user[field] ?? "NULL")")
    /// }
    /// ```
    public var fields: [String] {
        element.children.map { $0.name }
    }

    /// Number of fields in this struct
    public var fieldCount: Int {
        element.children.count
    }
}

// MARK: - CustomStringConvertible

extension StructValue: CustomStringConvertible {
    public var description: String {
        let fieldStrings = fields.map { field in
            let value = self[field]
            let valueStr = value.map { "\($0)" } ?? "NULL"
            return "\(field): \(valueStr)"
        }
        return "{\(fieldStrings.joined(separator: ", "))}"
    }
}

// MARK: - Equatable (for testing)

extension StructValue: Equatable {
    public static func == (lhs: StructValue, rhs: StructValue) -> Bool {
        // Elements must be the same struct type
        guard lhs.element.name == rhs.element.name else {
            return false
        }

        // All fields must match
        guard lhs.fields.count == rhs.fields.count else {
            return false
        }

        for field in lhs.fields {
            // Compare field values using description (handles Any?)
            let lhsValue = lhs[field].map { "\($0)" } ?? "nil"
            let rhsValue = rhs[field].map { "\($0)" } ?? "nil"

            if lhsValue != rhsValue {
                return false
            }
        }

        return true
    }
}
