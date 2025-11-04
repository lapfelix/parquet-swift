// MapValue - Lightweight representation of Parquet map data
//
// Licensed under the Apache License, Version 2.0

/// Represents a single key-value entry in a Parquet map.
///
/// Map entries use `Any` types to support heterogeneous key/value types.
/// The value can be `nil` to represent NULL values.
///
/// # Example
///
/// ```swift
/// let maps = try rowGroup.readMap(at: ["attributes"])
/// for (i, map) in maps.enumerated() {
///     if let map = map {
///         for entry in map {
///             print("  \(entry.key): \(entry.value ?? "NULL")")
///         }
///     } else {
///         print("Row \(i): NULL map")
///     }
/// }
/// ```
public struct MapEntry {
    /// The key (never nil - Parquet map keys are required)
    public let key: Any

    /// The value (nil = NULL value)
    public let value: Any?

    /// Create a map entry
    ///
    /// - Parameters:
    ///   - key: The key (must not be nil)
    ///   - value: The value (nil for NULL)
    internal init(key: Any, value: Any?) {
        self.key = key
        self.value = value
    }
}

// MARK: - CustomStringConvertible

extension MapEntry: CustomStringConvertible {
    public var description: String {
        let valueStr = value.map { "\($0)" } ?? "NULL"
        return "\(key): \(valueStr)"
    }
}

// MARK: - Equatable (for testing)

extension MapEntry: Equatable {
    public static func == (lhs: MapEntry, rhs: MapEntry) -> Bool {
        // Compare keys and values using string representation
        let lhsKey = "\(lhs.key)"
        let rhsKey = "\(rhs.key)"
        let lhsValue = lhs.value.map { "\($0)" } ?? "nil"
        let rhsValue = rhs.value.map { "\($0)" } ?? "nil"

        return lhsKey == rhsKey && lhsValue == rhsValue
    }
}
