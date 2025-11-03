# Struct and Map Support Design

**Date**: 2025-11-03
**Status**: Design Phase
**Goal**: Add support for reading structs and maps from Parquet files

---

## Background

### Current State
- ✅ Can read all primitive types (Int32, Int64, Float, Double, String)
- ✅ Can read single-level arrays (`maxRepetitionLevel == 1`)
- ✅ Can read multi-level nested lists (`maxRepetitionLevel > 1`)
- ❌ **Cannot read structs** (group nodes)
- ❌ **Cannot read maps** (repeated key-value pairs)

### Schema Representation

**Structs** in Parquet:
```
optional group user {
  optional string name;
  optional int32 age;
}
```

- Represented as `SchemaElement` with `ElementType.group(logicalType: nil)`
- Each field is a separate physical column (leaf node)
- No LIST or MAP logical type annotation

**Definition Levels** for struct fields:
- `defLevel = 0`: Struct itself is NULL
- `defLevel = 1`: Struct present, but this field is NULL
- `defLevel = 2`: Field is present and has a value

**Physical Columns**:
- Column 0: `user.name` (maxDef=2, maxRep=0)
- Column 1: `user.age` (maxDef=2, maxRep=0)

---

## Design Decisions

### 1. Struct Reading API

**Lightweight View Approach (CHOSEN)**

```swift
/// Represents a struct value as a lightweight view over columnar data
///
/// NULL Semantics:
/// - `StructValue?` (outer optional) = NULL struct instance (struct not present)
/// - `field["name"]` (inner optional) = NULL field value (field present but null)
/// - Empty StructValue with all nil fields = struct present, all fields null
///
/// This matches array semantics: [[T?]?] where outer=NULL list, inner=NULL element
public struct StructValue {
    /// Schema element for this struct
    internal let element: SchemaElement

    /// Field data indexed by field name (lazy, only materialized fields)
    /// Values are boxed as Any? to support heterogeneous types
    private var fieldData: [String: Any?]

    /// Access field by name
    ///
    /// Returns nil if:
    /// - Field doesn't exist in schema
    /// - Field value is NULL
    public subscript(field: String) -> Any? {
        fieldData[field] ?? nil
    }

    /// Get typed field value
    ///
    /// - Parameters:
    ///   - field: Field name
    ///   - type: Expected type
    /// - Returns: Typed value if present and type matches, nil otherwise
    public func get<T>(_ field: String, as type: T.Type) -> T? {
        fieldData[field] as? T
    }

    /// All field names in this struct (from schema)
    public var fields: [String] {
        element.children.map { $0.name }
    }

    /// Create a struct value
    internal init(element: SchemaElement, fieldData: [String: Any?]) {
        self.element = element
        self.fieldData = fieldData
    }
}

// RowGroupReader extension
extension RowGroupReader {
    /// Read all rows of a struct column
    ///
    /// - Parameter path: Path to the struct (e.g., ["user"])
    /// - Returns: Array where:
    ///   - `nil` = NULL struct (struct instance not present)
    ///   - `StructValue` = struct present (may have null fields)
    ///
    /// # Example
    ///
    /// ```swift
    /// let users = try rowGroup.readStruct(at: ["user"])
    /// for (i, user) in users.enumerated() {
    ///     if let user = user {
    ///         // Struct is present
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
    /// # NULL Semantics
    ///
    /// For schema: `optional group user { optional string name; optional int32 age; }`
    ///
    /// - `defLevel(name) = 0` → struct is NULL → return `nil` in array
    /// - `defLevel(name) = 1` → struct present, name is NULL → `StructValue` with `fieldData["name"] = nil`
    /// - `defLevel(name) = 2` → struct present, name has value → `StructValue` with `fieldData["name"] = value`
    ///
    /// The struct is considered NULL only if ALL fields have `defLevel = 0`.
    public func readStruct(at path: [String]) throws -> [StructValue?]
}
```

**Rationale**:
- ✅ **Lightweight** - Doesn't eagerly materialize all columns
- ✅ **Columnar** - Data stays in columnar format until accessed
- ✅ **Clear NULL semantics** - Matches array semantics (outer optional = NULL instance)
- ✅ **Type-safe** - Generic `get<T>` method for type-safe access
- ✅ **Flexible** - Works with any struct schema
- ✅ **Future-proof** - Can add lazy field loading later

---

### 2. Map Reading API

**Maps** in Parquet are represented as:
```
optional group my_map (MAP) {
  repeated group key_value {
    required string key;
    optional int32 value;
  }
}
```

- LogicalType: `.map`
- Structure: Outer group with MAP annotation → repeated group "key_value" → key + value fields
- The repeated group has `maxRepetitionLevel = 1`

**API Design**:

```swift
public typealias MapEntry = (key: Any, value: Any?)

extension RowGroupReader {
    /// Read all rows of a map column
    /// - Parameter path: Path to the map (e.g., ["my_map"])
    /// - Returns: Array of map dictionaries (nil for NULL maps)
    public func readMap(at path: [String]) throws -> [[MapEntry]?]
}
```

**Usage Example**:
```swift
let maps = try rowGroup.readMap(at: ["my_map"])

for map in maps {
    if let map = map {
        for (key, value) in map {
            print("\(key): \(value ?? "NULL")")
        }
    } else {
        print("NULL map")
    }
}
```

---

### 3. Implementation Strategy

#### Phase 1: Struct Support (Week 1)

**Step 1: Schema Analysis**
- Add `Schema.structFields(at:)` to find all fields of a struct
- Add validation to ensure path points to a group node

**Step 2: StructReader Implementation**
- Create `StructReader` class similar to column readers
- Read all child columns
- Align values based on definition levels
- Construct `StructValue` for each row

**Step 3: RowGroupReader Integration**
- Add `readStruct(at:)` method to RowGroupReader
- Handle NULL structs vs NULL fields

#### Phase 2: Map Support (Week 2)

**Step 1: Identify Map Schema**
- Detect MAP logical type on outer group
- Validate map structure (repeated key_value group with key + value fields)

**Step 2: MapReader Implementation**
- Read key and value columns
- Group by repetition levels (similar to array reconstruction)
- Return as array of dictionaries

**Step 3: RowGroupReader Integration**
- Add `readMap(at:)` method

#### Phase 3: Lists of Structs (Week 2-3)

**Combined Reading**:
- Detect `repeated group` without MAP annotation
- This is a list of structs
- Combine array reconstruction with struct reading

```swift
extension RowGroupReader {
    /// Read list of structs
    public func readListOfStructs(at path: [String]) throws -> [[StructValue?]?]
}
```

---

## Extensibility Design

### Protocol Consideration

**Question**: Should we introduce a `StructRow` protocol for sharing machinery between structs, maps, and nested types?

**Decision**: Start with concrete `StructValue` type, defer protocol until needed.

**Rationale**:
- ✅ **YAGNI**: We don't need the abstraction yet
- ✅ **Simpler API**: `[StructValue?]` is clearer than `[any StructRow]`
- ✅ **Swift limitations**: `any StructRow` has performance implications
- ✅ **Future-proof**: Can introduce protocol later without breaking API

**Future Protocol** (when maps/nested types need it):
```swift
protocol StructRow {
    var element: SchemaElement { get }
    subscript(field: String) -> Any? { get }
}

extension StructValue: StructRow { }
extension MapValue: StructRow { }  // Future
```

**For now**: Keep it simple with `StructValue`. Add protocol if/when maps need shared machinery.

---

## Definition Level Handling

### Struct Fields

For a struct field like `user.name`:
- `defLevel = 0`: `user` is NULL → struct itself is NULL, don't add field
- `defLevel = 1`: `user` present, `name` is NULL → add `"name": nil` to fields
- `defLevel = 2`: `name` is present → add `"name": value` to fields

**Algorithm**:
1. Read all child columns with their definition levels
2. For each row:
   - If ALL child columns have `defLevel = 0` → struct is NULL
   - Otherwise, create StructValue with fields based on defLevels

### Map Entries

For a map entry:
- Repetition level determines list boundaries (rep=0 = new map)
- Definition level determines NULL maps vs NULL values

**Algorithm**:
1. Read key and value columns with def/rep levels
2. Group entries by repetition level (similar to array reconstruction)
3. Build map dictionaries per row

---

## Test Cases

### Structs
1. Simple struct with required fields
2. Struct with optional fields (NULL field values)
3. Optional struct (NULL struct instances)
4. Nested struct (struct within struct)
5. Struct with all primitive types

### Maps
1. Simple map (string → int32)
2. Map with NULL values
3. Map with NULL map instances
4. Empty maps vs NULL maps
5. Maps with complex value types

### Lists of Structs
1. List of simple structs
2. List with NULL struct elements
3. NULL lists vs empty lists of structs

---

## API Summary

```swift
// Core types
public struct StructValue {
    public let fields: [String: Any?]
    public subscript(field: String) -> Any?
    public func get<T>(_ field: String, as type: T.Type) -> T?
}

public typealias MapEntry = (key: Any, value: Any?)

// RowGroupReader extensions
extension RowGroupReader {
    // Read struct column
    public func readStruct(at path: [String]) throws -> [StructValue?]

    // Read map column
    public func readMap(at path: [String]) throws -> [[MapEntry]?]

    // Read list of structs
    public func readListOfStructs(at path: [String]) throws -> [[StructValue?]?]
}

// Schema helpers
extension Schema {
    // Find all fields of a struct
    public func structFields(at path: [String]) -> [Column]?

    // Check if path points to a struct
    public func isStruct(at path: [String]) -> Bool

    // Check if path points to a map
    public func isMap(at path: [String]) -> Bool
}
```

---

## Success Criteria

- ✅ Can read simple struct columns
- ✅ Can read map columns
- ✅ Can read lists of structs
- ✅ Handles NULL structs, NULL fields, NULL maps correctly
- ✅ Comprehensive test coverage
- ✅ Clear API documentation
- ✅ Compatible with PyArrow-generated files

---

## Next Steps

1. Implement `Schema.structFields(at:)` helper
2. Create `StructReader` class
3. Add `RowGroupReader.readStruct(at:)` method
4. Create test fixtures with PyArrow
5. Add comprehensive test coverage

