# Struct NULL Semantics

**Date**: 2025-11-03
**Purpose**: Document how definition levels encode NULL structs vs NULL fields

---

## Overview

Parquet represents structs as group nodes where each field is a separate physical column.
NULL semantics follow the same pattern as arrays:
- **Outer optional** (`StructValue?`) = NULL struct instance
- **Inner optional** (`fieldData["name"]`) = NULL field value

This matches array semantics: `[[T?]?]` where outer=NULL list, inner=NULL element.

---

## Schema Example

```
optional group user {
  optional string name;
  optional int32 age;
}
```

**Max Definition Levels**:
- `user.name`: `maxDef = 2` (optional group + optional field)
- `user.age`: `maxDef = 2` (optional group + optional field)

**Definition Level Meanings**:
- `defLevel = 0`: Struct is NULL (not present)
- `defLevel = 1`: Struct is present, but field is NULL
- `defLevel = 2`: Field has a value

---

## NULL Case Examples

### Case 1: All Fields Present

**Data**: `{name: "Alice", age: 30}`

**Definition Levels**:
```
user.name: defLevel=2, value="Alice"
user.age:  defLevel=2, value=30
```

**Result**: `StructValue(fields: ["name": "Alice", "age": 30])`

---

### Case 2: Field is NULL (name)

**Data**: `{name: NULL, age: 25}`

**Definition Levels**:
```
user.name: defLevel=1, (no value)
user.age:  defLevel=2, value=25
```

**Result**: `StructValue(fields: ["name": nil, "age": 25])`

**Key Point**: Struct is present (`defLevel >= 1`), but field value is NULL (`defLevel < maxDef`).

---

### Case 3: Field is NULL (age)

**Data**: `{name: "Charlie", age: NULL}`

**Definition Levels**:
```
user.name: defLevel=2, value="Charlie"
user.age:  defLevel=1, (no value)
```

**Result**: `StructValue(fields: ["name": "Charlie", "age": nil])`

---

### Case 4: All Fields NULL (Struct Present)

**Data**: `{name: NULL, age: NULL}`

**Definition Levels**:
```
user.name: defLevel=1, (no value)
user.age:  defLevel=1, (no value)
```

**Result**: `StructValue(fields: ["name": nil, "age": nil])`

**Key Point**: Struct is present (all `defLevel >= 1`), but all field values are NULL.

---

### Case 5: Struct is NULL

**Data**: `NULL` (entire struct absent)

**Definition Levels**:
```
user.name: defLevel=0, (no value)
user.age:  defLevel=0, (no value)
```

**Result**: `nil` (not `StructValue`)

**Key Point**: ALL fields have `defLevel = 0` → struct is NULL.

---

## Detection Algorithm

To determine if a struct at row `i` is NULL or present:

```swift
// Read all field columns
let nameColumn = try rowGroup.stringColumn(at: nameIndex)
let ageColumn = try rowGroup.int32Column(at: ageIndex)

// For row i:
let nameDef = nameColumn.definitionLevels[i]
let ageDef = ageColumn.definitionLevels[i]

if nameDef == 0 && ageDef == 0 {
    // Struct is NULL
    result[i] = nil
} else {
    // Struct is present (may have NULL fields)
    var fieldData: [String: Any?] = [:]

    if nameDef >= maxDefinitionLevel {
        fieldData["name"] = nameColumn.values[nameValueIndex]
    } else if nameDef > 0 {
        fieldData["name"] = nil  // Field present but NULL
    }

    if ageDef >= maxDefinitionLevel {
        fieldData["age"] = ageColumn.values[ageValueIndex]
    } else if ageDef > 0 {
        fieldData["age"] = nil  // Field present but NULL
    }

    result[i] = StructValue(element: userElement, fieldData: fieldData)
}
```

---

## Comparison with Arrays

**Arrays** (`[[T?]?]`):
- Repetition level determines list boundaries (`rep=0` = new list)
- Definition level determines NULL list vs NULL element
- Empty list `[]` has `defLevel = ancestorDef` (list present, no elements)

**Structs** (`[StructValue?]`):
- No repetition levels (structs are not repeated)
- Definition level determines NULL struct vs NULL field
- Struct with all NULL fields still has `defLevel >= 1` (struct present)

**Key Similarity**: Both use outer optional for NULL instance, inner optional for NULL element/field.

---

## Nested Structs

For nested structs like:
```
optional group user {
  optional group address {
    optional string city;
  }
}
```

**Max Definition Levels**:
- `user.address.city`: `maxDef = 3` (3 optional levels)

**Definition Level Meanings**:
- `defLevel = 0`: `user` is NULL
- `defLevel = 1`: `user` present, `address` is NULL
- `defLevel = 2`: `address` present, `city` is NULL
- `defLevel = 3`: `city` has a value

**Algorithm**: Same as flat structs, but check definition level at each nesting level.

---

## Maps

Maps follow similar semantics but combine struct and array patterns:
```
optional group my_map (MAP) {
  repeated group key_value {
    required string key;
    optional int32 value;
  }
}
```

**Definition Levels**:
- `defLevel = 0`: Map is NULL
- `defLevel = 1`: Map is present (may be empty)
- `defLevel = 2`: Entry has NULL value

**Repetition Levels**:
- `rep = 0`: New map (start of row)
- `rep = 1`: Continuation of same map (next entry)

**Combined**: Maps use both definition levels (NULL semantics) and repetition levels (array boundaries).

---

## References

- Parquet Dremel Encoding: [https://github.com/apache/parquet-format/blob/master/Encodings.md](https://github.com/apache/parquet-format/blob/master/Encodings.md)
- Apache Arrow struct reading: `arrow/cpp/src/parquet/column_reader.cc`
- Array NULL semantics: `Sources/Parquet/Reader/ArrayReconstructor.swift`

---

**Next Steps**:
1. Implement `StructValue` type
2. Implement `StructReader` using this algorithm
3. Test with `struct_nullable.parquet` fixture
4. Extend to nested structs and maps

