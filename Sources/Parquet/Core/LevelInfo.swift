// LevelInfo - Level metadata for nested type reconstruction
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Metadata about definition and repetition levels for reconstructing nested structures.
///
/// This structure encapsulates the level information needed to correctly reconstruct
/// Parquet nested types (lists, maps, structs) from flat value sequences with
/// definition and repetition levels.
///
/// Based on Apache Arrow C++ `LevelInfo` structure from `level_conversion.h`.
///
/// # Background
///
/// Parquet uses definition and repetition levels to encode nested structures:
/// - **Definition level**: Indicates how deeply a value is defined (present vs NULL)
/// - **Repetition level**: Indicates which repeated field is being repeated
/// - **Repeated ancestor def level**: Critical threshold for filtering excluded values
///
/// # Example
///
/// For schema `list(struct(f0: int))`:
/// ```
/// Definition levels:
///   0 = null list                      ← excluded
///   1 = present but empty list         ← repeated_ancestor_def_level for struct/int
///   2 = list with null struct          ← null value
///   3 = non-null struct but null int   ← null value
///   4 = present integer                ← present value
/// ```
///
/// Data: `[null, [], [null], [{f0: null}], [{f0: 1}]]`
/// Def levels: `[0, 1, 2, 3, 4]`
///
/// LevelInfo for the int column:
/// ```swift
/// LevelInfo(
///     defLevel: 4,                     // Max def level (value present)
///     repLevel: 1,                     // List's repetition level
///     repeatedAncestorDefLevel: 2      // List present, has content
/// )
/// ```
///
/// Reconstruction uses this to filter:
/// - `defLevel < 2` → skip (belongs to null/empty ancestor list)
/// - `defLevel >= 2` → include in struct/int arrays
///
public struct LevelInfo {
    /// Definition level at which the field is considered not-null.
    ///
    /// For primitive fields: the level at which the value is present (not NULL).
    /// For lists: the level at which a list element is present (possibly NULL).
    ///
    /// This is typically equal to `maxDefinitionLevel` from the column descriptor.
    public let defLevel: Int

    /// Repetition level corresponding to this element or closest repeated ancestor.
    ///
    /// Any repetition level less than this indicates either:
    /// - A new list at a higher nesting level
    /// - An empty list (determined in conjunction with definition levels)
    ///
    /// This is typically equal to `maxRepetitionLevel` from the column descriptor.
    public let repLevel: Int

    /// Definition level indicating the closest repeated ancestor is not empty.
    ///
    /// This is the **critical threshold** for filtering values:
    /// - `defLevel < repeatedAncestorDefLevel` → value is **excluded** (belongs to NULL/empty ancestor)
    /// - `defLevel >= repeatedAncestorDefLevel` → value is **included** in reconstruction
    ///
    /// For example, in `list(struct(f0: int))`:
    /// - `repeatedAncestorDefLevel = 2` for the struct and int
    /// - When `defLevel = 0` (null list) or `defLevel = 1` (empty list), skip the value
    /// - When `defLevel >= 2`, include in struct/int arrays
    ///
    /// This field is crucial for multi-level reconstruction to avoid including values
    /// from NULL or empty ancestor lists.
    public let repeatedAncestorDefLevel: Int

    /// Creates level information for a column.
    ///
    /// - Parameters:
    ///   - defLevel: Maximum definition level (when value is not NULL)
    ///   - repLevel: Maximum repetition level (for repeated fields)
    ///   - repeatedAncestorDefLevel: Definition level when repeated ancestor is non-empty
    public init(defLevel: Int, repLevel: Int, repeatedAncestorDefLevel: Int) {
        self.defLevel = defLevel
        self.repLevel = repLevel
        self.repeatedAncestorDefLevel = repeatedAncestorDefLevel
    }

    /// Checks if this field can have nullable values.
    ///
    /// A field has nullable values when there are definition levels between the
    /// repeated ancestor and the maximum (i.e., the value can be NULL while still
    /// being part of a present list).
    ///
    /// - Returns: `true` if `repeatedAncestorDefLevel < defLevel`, `false` otherwise
    public var hasNullableValues: Bool {
        return repeatedAncestorDefLevel < defLevel
    }
}

// MARK: - Equatable

extension LevelInfo: Equatable {
    public static func == (lhs: LevelInfo, rhs: LevelInfo) -> Bool {
        return lhs.defLevel == rhs.defLevel &&
               lhs.repLevel == rhs.repLevel &&
               lhs.repeatedAncestorDefLevel == rhs.repeatedAncestorDefLevel
    }
}

// MARK: - CustomStringConvertible

extension LevelInfo: CustomStringConvertible {
    public var description: String {
        return "LevelInfo(def=\(defLevel), rep=\(repLevel), repeated_ancestor_def=\(repeatedAncestorDefLevel))"
    }
}

// MARK: - Helper Methods

extension LevelInfo {
    /// Creates LevelInfo for a column by matching Arrow C++'s ComputeLevelInfo logic.
    ///
    /// This replicates Arrow's approach:
    /// - `def_level` = column's max definition level (threshold for non-null value)
    /// - `rep_level` = column's max repetition level
    /// - `repeated_ancestor_def_level` = definition level of the innermost repeated ancestor
    ///
    /// **Multi-level repetition**: For columns with `maxRepetitionLevel > 1` (e.g., list<list<int>>),
    /// we use `column.repeatedAncestorDefLevels[repLevel - 1]` to get the def level of the
    /// repeated ancestor corresponding to this repetition level. For single-level repetition
    /// (`maxRepetitionLevel == 1`), we can use the singular `column.repeatedAncestorDefLevel`.
    ///
    /// For columns without repetition (flat columns), this returns nil because LevelInfo
    /// is only needed for reconstructing repeated structures.
    ///
    /// - Parameter column: The column to create LevelInfo for
    /// - Returns: LevelInfo if column has repetition, nil otherwise
    public static func from(column: Column) -> LevelInfo? {
        // Only create LevelInfo for columns with repetition
        guard column.maxRepetitionLevel > 0 else {
            return nil
        }

        let repeatedAncestorDefLevel: Int

        if column.maxRepetitionLevel == 1 {
            // Single-level repetition: use the singular property
            guard let defLevel = column.repeatedAncestorDefLevel else {
                fatalError("Column with maxRepetitionLevel=1 has nil repeatedAncestorDefLevel")
            }
            repeatedAncestorDefLevel = defLevel
        } else {
            // Multi-level repetition: use the plural array indexed by rep level
            // For maxRepetitionLevel = 2, we want index 1 (innermost repeated ancestor)
            // In general: index = maxRepetitionLevel - 1
            guard let defLevels = column.repeatedAncestorDefLevels else {
                fatalError("Column with maxRepetitionLevel=\(column.maxRepetitionLevel) has nil repeatedAncestorDefLevels")
            }

            let index = column.maxRepetitionLevel - 1
            guard index < defLevels.count else {
                fatalError("repeatedAncestorDefLevels has \(defLevels.count) entries but need index \(index) for maxRepetitionLevel=\(column.maxRepetitionLevel)")
            }

            repeatedAncestorDefLevel = defLevels[index]
        }

        return LevelInfo(
            defLevel: column.maxDefinitionLevel,
            repLevel: column.maxRepetitionLevel,
            repeatedAncestorDefLevel: repeatedAncestorDefLevel
        )
    }
}
