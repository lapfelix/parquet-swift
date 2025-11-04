#!/usr/bin/env python3
"""
Generate Parquet test fixtures for map columns.

Maps in Parquet are represented as:
  optional group map_field (MAP) {
    repeated group key_value {
      required/optional key;
      required/optional value;
    }
  }

This script generates fixtures with various map scenarios:
1. Simple maps (no NULLs)
2. Maps with NULL values
3. NULL maps vs empty maps
4. Different key/value types
"""

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

# Output directory
FIXTURES_DIR = Path(__file__).parent
print(f"Generating map fixtures in {FIXTURES_DIR}")


def generate_map_simple():
    """
    Simple map without NULLs.
    Schema: map<string, int64>

    Data:
    - Row 0: {"a": 1, "b": 2}
    - Row 1: {"x": 10, "y": 20, "z": 30}
    - Row 2: {"foo": 100}
    """
    # PyArrow maps are represented as list of (key, value) tuples
    data = [
        [("a", 1), ("b", 2)],
        [("x", 10), ("y", 20), ("z", 30)],
        [("foo", 100)]
    ]

    # Create array with map type
    map_array = pa.array(data, type=pa.map_(pa.string(), pa.int64()))

    # Create table
    table = pa.table({
        'id': pa.array([0, 1, 2], type=pa.int32()),
        'attributes': map_array
    })

    output_path = FIXTURES_DIR / "map_simple.parquet"
    pq.write_table(table, output_path)
    print(f"✓ Generated {output_path.name}")

    # Print schema for verification
    parquet_file = pq.ParquetFile(output_path)
    print(f"  Schema: {parquet_file.schema_arrow}")
    print(f"  Parquet schema:\n{parquet_file.schema}")


def generate_map_nullable():
    """
    Map with all NULL combinations.
    Schema: map<string, int64>

    Data:
    - Row 0: {"a": 1, "b": 2}           # All present
    - Row 1: {"x": 10, "y": None}       # Map present, one NULL value
    - Row 2: {}                         # Empty map
    - Row 3: None                       # NULL map
    - Row 4: {"k": None}                # Map with only NULL values
    """
    # PyArrow uses list of tuples for maps
    data = [
        [("a", 1), ("b", 2)],               # Row 0: all present
        [("x", 10), ("y", None)],           # Row 1: NULL value
        [],                                  # Row 2: empty map
        None,                                # Row 3: NULL map
        [("k", None)],                       # Row 4: only NULL values
    ]

    # Create array with map type
    map_array = pa.array(data, type=pa.map_(pa.string(), pa.int64()))

    # Create table
    table = pa.table({
        'id': pa.array([0, 1, 2, 3, 4], type=pa.int32()),
        'attributes': map_array
    })

    output_path = FIXTURES_DIR / "map_nullable.parquet"
    pq.write_table(table, output_path)
    print(f"✓ Generated {output_path.name}")

    # Print schema for verification
    parquet_file = pq.ParquetFile(output_path)
    print(f"  Schema: {parquet_file.schema_arrow}")
    print(f"  Parquet schema:\n{parquet_file.schema}")


def generate_map_string_values():
    """
    Map with string values.
    Schema: map<string, string>

    Data:
    - Row 0: {"name": "Alice", "city": "NYC"}
    - Row 1: {"lang": "Swift", "framework": "SwiftUI"}
    - Row 2: {"key": None}
    """
    data = [
        [("name", "Alice"), ("city", "NYC")],
        [("lang", "Swift"), ("framework", "SwiftUI")],
        [("key", None)],
    ]

    # Create array with map type
    map_array = pa.array(data, type=pa.map_(pa.string(), pa.string()))

    # Create table
    table = pa.table({
        'id': pa.array([0, 1, 2], type=pa.int32()),
        'metadata': map_array
    })

    output_path = FIXTURES_DIR / "map_string_values.parquet"
    pq.write_table(table, output_path)
    print(f"✓ Generated {output_path.name}")


def generate_map_int_keys():
    """
    Map with integer keys.
    Schema: map<int32, string>

    Data:
    - Row 0: {1: "one", 2: "two", 3: "three"}
    - Row 1: {100: "hundred"}
    - Row 2: {}
    """
    data = [
        [(1, "one"), (2, "two"), (3, "three")],
        [(100, "hundred")],
        [],
    ]

    # Create map array with explicit type
    map_array = pa.array(data, type=pa.map_(pa.int32(), pa.string()))

    table = pa.table({
        'id': pa.array([0, 1, 2], type=pa.int32()),
        'lookup': map_array
    })

    output_path = FIXTURES_DIR / "map_int_keys.parquet"
    pq.write_table(table, output_path)
    print(f"✓ Generated {output_path.name}")


def main():
    print("=" * 60)
    print("Generating Map Test Fixtures")
    print("=" * 60)
    print()

    generate_map_simple()
    print()

    generate_map_nullable()
    print()

    generate_map_string_values()
    print()

    generate_map_int_keys()
    print()

    print("=" * 60)
    print("Map fixture generation complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
