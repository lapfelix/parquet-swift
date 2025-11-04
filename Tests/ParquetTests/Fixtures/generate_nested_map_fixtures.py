#!/usr/bin/env python3
"""
Generate Parquet test fixtures for NESTED map columns.

These fixtures expose bugs in multi-level repetition and definition level handling:
1. list<map<k,v>> - Multi-level repetition (repLevel=0,1,2)
2. map<k,list<v>> - Lists as map values
3. optional struct { optional map } - Maps in optional parents

Each fixture is designed to expose specific bugs in the current implementation.
"""

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

# Output directory
FIXTURES_DIR = Path(__file__).parent
print(f"Generating nested map fixtures in {FIXTURES_DIR}")


def generate_list_of_maps():
    """
    list<map<string, int64>>

    This exposes the multi-level repetition bug:
    - repLevel = 0: new row (new outer list)
    - repLevel = 1: new list element (new map)
    - repLevel = 2: continuation of map (new key-value pair)

    Current implementation treats repLevel < 2 as "start new list",
    so repLevel=1 incorrectly starts a new row instead of a new map.

    Data:
    - Row 0: [{"a": 1, "b": 2}, {"x": 10}]         # List with 2 maps
    - Row 1: [{"foo": 100}]                         # List with 1 map
    - Row 2: []                                      # Empty list
    - Row 3: None                                    # NULL list
    - Row 4: [{"k": None}]                          # Map with NULL value
    """
    data = [
        # Row 0: List with 2 maps, each map has multiple entries
        [
            [("a", 1), ("b", 2)],      # First map: {"a": 1, "b": 2}
            [("x", 10)]                 # Second map: {"x": 10}
        ],
        # Row 1: List with 1 map
        [
            [("foo", 100)]
        ],
        # Row 2: Empty list
        [],
        # Row 3: NULL list
        None,
        # Row 4: List with map containing NULL value
        [
            [("k", None)]
        ]
    ]

    # Create list of maps type
    map_type = pa.map_(pa.string(), pa.int64())
    list_type = pa.list_(map_type)

    array = pa.array(data, type=list_type)

    table = pa.table({
        'id': pa.array([0, 1, 2, 3, 4], type=pa.int32()),
        'list_of_maps': array
    })

    output_path = FIXTURES_DIR / "nested_list_of_maps.parquet"
    pq.write_table(table, output_path)
    print(f"✓ Generated {output_path.name}")

    # Print schema for verification
    parquet_file = pq.ParquetFile(output_path)
    print(f"  Schema: {parquet_file.schema_arrow}")
    print(f"  Parquet schema:\n{parquet_file.schema}")
    print()


def generate_map_with_list_values():
    """
    map<string, list<int64>>

    This tests lists as map values.

    Data:
    - Row 0: {"nums": [1, 2, 3], "evens": [2, 4]}   # Map with list values
    - Row 1: {"empty": []}                          # Map with empty list value
    - Row 2: {"nulls": None}                        # Map with NULL list value
    - Row 3: {}                                      # Empty map
    - Row 4: None                                    # NULL map
    """
    # PyArrow represents map<k, list<v>> as list of (key, value) tuples
    # where value is a list
    data = [
        # Row 0: Map with list values
        [
            ("nums", [1, 2, 3]),
            ("evens", [2, 4])
        ],
        # Row 1: Map with empty list value
        [
            ("empty", [])
        ],
        # Row 2: Map with NULL list value
        [
            ("nulls", None)
        ],
        # Row 3: Empty map
        [],
        # Row 4: NULL map
        None
    ]

    # Create map<string, list<int64>> type
    list_type = pa.list_(pa.int64())
    map_type = pa.map_(pa.string(), list_type)

    array = pa.array(data, type=map_type)

    table = pa.table({
        'id': pa.array([0, 1, 2, 3, 4], type=pa.int32()),
        'map_of_lists': array
    })

    output_path = FIXTURES_DIR / "nested_map_with_lists.parquet"
    pq.write_table(table, output_path)
    print(f"✓ Generated {output_path.name}")

    # Print schema for verification
    parquet_file = pq.ParquetFile(output_path)
    print(f"  Schema: {parquet_file.schema_arrow}")
    print(f"  Parquet schema:\n{parquet_file.schema}")
    print()


def generate_struct_with_map():
    """
    struct { optional map<string, int64> }

    This exposes the definition level bug for maps in optional parents.

    When the struct is optional and contains an optional map:
    - defLevel = 0: struct is NULL
    - defLevel = 1: struct present, map is NULL
    - defLevel = 2: map present (may be empty)
    - defLevel = 3+: map entries present

    Current implementation uses maxDef-1 heuristic, which breaks when
    the map is inside an optional struct.

    Data:
    - Row 0: {user: {"name": "Alice"}}              # Struct and map present
    - Row 1: {user: {}}                             # Struct present, empty map
    - Row 2: {user: None}                           # Struct present, NULL map
    - Row 3: None                                   # NULL struct
    - Row 4: {user: {"key": None}}                  # Map with NULL value
    """
    # Create struct type with optional map field
    map_type = pa.map_(pa.string(), pa.int64())
    struct_type = pa.struct([
        pa.field('attributes', map_type, nullable=True)
    ])

    data = [
        # Row 0: Struct and map present
        {
            'attributes': [("name", 1), ("age", 30)]
        },
        # Row 1: Struct present, empty map
        {
            'attributes': []
        },
        # Row 2: Struct present, NULL map
        {
            'attributes': None
        },
        # Row 3: NULL struct
        None,
        # Row 4: Map with NULL value
        {
            'attributes': [("key", None)]
        }
    ]

    array = pa.array(data, type=struct_type)

    table = pa.table({
        'id': pa.array([0, 1, 2, 3, 4], type=pa.int32()),
        'user': array
    })

    output_path = FIXTURES_DIR / "nested_struct_with_map.parquet"
    pq.write_table(table, output_path)
    print(f"✓ Generated {output_path.name}")

    # Print schema for verification
    parquet_file = pq.ParquetFile(output_path)
    print(f"  Schema: {parquet_file.schema_arrow}")
    print(f"  Parquet schema:\n{parquet_file.schema}")
    print()


def generate_deep_nesting():
    """
    list<struct<name: string, scores: map<string, int64>>>

    This is a complex nested structure combining all elements:
    - Outer list (repLevel 1)
    - Struct in list
    - Map in struct (repLevel 2 for map entries)

    Data:
    - Row 0: [{name: "Alice", scores: {"math": 90, "eng": 85}}]
    - Row 1: [{name: "Bob", scores: {}}]           # Empty map
    - Row 2: [{name: "Charlie", scores: None}]      # NULL map
    - Row 3: []                                     # Empty list
    - Row 4: None                                   # NULL list
    """
    map_type = pa.map_(pa.string(), pa.int64())
    struct_type = pa.struct([
        pa.field('name', pa.string()),
        pa.field('scores', map_type, nullable=True)
    ])
    list_type = pa.list_(struct_type)

    data = [
        # Row 0: Struct with map
        [
            {
                'name': 'Alice',
                'scores': [("math", 90), ("eng", 85)]
            }
        ],
        # Row 1: Struct with empty map
        [
            {
                'name': 'Bob',
                'scores': []
            }
        ],
        # Row 2: Struct with NULL map
        [
            {
                'name': 'Charlie',
                'scores': None
            }
        ],
        # Row 3: Empty list
        [],
        # Row 4: NULL list
        None
    ]

    array = pa.array(data, type=list_type)

    table = pa.table({
        'id': pa.array([0, 1, 2, 3, 4], type=pa.int32()),
        'students': array
    })

    output_path = FIXTURES_DIR / "nested_deep.parquet"
    pq.write_table(table, output_path)
    print(f"✓ Generated {output_path.name}")

    # Print schema for verification
    parquet_file = pq.ParquetFile(output_path)
    print(f"  Schema: {parquet_file.schema_arrow}")
    print(f"  Parquet schema:\n{parquet_file.schema}")
    print()


def main():
    print("=" * 60)
    print("Generating Nested Map Test Fixtures")
    print("=" * 60)
    print()

    print("1. list<map<string, int64>> - Multi-level repetition")
    generate_list_of_maps()

    print("2. map<string, list<int64>> - Lists as map values")
    generate_map_with_list_values()

    print("3. struct with optional map - Maps in optional parents")
    generate_struct_with_map()

    print("4. list<struct<map>> - Deep nesting")
    generate_deep_nesting()

    print("=" * 60)
    print("Nested map fixture generation complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
