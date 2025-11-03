#!/usr/bin/env python3
"""
Generate Parquet test fixtures for struct support

Usage:
    python3 generate_struct_fixtures.py

Generates:
    - struct_nullable.parquet: Struct with all NULL combinations
    - struct_simple.parquet: Simple struct without nulls
    - struct_nested.parquet: Nested struct (struct within struct)

Requirements:
    pip install pyarrow
"""

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent


def generate_struct_nullable():
    """
    Generate struct with all NULL combinations.

    Schema: optional group user { optional string name; optional int32 age; }

    Test cases:
    - Row 0: All fields present
    - Row 1: name is NULL
    - Row 2: age is NULL
    - Row 3: All fields NULL (struct present)
    - Row 4: Struct is NULL
    """
    print("Generating struct_nullable.parquet...")

    data = [
        {'id': 1, 'user': {'name': 'Alice', 'age': 30}},      # All fields present
        {'id': 2, 'user': {'name': None, 'age': 25}},          # name is NULL
        {'id': 3, 'user': {'name': 'Charlie', 'age': None}},   # age is NULL
        {'id': 4, 'user': {'name': None, 'age': None}},        # All fields NULL
        {'id': 5, 'user': None},                                # Struct is NULL
    ]

    table = pa.Table.from_pylist(data)

    output_path = SCRIPT_DIR / 'struct_nullable.parquet'
    pq.write_table(table, output_path)

    print(f"  Created: {output_path}")
    print(f"  Rows: {len(table)}")
    print(f"  Schema:\n{table.schema}")
    print()


def generate_struct_simple():
    """
    Generate simple struct without NULLs for basic testing.

    Schema: required group user { required string name; required int32 age; }
    """
    print("Generating struct_simple.parquet...")

    # Force required fields by using schema
    schema = pa.schema([
        ('id', pa.int32()),
        ('user', pa.struct([
            ('name', pa.string()),
            ('age', pa.int32()),
        ])),
    ])

    data = [
        {'id': 1, 'user': {'name': 'Alice', 'age': 30}},
        {'id': 2, 'user': {'name': 'Bob', 'age': 25}},
        {'id': 3, 'user': {'name': 'Charlie', 'age': 35}},
    ]

    table = pa.Table.from_pylist(data, schema=schema)

    output_path = SCRIPT_DIR / 'struct_simple.parquet'
    pq.write_table(table, output_path)

    print(f"  Created: {output_path}")
    print(f"  Rows: {len(table)}")
    print(f"  Schema:\n{table.schema}")
    print()


def generate_struct_nested():
    """
    Generate nested struct (struct within struct).

    Schema: optional group user {
              optional string name;
              optional group address {
                optional string city;
                optional string state;
              }
            }

    Test cases:
    - Row 0: All present
    - Row 1: user present, address NULL
    - Row 2: user present, address present, city NULL
    - Row 3: user NULL
    """
    print("Generating struct_nested.parquet...")

    data = [
        {'id': 1, 'user': {'name': 'Alice', 'address': {'city': 'NYC', 'state': 'NY'}}},
        {'id': 2, 'user': {'name': 'Bob', 'address': None}},
        {'id': 3, 'user': {'name': 'Charlie', 'address': {'city': None, 'state': 'CA'}}},
        {'id': 4, 'user': None},
    ]

    table = pa.Table.from_pylist(data)

    output_path = SCRIPT_DIR / 'struct_nested.parquet'
    pq.write_table(table, output_path)

    print(f"  Created: {output_path}")
    print(f"  Rows: {len(table)}")
    print(f"  Schema:\n{table.schema}")
    print()


def verify_fixture(filename: str):
    """Verify a generated fixture by reading it back."""
    filepath = SCRIPT_DIR / filename
    if not filepath.exists():
        print(f"  ⚠️  {filename} not found")
        return

    pf = pq.ParquetFile(filepath)

    print(f"Verifying {filename}:")
    print(f"  Rows: {pf.metadata.num_rows}")
    print(f"  Columns: {pf.metadata.num_columns}")

    # Show column paths
    print("  Column paths:")
    for i in range(pf.metadata.num_columns):
        col_meta = pf.metadata.row_group(0).column(i)
        print(f"    {i}: {col_meta.path_in_schema}")

    # Show Parquet schema
    print(f"  Parquet schema:\n{pf.schema}")
    print()


def main():
    print("=" * 60)
    print("Generating Parquet struct test fixtures")
    print("=" * 60)
    print()

    # Generate all fixtures
    generate_struct_nullable()
    generate_struct_simple()
    generate_struct_nested()

    # Verify all fixtures
    print("=" * 60)
    print("Verification")
    print("=" * 60)
    print()

    verify_fixture('struct_nullable.parquet')
    verify_fixture('struct_simple.parquet')
    verify_fixture('struct_nested.parquet')

    print("✅ All struct fixtures generated successfully!")


if __name__ == '__main__':
    main()
