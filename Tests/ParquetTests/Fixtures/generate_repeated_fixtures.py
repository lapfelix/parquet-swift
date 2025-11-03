#!/usr/bin/env python3
"""
Generate Parquet test fixtures with repeated columns for array reconstruction tests.

This script creates multiple test files covering various repeated column scenarios:
- Simple repeated int32 with non-empty lists
- Repeated with empty lists
- Repeated with null elements
- Mixed scenarios
- Different types (int64, float, double, string)
"""

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

def main():
    # Output directory
    output_dir = Path(__file__).parent
    output_dir.mkdir(exist_ok=True)

    print(f"Generating repeated column test fixtures...")
    print(f"Output directory: {output_dir}")
    print(f"PyArrow version: {pa.__version__}")
    print()

    # Test 1: Simple repeated int32 - [[1, 2], [3], [4, 5, 6]]
    print("1. Generating repeated_int32_simple.parquet")
    data1 = [[1, 2], [3], [4, 5, 6]]
    schema1 = pa.schema([
        pa.field('numbers', pa.list_(pa.int32()))
    ])
    table1 = pa.table({'numbers': data1}, schema=schema1)
    pq.write_table(table1, output_dir / 'repeated_int32_simple.parquet')
    print(f"   Rows: {len(data1)}")
    print(f"   Data: {data1}")
    print()

    # Test 2: Repeated int32 with empty lists - [[1, 2], [], [3]]
    print("2. Generating repeated_int32_empty.parquet")
    data2 = [[1, 2], [], [3]]
    schema2 = pa.schema([
        pa.field('numbers', pa.list_(pa.int32()))
    ])
    table2 = pa.table({'numbers': data2}, schema=schema2)
    pq.write_table(table2, output_dir / 'repeated_int32_empty.parquet')
    print(f"   Rows: {len(data2)}")
    print(f"   Data: {data2}")
    print()

    # Test 3: Repeated int32 with nulls - [[1, None, 2], [None], [3, 4]]
    print("3. Generating repeated_int32_nulls.parquet")
    data3 = [[1, None, 2], [None], [3, 4]]
    schema3 = pa.schema([
        pa.field('numbers', pa.list_(pa.int32()))
    ])
    table3 = pa.table({'numbers': data3}, schema=schema3)
    pq.write_table(table3, output_dir / 'repeated_int32_nulls.parquet')
    print(f"   Rows: {len(data3)}")
    print(f"   Data: {data3}")
    print()

    # Test 4: All empty lists - [[], [], []]
    print("4. Generating repeated_int32_all_empty.parquet")
    data4 = [[], [], []]
    schema4 = pa.schema([
        pa.field('numbers', pa.list_(pa.int32()))
    ])
    table4 = pa.table({'numbers': data4}, schema=schema4)
    pq.write_table(table4, output_dir / 'repeated_int32_all_empty.parquet')
    print(f"   Rows: {len(data4)}")
    print(f"   Data: {data4}")
    print()

    # Test 5: Single element lists - [[1], [2], [3], [4], [5]]
    print("5. Generating repeated_int32_single.parquet")
    data5 = [[1], [2], [3], [4], [5]]
    schema5 = pa.schema([
        pa.field('numbers', pa.list_(pa.int32()))
    ])
    table5 = pa.table({'numbers': data5}, schema=schema5)
    pq.write_table(table5, output_dir / 'repeated_int32_single.parquet')
    print(f"   Rows: {len(data5)}")
    print(f"   Data: {data5}")
    print()

    # Test 6: Large list - [[1, 2, 3, ..., 100]]
    print("6. Generating repeated_int32_large.parquet")
    data6 = [list(range(1, 101))]
    schema6 = pa.schema([
        pa.field('numbers', pa.list_(pa.int32()))
    ])
    table6 = pa.table({'numbers': data6}, schema=schema6)
    pq.write_table(table6, output_dir / 'repeated_int32_large.parquet')
    print(f"   Rows: {len(data6)}")
    print(f"   Data: [[1, 2, ..., 100]] (100 elements)")
    print()

    # Test 7: Repeated int64 - [[100, 200], [300]]
    print("7. Generating repeated_int64.parquet")
    data7 = [[100, 200], [300]]
    schema7 = pa.schema([
        pa.field('numbers', pa.list_(pa.int64()))
    ])
    table7 = pa.table({'numbers': data7}, schema=schema7)
    pq.write_table(table7, output_dir / 'repeated_int64.parquet')
    print(f"   Rows: {len(data7)}")
    print(f"   Data: {data7}")
    print()

    # Test 8: Repeated float - [[1.5, 2.5], [], [3.5]]
    print("8. Generating repeated_float.parquet")
    data8 = [[1.5, 2.5], [], [3.5]]
    schema8 = pa.schema([
        pa.field('numbers', pa.list_(pa.float32()))
    ])
    table8 = pa.table({'numbers': data8}, schema=schema8)
    pq.write_table(table8, output_dir / 'repeated_float.parquet')
    print(f"   Rows: {len(data8)}")
    print(f"   Data: {data8}")
    print()

    # Test 9: Repeated double - [[1.1, 2.2], [3.3, 4.4]]
    print("9. Generating repeated_double.parquet")
    data9 = [[1.1, 2.2], [3.3, 4.4]]
    schema9 = pa.schema([
        pa.field('numbers', pa.list_(pa.float64()))
    ])
    table9 = pa.table({'numbers': data9}, schema=schema9)
    pq.write_table(table9, output_dir / 'repeated_double.parquet')
    print(f"   Rows: {len(data9)}")
    print(f"   Data: {data9}")
    print()

    # Test 10: Repeated string - [["Alice", "Bob"], [], ["Charlie"]]
    print("10. Generating repeated_string.parquet")
    data10 = [["Alice", "Bob"], [], ["Charlie"]]
    schema10 = pa.schema([
        pa.field('names', pa.list_(pa.string()))
    ])
    table10 = pa.table({'names': data10}, schema=schema10)
    pq.write_table(table10, output_dir / 'repeated_string.parquet')
    print(f"   Rows: {len(data10)}")
    print(f"   Data: {data10}")
    print()

    # Test 11: Mixed - multiple columns
    print("11. Generating repeated_mixed.parquet")
    schema11 = pa.schema([
        pa.field('int_lists', pa.list_(pa.int32())),
        pa.field('str_lists', pa.list_(pa.string()))
    ])
    table11 = pa.table({
        'int_lists': [[1, 2], [3], []],
        'str_lists': [["a"], [], ["b", "c"]]
    }, schema=schema11)
    pq.write_table(table11, output_dir / 'repeated_mixed.parquet')
    print(f"   Rows: 3")
    print(f"   int_lists: [[1, 2], [3], []]")
    print(f"   str_lists: [['a'], [], ['b', 'c']]")
    print()

    print("✓ All fixtures generated successfully!")
    print(f"\nTotal files: 11")

if __name__ == '__main__':
    main()
