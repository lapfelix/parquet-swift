#!/usr/bin/env python3
"""
Generate a PLAIN-only Parquet test fixture for parquet-swift.

This creates a simple Parquet file with:
- Multiple primitive types (Int32, Int64, Float, Double, String)
- PLAIN encoding only (no dictionary)
- Uncompressed data
- Small number of rows for easy testing
"""

import pyarrow as pa
import pyarrow.parquet as pq

# Create test data with various types (20 rows)
num_rows = 20

id_col = list(range(num_rows))                          # Int32
bigint_col = [i * 1000000000 for i in range(num_rows)]  # Int64
float_col = [float(i * 1.5) for i in range(num_rows)]   # Float
double_col = [float(i * 3.14159) for i in range(num_rows)]  # Double
string_col = [f'row_{i:02d}' for i in range(num_rows)]  # String

# Define schema with explicit types
schema = pa.schema([
    ('id', pa.int32()),
    ('bigint_col', pa.int64()),
    ('float_col', pa.float32()),
    ('double_col', pa.float64()),
    ('string_col', pa.string())
])

# Create Arrow arrays
arrays = [
    pa.array(id_col, type=pa.int32()),
    pa.array(bigint_col, type=pa.int64()),
    pa.array(float_col, type=pa.float32()),
    pa.array(double_col, type=pa.float64()),
    pa.array(string_col, type=pa.string())
]

# Create Arrow Table
table = pa.Table.from_arrays(arrays, schema=schema)

# Write with PLAIN encoding only (no dictionary, no compression)
# Use write_to_dataset for more control, or simpler write_table
writer = pq.ParquetWriter(
    'Tests/ParquetTests/Fixtures/plain_types.parquet',
    schema,
    version='1.0',
    compression='none',
    use_dictionary=False,
    write_statistics=False  # Disable stats for simpler file
)
writer.write_table(table)
writer.close()

print("Generated plain_types.parquet")
print(f"Rows: {num_rows}")
print(f"Columns: {[field.name for field in schema]}")
print("\nColumn types:")
for col in table.schema:
    print(f"  {col.name}: {col.type}")
