#!/bin/bash
set -e

# Remove Apache Arrow C++ reference code
# This is safe to run - the package builds and tests work without it

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ARROW_DIR="$PROJECT_ROOT/third_party/arrow"

echo "=== Parquet-Swift Developer Cleanup ==="
echo ""

# Check if Arrow directory exists
if [ ! -d "$ARROW_DIR" ]; then
    echo "✓ Arrow C++ reference code is not present"
    echo "  Nothing to clean up"
    exit 0
fi

echo "Removing Apache Arrow C++ reference code..."
echo "Location: $ARROW_DIR"
echo ""

# Remove Arrow directory
rm -rf "$ARROW_DIR"

echo "✓ Successfully removed Arrow C++ reference code"
echo ""
echo "The package still builds and tests normally."
echo "To restore the reference code, run: ./scripts/setup-dev.sh"
