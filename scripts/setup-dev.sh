#!/bin/bash
set -e

# Setup development environment by cloning Apache Arrow C++ reference code
# This is optional - the package builds and tests work without it

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ARROW_DIR="$PROJECT_ROOT/third_party/arrow"

echo "=== Parquet-Swift Developer Setup ==="
echo ""

# Check if Arrow directory already exists
if [ -d "$ARROW_DIR" ]; then
    echo "✓ Arrow C++ reference code already exists at:"
    echo "  $ARROW_DIR"
    echo ""
    echo "To update to latest main branch, run:"
    echo "  cd $ARROW_DIR && git pull"
    exit 0
fi

# Create third_party directory if needed
mkdir -p "$PROJECT_ROOT/third_party"

echo "Cloning Apache Arrow repository (main branch)..."
echo "This may take a few minutes (repository is ~70MB)..."
echo ""

# Clone Arrow repository (main branch)
git clone --depth 1 https://github.com/apache/arrow.git "$ARROW_DIR"

echo ""
echo "✓ Successfully cloned Apache Arrow C++ reference code"
echo ""
echo "Location: $ARROW_DIR"
echo "Key reference: $ARROW_DIR/cpp/src/parquet/"
echo ""
echo "The C++ implementation can be used as a reference while developing."
echo "It is not required for building or testing parquet-swift."
