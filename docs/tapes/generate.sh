#!/bin/bash
# Generate all VHS gifs for documentation
# Run from repository root: ./docs/tapes/generate.sh

set -e

cd "$(dirname "$0")/../.."

# Build release binary first
cargo build --profile release

# Add to PATH
export PATH="$PWD/target/release:$PATH"

# Generate each gif
for tape in docs/tapes/*.tape; do
    echo "Generating: $tape"
    vhs "$tape"
done

echo "Done! Gifs are in docs/images/"
