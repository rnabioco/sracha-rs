#!/bin/bash
# Generate all VHS gifs for documentation
# Run from repository root: ./docs/tapes/generate.sh
#
# Tapes set `MarginFill "#fe00fe"` (magenta sentinel) so the margin and
# rounded-corner pixels can be keyed out to transparent after render,
# letting the terminal float over any docs background. GIF only supports
# single-color transparency; the inside of the window keeps Dracula's bg.

set -e

cd "$(dirname "$0")/../.."

# Build release binary first
cargo build --profile release

# Add to PATH
export PATH="$PWD/target/release:$PATH"

# Generate each gif, then key out the magenta margin
for tape in docs/tapes/*.tape; do
    echo "Generating: $tape"
    vhs "$tape"
done

echo "Making margins transparent..."
# -fuzz absorbs the palette quantization + border antialiasing pixels
# (e.g. #fb00fc, #e709ea) that cluster around the sentinel magenta.
for gif in docs/images/*.gif; do
    magick "$gif" -fuzz 15% -transparent '#fe00fe' "$gif"
done

echo "Done! Gifs are in docs/images/"
