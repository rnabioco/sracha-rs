#!/usr/bin/env bash
# Download and extract a versioned NCBI sra-tools binary release into
# validation/sra-tools/. The bioconda package has been unreliable; this
# installs the upstream tarball from ftp-trace.ncbi.nlm.nih.gov so
# `validation/benchmark.sh` has a known-good reference to compare against.
#
# Usage:
#   bash validation/install-sratools.sh           # default version
#   SRATOOLS_VERSION=3.4.1 bash validation/install-sratools.sh

set -euo pipefail

VERSION="${SRATOOLS_VERSION:-3.4.1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST_DIR="$SCRIPT_DIR/sra-tools"

case "$(uname -s)-$(uname -m)" in
    Linux-x86_64)
        case "$VERSION" in
            3.0.*|3.1.*|3.2.*|3.3.*) PLATFORM="centos_linux64" ;;
            *)                       PLATFORM="alma_linux64"   ;;
        esac
        ;;
    Darwin-arm64)  PLATFORM="mac-arm64" ;;
    Darwin-x86_64) PLATFORM="mac-x86_64" ;;
    *) echo "unsupported platform: $(uname -s)-$(uname -m)" >&2; exit 2 ;;
esac

TARBALL="sratoolkit.${VERSION}-${PLATFORM}.tar.gz"
URL="https://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/${VERSION}/${TARBALL}"
EXTRACTED="${DEST_DIR}/sratoolkit.${VERSION}-${PLATFORM}"

if [[ -x "${EXTRACTED}/bin/fasterq-dump" ]]; then
    echo "sra-tools ${VERSION} already installed at ${EXTRACTED}"
    exit 0
fi

mkdir -p "$DEST_DIR"
echo "Downloading sra-tools ${VERSION} (${PLATFORM})..."
curl -fSL -o "${DEST_DIR}/${TARBALL}" "$URL"

echo "Extracting to ${DEST_DIR}..."
tar -xzf "${DEST_DIR}/${TARBALL}" -C "$DEST_DIR"
rm -f "${DEST_DIR}/${TARBALL}"

echo "Installed: ${EXTRACTED}/bin/fasterq-dump"
"${EXTRACTED}/bin/fasterq-dump" --version 2>&1 | head -1
