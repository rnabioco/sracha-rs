#!/usr/bin/env python3
"""Sync the Bioconda / BioContainers version pins in README.md and docs/.

The docs pin a concrete BioContainers tag (`sracha:<version>--<build>`)
because quay.io publishes no `latest` tag for Bioconda-derived images — the
tag has to name a version and a conda build hash that actually exist. That
makes the pins rot every release, so this script looks up what is current and
rewrites them in place.

Two independent sources, because they can legitimately disagree:

  * the container tag comes from quay.io, so we only ever write a tag that has
    really been published (BioContainers lags the Bioconda package by hours to
    days);
  * the `conda` directive pin comes from anaconda.org, which is the package
    that `conda`/`pixi` would actually resolve.

The Anaconda badges are rewritten too, for a different reason: they render the
current version by themselves, but GitHub serves README images through its
camo proxy, which caches them long past a release. The `?v=` on those URLs is
a cache-buster with no meaning to anaconda.org, so flipping it is what makes
the badge re-fetch. It is flipped only when a pin actually moved, so a
no-change week still produces no diff and no PR.

Usage:
    sync-bioconda-version.py            # rewrite the files in place
    sync-bioconda-version.py --check    # exit 1 if anything is out of date
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TARGETS = ["README.md", "docs/index.md"]

QUAY_TAGS = (
    "https://quay.io/api/v1/repository/biocontainers/sracha/tag/"
    "?limit=100&onlyActiveTags=true"
)
ANACONDA_PKG = "https://api.anaconda.org/package/bioconda/sracha"

# `quay.io/biocontainers/sracha:0.3.11--h54198d6_0` and the matching
# Singularity URL on depot.galaxyproject.org.
CONTAINER_TAG_RE = re.compile(r"(?<=sracha:)(\d[\d.]*)--([A-Za-z0-9_]+)")
# `conda 'bioconda::sracha=0.3.11'`
CONDA_PIN_RE = re.compile(r"(?<=bioconda::sracha=)(\d[\d.]*)")
# `anaconda.org/bioconda/sracha/badges/version.svg?v=1`, with the query string
# optional — a badge that has never been flipped does not carry one yet.
BADGE_RE = re.compile(r"(anaconda\.org/bioconda/sracha/badges/\w+\.svg)(?:\?v=(\d+))?")


def flip_badge_cache_busters(text: str) -> str:
    """Toggle every Anaconda badge URL's `?v=` between 1 and 2.

    The value itself is meaningless; only the change matters, because that is
    what makes GitHub's camo proxy treat it as a new image and fetch it again.
    Alternating between two values keeps the URL from growing forever.
    """

    def flip(match: re.Match) -> str:
        url, current = match.group(1), match.group(2)
        return f"{url}?v={2 if current == '1' else 1}"

    return BADGE_RE.sub(flip, text)


def fetch_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def version_key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def latest_container_tag() -> str:
    """Newest `<version>--<build>` tag published on quay.io."""
    tags = []
    for tag in fetch_json(QUAY_TAGS).get("tags", []):
        match = re.fullmatch(r"(\d[\d.]*)--([A-Za-z0-9_]+)", tag.get("name", ""))
        if match:
            tags.append((version_key(match.group(1)), match.group(0)))
    if not tags:
        raise SystemExit("no versioned tags found on quay.io/biocontainers/sracha")
    return max(tags)[1]


def latest_conda_version() -> str:
    """Newest version of the `sracha` package on anaconda.org/bioconda."""
    version = fetch_json(ANACONDA_PKG).get("latest_version")
    if not version:
        raise SystemExit("anaconda.org returned no latest_version for bioconda::sracha")
    return version


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="report drift and exit 1 instead of rewriting files",
    )
    args = parser.parse_args()

    try:
        container_tag = latest_container_tag()
        conda_version = latest_conda_version()
    except (urllib.error.URLError, TimeoutError) as err:
        print(f"error: upstream lookup failed: {err}", file=sys.stderr)
        return 2

    print(f"biocontainers tag: {container_tag}")
    print(f"bioconda version:  {conda_version}")

    stale = []
    for relative in TARGETS:
        path = REPO_ROOT / relative
        original = path.read_text()
        updated = CONTAINER_TAG_RE.sub(container_tag, original)
        updated = CONDA_PIN_RE.sub(conda_version, updated)
        if updated == original:
            continue
        stale.append(relative)
        if not args.check:
            # Only after a pin moved: --check stays a question about pins, and
            # flipping unconditionally would open an empty PR every week.
            path.write_text(flip_badge_cache_busters(updated))

    if not stale:
        print("pins are up to date")
        return 0

    if args.check:
        print(f"out of date: {', '.join(stale)}", file=sys.stderr)
        return 1

    print(f"updated: {', '.join(stale)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
