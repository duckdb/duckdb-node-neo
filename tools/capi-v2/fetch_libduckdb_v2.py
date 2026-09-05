#!/usr/bin/env python3
"""Fetch the DuckDB 2.0 preview shared library + headers for a platform.

The 2.0 preview is not a GitHub release, so bindings/scripts/fetch_libduckdb_*.py
cannot reach it. Preview artifacts live under artifacts.duckdb.org keyed by the
release branch, and ship as tarballs rather than the release zips:

    https://artifacts.duckdb.org/v2.0-cyanoptera/duckdb-shared-libs-<suffix>.tar.gz

Each tarball contains libduckdb.{dylib,so,dll}, duckdb.h, duckdb_v2.h,
duckdb_extension.h and duckdb_extension_v2.h.

    python3 tools/capi-v2/fetch_libduckdb_v2.py                  # host platform
    python3 tools/capi-v2/fetch_libduckdb_v2.py linux-amd64      # a named one
"""

import os
import platform
import sys
import tarfile
import urllib.request

BRANCH = "v2.0-cyanoptera"
URL = "https://artifacts.duckdb.org/{branch}/duckdb-shared-libs-{suffix}.tar.gz"

# The suffixes published for the preview, one per platform Node Neo targets.
SUFFIXES = [
    "osx-universal",
    "linux-amd64",
    "linux-arm64",
    "linux-amd64-musl",
    "linux-arm64-musl",
    "windows-amd64",
    "windows-arm64",
]

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "libduckdb-v2")


def host_suffix():
    system, machine = platform.system(), platform.machine().lower()
    if system == "Darwin":
        return "osx-universal"
    arch = "arm64" if machine in ("arm64", "aarch64") else "amd64"
    if system == "Linux":
        return f"linux-{arch}"
    if system == "Windows":
        return f"windows-{arch}"
    sys.exit(f"unrecognized platform {system}/{machine}; pass a suffix explicitly")


def main():
    suffix = sys.argv[1] if len(sys.argv) > 1 else host_suffix()
    if suffix not in SUFFIXES:
        sys.exit(f"unknown suffix {suffix!r}; expected one of: {', '.join(SUFFIXES)}")

    url = URL.format(branch=BRANCH, suffix=suffix)
    os.makedirs(OUT, exist_ok=True)
    archive = os.path.join(OUT, f"duckdb-shared-libs-{suffix}.tar.gz")

    print(f"fetching: {url}")
    # artifacts.duckdb.org rejects urllib's default User-Agent with a 403.
    request = urllib.request.Request(url, headers={"User-Agent": "duckdb-node-neo/capi-v2-survey"})
    with urllib.request.urlopen(request) as response, open(archive, "wb") as f:
        while chunk := response.read(1 << 20):
            f.write(chunk)
    with tarfile.open(archive) as tar:
        for member in tar.getmembers():
            print(f"extracting: {member.name}")
        tar.extractall(OUT)
    print(f"\n{OUT}")


if __name__ == "__main__":
    main()
