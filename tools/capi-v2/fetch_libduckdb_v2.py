#!/usr/bin/env python3
"""Fetch the DuckDB shared library + all headers for a platform, for the probes here.

bindings/scripts/fetch_libduckdb_*.py extract only duckdb.h and the library, but the
probes in this directory also need duckdb_v2.h. This pulls the same release tarball
(same version, read from bindings/scripts/fetch_libduckdb.py) and unpacks all of it
into libduckdb-v2/:

    <base_url>/duckdb-shared-libs-<suffix>.tar.gz

Each tarball contains libduckdb.{dylib,so,dll}, duckdb.h, duckdb_v2.h,
duckdb_extension.h, duckdb_extension_v2.h and duckdb_static_extension.h.

    python3 tools/capi-v2/fetch_libduckdb_v2.py                  # host platform
    python3 tools/capi-v2/fetch_libduckdb_v2.py linux-amd64      # a named one
"""

import os
import platform
import sys
import tarfile
import urllib.request

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(here, "..", "..", "bindings", "scripts"))

from fetch_libduckdb import base_url  # noqa: E402

URL = "{base_url}/duckdb-shared-libs-{suffix}.tar.gz"

# The suffixes the release publishes for the platforms Node Neo targets.
SUFFIXES = [
    "osx-arm64",
    "osx-amd64",
    "osx-universal",
    "linux-amd64",
    "linux-arm64",
    "linux-amd64-musl",
    "linux-arm64-musl",
    "windows-amd64",
    "windows-arm64",
]

OUT = os.path.join(here, "libduckdb-v2")


def host_suffix():
    system, machine = platform.system(), platform.machine().lower()
    arch = "arm64" if machine in ("arm64", "aarch64") else "amd64"
    if system == "Darwin":
        return f"osx-{arch}"
    if system == "Linux":
        return f"linux-{arch}"
    if system == "Windows":
        return f"windows-{arch}"
    sys.exit(f"unrecognized platform {system}/{machine}; pass a suffix explicitly")


def main():
    suffix = sys.argv[1] if len(sys.argv) > 1 else host_suffix()
    if suffix not in SUFFIXES:
        sys.exit(f"unknown suffix {suffix!r}; expected one of: {', '.join(SUFFIXES)}")

    url = URL.format(base_url=base_url, suffix=suffix)
    os.makedirs(OUT, exist_ok=True)
    archive = os.path.join(OUT, f"duckdb-shared-libs-{suffix}.tar.gz")

    print(f"fetching: {url}")
    with urllib.request.urlopen(url) as response, open(archive, "wb") as f:
        while chunk := response.read(1 << 20):
            f.write(chunk)
    with tarfile.open(archive) as tar:
        for member in tar.getmembers():
            print(f"extracting: {member.name}")
        if hasattr(tarfile, "data_filter"):
            tar.extractall(OUT, filter="data")
        else:
            tar.extractall(OUT)
    print(f"\n{OUT}")


if __name__ == "__main__":
    main()
