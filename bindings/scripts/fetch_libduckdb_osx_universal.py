import os
from fetch_libduckdb import fetch_libduckdb
from duckdb_version import DUCKDB_VERSION

zip_url = f"https://github.com/duckdb/duckdb/releases/download/{DUCKDB_VERSION}/libduckdb-osx-universal.zip"
output_dir = os.path.join(os.path.dirname(__file__), "..", "libduckdb")
files = [
  "duckdb.h",
  "libduckdb.dylib",
]

fetch_libduckdb(zip_url, output_dir, files)
