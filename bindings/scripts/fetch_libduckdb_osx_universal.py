import os
from fetch_libduckdb import fetch_libduckdb

zip_url = "https://github.com/duckdb/duckdb/releases/download/v1.4.0/libduckdb-osx-universal.zip"
output_dir = os.path.join(os.path.dirname(__file__), "..", "libduckdb")
files = [
  "duckdb.h",
  "libduckdb.1.4.dylib",
]

fetch_libduckdb(zip_url, output_dir, files)
