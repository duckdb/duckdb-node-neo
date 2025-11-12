import os
from fetch_libduckdb import fetch_libduckdb

zip_url = "https://github.com/duckdb/duckdb/releases/download/v1.4.2/libduckdb-osx-universal.zip"
output_dir = os.path.join(os.path.dirname(__file__), "..", "libduckdb")
files = [
  "duckdb.h",
  "libduckdb.dylib",
]

fetch_libduckdb(zip_url, output_dir, files)
