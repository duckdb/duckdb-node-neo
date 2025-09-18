import os
from fetch_libduckdb import fetch_libduckdb

zip_url = "https://github.com/duckdb/duckdb/releases/download/v1.4.0/libduckdb-linux-arm64.zip"
output_dir = os.path.join(os.path.dirname(__file__), "..", "libduckdb")
files = [
  "duckdb.h",
  "libduckdb.so.1.4",
]

fetch_libduckdb(zip_url, output_dir, files)
