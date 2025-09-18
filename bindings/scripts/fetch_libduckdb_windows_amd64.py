import os
from fetch_libduckdb import fetch_libduckdb

zip_url = "https://github.com/duckdb/duckdb/releases/download/v1.4.0/libduckdb-windows-amd64.zip"
output_dir = os.path.join(os.path.dirname(__file__), "..", "libduckdb")
files = [
  "duckdb.h",
  "duckdb.lib",
  "duckdb.dll",
]

fetch_libduckdb(zip_url, output_dir, files)
