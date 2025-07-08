import os
from fetch_libduckdb import fetch_libduckdb

zip_url = "https://github.com/duckdb/duckdb/releases/download/v1.3.2/libduckdb-linux-amd64.zip"
output_dir = os.path.join(os.path.dirname(__file__), "..", "libduckdb")
files = [
  "duckdb.h",
  "libduckdb.so",
]

fetch_libduckdb(zip_url, output_dir, files)
