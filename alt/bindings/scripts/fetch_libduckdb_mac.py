from fetch_libduckdb import fetch_libduckdb

zip_url = "https://github.com/duckdb/duckdb/releases/download/v1.0.0/libduckdb-osx-universal.zip"
output_dir = "libduckdb"
files = [
  "duckdb.h",
  "libduckdb.dylib",
]

fetch_libduckdb(zip_url, output_dir, files)
