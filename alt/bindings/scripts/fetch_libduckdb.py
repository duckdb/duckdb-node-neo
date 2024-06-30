import os
import sys
import urllib.request
import zipfile

libduckdb_zip_url_for_os = {
  "linux": "https://github.com/duckdb/duckdb/releases/download/v1.0.0/libduckdb-linux-amd64.zip", # TODO: handle linux-aarch64
  "mac": "https://github.com/duckdb/duckdb/releases/download/v1.0.0/libduckdb-osx-universal.zip",
  "win": "https://github.com/duckdb/duckdb/releases/download/v1.0.0/libduckdb-windows-amd64.zip",
}

libduckdb_file_names_for_os = {
  "linux": ["duckdb.h", "libduckdb.so"],
  "mac": ["duckdb.h", "libduckdb.dylib"],
  "win": ["duckdb.h", "libduckdb.lib", "libduckdb.dll"],
}

os_name = sys.argv[1]
output_dir = sys.argv[2]

libduckdb_zip_url = libduckdb_zip_url_for_os[os_name]
libduckdb_file_names = libduckdb_file_names_for_os[os_name]

libduckdb_zip_path = os.path.join(output_dir, "libduckdb.zip")
print("fetching: " + libduckdb_zip_url)
urllib.request.urlretrieve(libduckdb_zip_url, libduckdb_zip_path)

zip = zipfile.ZipFile(libduckdb_zip_path)

for file_name in libduckdb_file_names:
  print("extracting: " + file_name)
  zip.extract(file_name, output_dir)
