import os
import sys
import urllib.request
import zipfile

output_dir = sys.argv[1]
zip_url = "https://github.com/duckdb/duckdb/releases/download/v1.0.0/libduckdb-windows-amd64.zip"

print("output_dir: " + output_dir)
print("zip_url: " + zip_url)

local_zip_path = os.path.join(output_dir, "libduckdb.zip")
print("fetching: " + zip_url)
urllib.request.urlretrieve(zip_url, local_zip_path)

zip = zipfile.ZipFile(local_zip_path)
print("extracting: " + ", ".join(zip.namelist()))
zip.extractall(output_dir)
