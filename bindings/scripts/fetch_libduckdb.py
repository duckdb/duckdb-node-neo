import os
import tarfile
import urllib.request

# Where the duckdb-shared-libs-<suffix>.tar.gz archives are fetched from.
base_url = "https://github.com/duckdb/duckdb/releases/download/v2.0.0"

def fetch_libduckdb(suffix, files):
  output_dir = os.path.join(os.path.dirname(__file__), "..", "libduckdb")
  if not os.path.exists(output_dir):
    os.makedirs(output_dir)

  url = f"{base_url}/duckdb-shared-libs-{suffix}.tar.gz"
  local_archive_path = os.path.join(output_dir, "libduckdb.tar.gz")
  print("fetching: " + url)
  with urllib.request.urlopen(url) as response, open(local_archive_path, "wb") as f:
    while chunk := response.read(1 << 20):
      f.write(chunk)

  with tarfile.open(local_archive_path) as tar:
    for file in files:
      print("extracting: " + file)
      if hasattr(tarfile, "data_filter"):
        tar.extract(file, output_dir, filter="data")
      else:
        tar.extract(file, output_dir)
