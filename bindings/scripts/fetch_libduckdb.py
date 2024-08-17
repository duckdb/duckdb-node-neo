import os
import urllib.request
import zipfile

def fetch_libduckdb(zip_url, output_dir, files):
  if not os.path.exists(output_dir):
    os.makedirs(output_dir)
  
  local_zip_path = os.path.join(output_dir, "libduckdb.zip")
  print("fetching: " + zip_url)
  urllib.request.urlretrieve(zip_url, local_zip_path)

  zip = zipfile.ZipFile(local_zip_path)
  for file in files:
    print("extracting: " + file)
    zip.extract(file, output_dir)
