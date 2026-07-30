"""Shallow-clone the other DuckDB C API clients for coverage analysis.

Only the source paths needed for detection are checked out; the vendored copies of
DuckDB itself (which are large, and which every client bundles) are excluded.
"""

import os
import subprocess
import sys

clients_dir = os.path.join(os.path.dirname(__file__), "clients")

# repo -> sparse-checkout patterns for the paths we actually parse
repos = {
  "duckdb/duckdb-go-bindings": ["/*.go", "/README.md"],
  "duckdb/duckdb-go": ["/*", "!/examples"],
  "duckdb/duckdb-rs": [
    "/crates/duckdb/*",
    "/crates/libduckdb-sys/src/*",
    "/crates/libduckdb-sys/Cargo.toml",
    "/Cargo.toml",
  ],
  "Giorgi/DuckDB.NET": ["/DuckDB.NET.Bindings/*", "/DuckDB.NET.Data/*"],
  "duckdb/duckdb-swift": ["/Sources/DuckDB/*", "/Package.swift"],
  "duckdb/DuckDB.jl": ["/src/*", "/Project.toml"],
}


def run(args, cwd=None):
  subprocess.run(args, cwd=cwd, check=True, stdout=subprocess.DEVNULL)


def fetch(repo, patterns):
  name = repo.split("/")[-1]
  path = os.path.join(clients_dir, name)
  if os.path.exists(path):
    print("updating: " + name)
    run(["git", "fetch", "--depth", "1", "origin"], cwd=path)
    run(["git", "reset", "--hard", "origin/HEAD"], cwd=path)
    return path

  print("cloning: " + repo)
  run([
    "git", "clone",
    "--depth", "1",
    "--filter=blob:none",
    "--no-checkout",
    "https://github.com/" + repo + ".git",
    path,
  ])
  run(["git", "sparse-checkout", "set", "--no-cone"] + patterns, cwd=path)
  run(["git", "checkout", "HEAD"], cwd=path)
  return path


def main():
  if not os.path.exists(clients_dir):
    os.makedirs(clients_dir)
  for repo, patterns in repos.items():
    try:
      fetch(repo, patterns)
    except subprocess.CalledProcessError as e:
      print("FAILED: " + repo + ": " + str(e), file=sys.stderr)
      return 1
  print("\nclients checked out under: " + clients_dir)
  return 0


if __name__ == "__main__":
  sys.exit(main())
