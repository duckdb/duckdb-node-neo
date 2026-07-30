"""Detect C API coverage in the five other C-API-based DuckDB clients.

Each detector returns (binding, wrapper, reason, symbol) dicts keyed by C API
function name, where:

  binding -- a publicly reachable 1:1 binding exists for this exact function
  wrapper -- the function is reached from the client's idiomatic/high-level layer

Only three of the six clients have a hand-written, selective binding layer
(Node Neo, Go, C#), so only for those is `binding` an informative signal. Rust and
Julia auto-generate a complete binding layer, and Swift publishes no raw C layer at
all -- for those three only `wrapper` discriminates. See README.md.

None of these five records a machine-readable reason for non-exposure, so `reason`
is always empty; only Node Neo has that convention.
"""

import glob
import os
import re

clients_dir = os.path.join(os.path.dirname(__file__), "clients")


def read_all(*patterns):
  """Concatenate every file matching the given globs (relative to clients_dir)."""
  out = []
  for pattern in patterns:
    for path in glob.glob(os.path.join(clients_dir, pattern), recursive=True):
      if os.path.isfile(path):
        with open(path, errors="replace") as f:
          out.append(f.read())
  return "\n".join(out)


def canonical_hits(text, pattern, canonical):
  """Names captured by `pattern` that are real C API functions.

  Intersecting against the canonical list is load-bearing for Go and Rust, where
  type conversions like `C.duckdb_database(x)` are syntactically identical to calls.
  No C API function name collides with a duckdb_* typedef, so this is exact.
  """
  return set(m for m in re.findall(pattern, text) if m in canonical)


def go(canonical):
  """duckdb-go-bindings is the raw layer; duckdb-go/mapping re-exports what it uses."""
  go_to_capi = {}
  for path in glob.glob(os.path.join(clients_dir, "duckdb-go-bindings", "*.go")):
    if path.endswith("_test.go"):
      continue
    with open(path, errors="replace") as f:
      source = f.read()
    for part in re.split(r"\n(?=func\s)", source):
      match = re.match(r"func\s+(?:\([^)]*\)\s*)?([A-Za-z0-9_]+)\s*\(", part)
      if not match or not match.group(1)[0].isupper():
        continue
      calls = canonical_hits(part, r"C\.(duckdb_[a-z0-9_]+)\s*\(", canonical)
      if calls:
        go_to_capi.setdefault(match.group(1), set()).update(calls)

  binding = {}
  symbol = {}
  for go_name, capi_names in go_to_capi.items():
    for name in capi_names:
      binding[name] = True
      symbol.setdefault(name, go_name)

  # A few are reached only through unexported helpers (duckdb_malloc via Malloc);
  # still bound, just without an attributable Go symbol.
  everything = read_all(os.path.join("duckdb-go-bindings", "*.go"))
  for name in canonical_hits(everything, r"C\.(duckdb_[a-z0-9_]+)\s*\(", canonical):
    binding.setdefault(name, True)

  mapping_text = read_all(
    os.path.join("duckdb-go", "mapping", "*.go"),
    os.path.join("duckdb-go", "arrowmapping", "*.go"),
  )
  reexported = set(re.findall(r"bindings\.([A-Za-z0-9_]+)", mapping_text))
  driver_text = read_all(os.path.join("duckdb-go", "*.go"))

  wrapper = {}
  for name in binding:
    go_name = symbol.get(name)
    wrapper[name] = bool(go_name) and go_name in reexported and bool(
      re.search(r"\bmapping\." + re.escape(go_name) + r"\b", driver_text)
    )
  return binding, wrapper, {}, symbol


def rust(canonical):
  """libduckdb-sys is bindgen output over the whole header; crates/duckdb is the safe API."""
  sys_source = read_all(
    os.path.join("duckdb-rs", "crates", "libduckdb-sys", "src", "bindgen_bundled_version.rs"),
    os.path.join(
      "duckdb-rs", "crates", "libduckdb-sys", "src", "bindgen_bundled_version_loadable.rs"
    ),
  )
  binding = {
    name: True
    for name in canonical_hits(sys_source, r"pub fn (duckdb_[a-z0-9_]+)\s*\(", canonical)
  }
  safe = read_all(os.path.join("duckdb-rs", "crates", "duckdb", "**", "*.rs"))
  safe = re.sub(r"//[^\n]*", "", safe)  # drop line comments so doc mentions don't count
  used = canonical_hits(safe, r"\b(duckdb_[a-z0-9_]+)\s*\(", canonical)
  return binding, {n: n in used for n in binding}, {}, {n: n for n in binding}


def csharp(canonical):
  """Every DuckDB.NET binding is an explicit [LibraryImport(..., EntryPoint = "...")]."""
  binding = {}
  symbol = {}
  pattern = os.path.join(clients_dir, "DuckDB.NET", "DuckDB.NET.Bindings", "**", "*.cs")
  for path in glob.glob(pattern, recursive=True):
    enclosing_class = None
    pending = []
    with open(path, errors="replace") as f:
      for line in f:
        s = line.strip()
        class_match = re.match(
          r"public\s+(?:static\s+)?partial\s+class\s+([A-Za-z0-9_]+)", s
        )
        if class_match:
          enclosing_class = class_match.group(1)
          continue
        for match in re.finditer(r'EntryPoint\s*=\s*"(duckdb_[a-z0-9_]+)"', s):
          if match.group(1) in canonical:
            binding[match.group(1)] = True
            pending.append(match.group(1))
        if not pending:
          continue
        # the declaration itself: "public static partial <ReturnType> <Name>(...)"
        decl = re.search(r"\bpartial\s+[\w<>?\[\],\s.]+?\s([A-Za-z_][A-Za-z0-9_]*)\s*\(", s)
        if decl:
          name = (
            enclosing_class + "." + decl.group(1) if enclosing_class else decl.group(1)
          )
          for capi_name in pending:
            symbol.setdefault(capi_name, name)
          pending = []

  # NOTE: matched on managed method name, so C API functions that share one name via
  # overloads (duckdb_open and duckdb_open_ext are both Startup.DuckDBOpen) are
  # slightly over-reported here.
  data = read_all(os.path.join("DuckDB.NET", "DuckDB.NET.Data", "**", "*.cs"))
  wrapper = {}
  for name in binding:
    method = symbol.get(name, "").split(".")[-1]
    wrapper[name] = bool(method) and bool(
      re.search(r"\b" + re.escape(method) + r"\s*\(", data)
    )
  return binding, wrapper, {}, symbol


def swift(canonical):
  """Cduckdb is an internal target, not a Package.swift product -- no public raw layer."""
  source = read_all(os.path.join("duckdb-swift", "Sources", "DuckDB", "**", "*.swift"))
  source = re.sub(r"//[^\n]*", "", source)
  used = canonical_hits(source, r"\b(duckdb_[a-z0-9_]+)\s*\(", canonical)
  return {}, {n: True for n in used}, {}, {n: n for n in used}


def julia(canonical):
  """src/api.jl is generated by scripts/julia_adapter.py and covers the whole header."""
  api = read_all(os.path.join("DuckDB.jl", "src", "api.jl"))
  binding = {
    name: True
    for name in canonical_hits(api, r"\(:(duckdb_[a-z0-9_]+),\s*libduckdb\)", canonical)
  }
  others = []
  for path in glob.glob(os.path.join(clients_dir, "DuckDB.jl", "src", "*.jl")):
    if not path.endswith("api.jl"):
      with open(path, errors="replace") as f:
        others.append(f.read())
  used = canonical_hits("\n".join(others), r"\b(duckdb_[a-z0-9_]+)\s*\(", canonical)
  return binding, {n: n in used for n in binding}, {}, {n: n for n in binding}
