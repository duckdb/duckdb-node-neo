#!/usr/bin/env python3
"""Compare the DuckDB C API V1 and V2 surfaces, and Node Neo's coverage of them.

Companion to ../capi-coverage, which measures Node Neo against the other five
C-API clients. This one measures V1 against V2, for the DuckDB 2.0 migration. The
V1 side -- the canonical function list and Node Neo's exposure of it -- is reused
from that tool rather than re-derived, so both stay in agreement.

Every V2 function is still marked unstable, and V2 changes between patch releases,
so nothing here is hard-coded; every count comes from the headers.

By default the headers come from the DuckDB release the bindings are built against
(the version in bindings/scripts/fetch_libduckdb.py). --ref names any other tag or
branch, e.g. v2.0-cyanoptera to see what the next patch release will change.

    python3 tools/capi-v2/compare_api.py --fetch      # download the release's headers
    python3 tools/capi-v2/compare_api.py --fetch --ref v2.0-cyanoptera
    python3 tools/capi-v2/compare_api.py              # print the report
    python3 tools/capi-v2/compare_api.py --json DIR   # also write the inventories

Headers are cached in .headers/ next to this script (gitignored).
"""

import argparse
import json
import os
import re
import sys
import urllib.request

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(here, "..", "capi-coverage"))
sys.path.insert(0, os.path.join(here, "..", "..", "bindings", "scripts"))

from capi_functions import capi_functions  # noqa: E402
from fetch_libduckdb import base_url  # noqa: E402
from node_neo import node_neo_coverage  # noqa: E402

# The release tag the bindings fetch from, e.g. "v2.0.1".
release_ref = base_url.rstrip("/").rsplit("/", 1)[1]
raw_url = "https://raw.githubusercontent.com/duckdb/duckdb/{ref}/src/include/{name}"
cache_dir = os.path.join(here, ".headers")
ref_file = os.path.join(cache_dir, "REF")

# V2 declarations look the same as V1's, but V2 wraps them across lines more often.
declaration_rule = re.compile(r"^DUCKDB_C_API\s+((?:[^;])*?);", re.M | re.S)
name_rule = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s*\(")

# V2 groups functions under banner comments inside a block comment, rather than V1's
# //---- rules:
#     /* ====...
#      * MODULE: vector
#      * ====... */
module_rule = re.compile(r"^ \* MODULE: (.*)$")

# V1 enumerators, e.g. `DUCKDB_TYPE_TIMESTAMP_TZ_NS = 42,`. New values can change
# behavior without touching a single function signature.
enumerator_rule = re.compile(r"^\s+(DUCKDB_[A-Z0-9_]+) = (-?\w+),?$", re.M)


def fetch_headers(ref):
  os.makedirs(cache_dir, exist_ok=True)
  for name in ("duckdb.h", "duckdb_v2.h"):
    url = raw_url.format(ref=ref, name=name)
    print(f"fetching: {url}")
    urllib.request.urlretrieve(url, os.path.join(cache_dir, name))
  with open(ref_file, "w", encoding="utf-8") as f:
    f.write(ref + "\n")


def cached_ref():
  """The ref the cached headers were fetched from, or "?" if unrecorded."""
  if not os.path.exists(ref_file):
    return "?"
  return open(ref_file, encoding="utf-8").read().strip()


def unstable_functions(path):
  """Names of the V2 functions declared inside an #if on DUCKDB_V2_API_ALLOW_UNSTABLE."""
  stack = []
  unstable = set()
  pending = None
  for line in open(path, encoding="utf-8"):
    stripped = line.strip()
    if stripped.startswith("#if"):
      stack.append("DUCKDB_V2_API_ALLOW_UNSTABLE" in stripped)
    elif stripped.startswith(("#else", "#elif")):
      stack[-1] = False
    elif stripped.startswith("#endif"):
      stack.pop()
    if pending is None and stripped.startswith("DUCKDB_C_API"):
      pending, guarded = stripped, any(stack)
    elif pending is not None:
      pending += " " + stripped
    if pending is not None and "(" in pending:
      if guarded:
        unstable.add(name_rule.search(pending).group(1))
      pending = None
  return unstable


def declarations(path):
  """Map function name -> normalized declaration, in header order."""
  text = open(path, encoding="utf-8").read()
  out = {}
  for match in declaration_rule.finditer(text):
    declaration = re.sub(r"\s+", " ", match.group(1)).strip()
    name = name_rule.search(declaration)
    if name:
      out[name.group(1)] = declaration
  return out


def v2_modules(path):
  """Map V2 module name -> function names."""
  text = open(path, encoding="utf-8").read()
  lines = text.split("\n")
  banners = [
    (i + 1, match.group(1).strip())
    for i, line in enumerate(lines)
    for match in [module_rule.match(line)]
    if match
  ]

  modules = {}
  for match in declaration_rule.finditer(text):
    line_no = text.count("\n", 0, match.start()) + 1
    name = name_rule.search(re.sub(r"\s+", " ", match.group(1)).strip()).group(1)
    module = "(preamble)"
    for at, title in banners:
      if at <= line_no:
        module = title
      else:
        break
    modules.setdefault(module, []).append(name)
  return modules


def enumerators(path):
  """Map V1 enumerator name -> value."""
  return dict(enumerator_rule.findall(open(path, encoding="utf-8").read()))


def v1_drift(shipped, on_branch, exposed, ref):
  """What differs in the V1 surface between the bindings' header and ref's."""
  removed = sorted(set(shipped) - set(on_branch))
  added = sorted(set(on_branch) - set(shipped))
  changed = sorted(
    name for name in set(shipped) & set(on_branch)
    if shipped[name] != on_branch[name]
  )

  print(f"\nV1 drift, bindings -> {ref}: {len(removed)} removed, {len(added)} added, "
        f"{len(changed)} signature changes")
  for name in removed:
    print(f"  removed  {name}{'   [exposed by Neo]' if name in exposed else ''}")
  for name in added:
    print(f"  added    {name}")
  for name in changed:
    print(f"  changed  {name}{'   [exposed by Neo]' if name in exposed else ''}")
    print(f"             was: {shipped[name]}")
    print(f"             now: {on_branch[name]}")


def main():
  parser = argparse.ArgumentParser(
    description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
  )
  parser.add_argument("--fetch", action="store_true",
                      help="download duckdb.h and duckdb_v2.h into .headers/")
  parser.add_argument("--ref", default=release_ref,
                      help=f"tag or branch to fetch from (default: {release_ref}, "
                           "the release the bindings use)")
  parser.add_argument("--json", metavar="DIR", help="write inventories as JSON into DIR")
  args = parser.parse_args()

  if args.fetch:
    fetch_headers(args.ref)
  ref = cached_ref()

  v2_path = os.path.join(cache_dir, "duckdb_v2.h")
  v1_branch_path = os.path.join(cache_dir, "duckdb.h")
  if not os.path.exists(v2_path):
    sys.exit(f"missing {v2_path}; run with --fetch first")

  # The V1 side comes from capi-coverage, so the two tools cannot disagree.
  v1_functions = capi_functions()
  binding, _wrapper, reason, _symbol = node_neo_coverage()
  exposed = {name for name, is_exposed in binding.items() if is_exposed}

  # capi_functions keeps the trailing semicolon; declarations() does not.
  v1_shipped = {
    f["function"]: f["declaration"].rstrip(";").strip() for f in v1_functions
  }
  v1_on_branch = declarations(v1_branch_path) if os.path.exists(v1_branch_path) else {}
  v2 = declarations(v2_path)
  unstable = unstable_functions(v2_path)

  deprecated = sum(1 for f in v1_functions if f["deprecated"])
  print(f"V1 (shipped, bindings/libduckdb/duckdb.h): {len(v1_shipped)} functions "
        f"({deprecated} deprecated)")
  if v1_on_branch:
    print(f"V1 (duckdb.h at {ref}):{'':<21}{len(v1_on_branch)} functions")
  print(f"V2 (duckdb_v2.h at {ref}):{'':<18}{len(v2)} functions "
        f"({len(unstable)} behind DUCKDB_V2_API_ALLOW_UNSTABLE)")
  print(f"Node Neo exposes:                          {len(exposed)} of "
        f"{len(v1_shipped)} V1 functions")

  if v1_on_branch:
    v1_drift(v1_shipped, v1_on_branch, exposed, ref)
    enum_shipped = enumerators(os.path.join(here, "..", "..", "bindings", "libduckdb", "duckdb.h"))
    enum_on_branch = enumerators(v1_branch_path)
    enum_added = sorted(set(enum_on_branch) - set(enum_shipped))
    enum_removed = sorted(set(enum_shipped) - set(enum_on_branch))
    enum_changed = sorted(
      name for name in set(enum_shipped) & set(enum_on_branch)
      if enum_shipped[name] != enum_on_branch[name]
    )
    print(f"\nV1 enum drift, bindings -> {ref}: {len(enum_removed)} removed, {len(enum_added)} added, "
          f"{len(enum_changed)} renumbered")
    for name in enum_removed:
      print(f"  removed  {name}")
    for name in enum_added:
      print(f"  added    {name} = {enum_on_branch[name]}")
    for name in enum_changed:
      print(f"  changed  {name}: {enum_shipped[name]} -> {enum_on_branch[name]}")

  groups = {}
  for function in v1_functions:
    groups.setdefault(function["group"], []).append(function["function"])

  print(f"\n{'V1 group':<38} {'total':>6} {'in Neo':>7}")
  print("-" * 53)
  for title, names in groups.items():
    print(f"{title:<38} {len(names):6d} "
          f"{sum(1 for n in names if n in exposed):7d}")

  modules = v2_modules(v2_path)
  print(f"\n{'V2 module':<38} {'total':>6} {'unstable':>9}")
  print("-" * 55)
  for title, names in modules.items():
    print(f"{title:<38} {len(names):6d} "
          f"{sum(1 for n in names if n in unstable):9d}")
  stable = sorted(set(v2) - unstable)
  if stable and unstable:
    print(f"\nV2 functions outside DUCKDB_V2_API_ALLOW_UNSTABLE: {len(stable)}")
    for name in stable:
      print(f"  {name}")

  print("\nWhy Node Neo skips the rest of V1:")
  tally = {}
  for name in set(v1_shipped) - exposed:
    key = reason.get(name) or "(no reason recorded)"
    tally[key] = tally.get(key, 0) + 1
  for key, count in sorted(tally.items(), key=lambda item: -item[1]):
    print(f"  {count:3d}  {key}")

  if args.json:
    os.makedirs(args.json, exist_ok=True)
    inventories = [
      ("v1_functions.json", v1_shipped),
      ("v2_functions.json", v2),
      ("v2_unstable.json", sorted(unstable)),
      ("v1_groups.json", groups),
      ("v2_modules.json", modules),
      ("neo_coverage.json", {"exposed": sorted(exposed), "reasons": reason}),
    ]
    for name, payload in inventories:
      with open(os.path.join(args.json, name), "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=1)
    print(f"\nwrote inventories to {args.json}")


if __name__ == "__main__":
  main()
