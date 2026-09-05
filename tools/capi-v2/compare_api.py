#!/usr/bin/env python3
"""Compare the DuckDB C API V1 and V2 surfaces, and Node Neo's coverage of them.

Companion to ../capi-coverage, which measures Node Neo against the other five
C-API clients. This one measures V1 against V2, for the DuckDB 2.0 migration. The
V1 side -- the canonical function list and Node Neo's exposure of it -- is reused
from that tool rather than re-derived, so both stay in agreement.

The V2 API is still in flux on the duckdb v2.0-cyanoptera branch, so nothing here
is hard-coded; every count comes from the headers.

    python3 tools/capi-v2/compare_api.py --fetch      # download the 2.0-branch headers
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

from capi_functions import capi_functions  # noqa: E402
from node_neo import node_neo_coverage  # noqa: E402

branch = "v2.0-cyanoptera"
raw_url = "https://raw.githubusercontent.com/duckdb/duckdb/{branch}/src/include/{name}"
cache_dir = os.path.join(here, ".headers")

# V2 declarations look the same as V1's, but V2 wraps them across lines more often.
declaration_rule = re.compile(r"^DUCKDB_C_API\s+((?:[^;])*?);", re.M | re.S)
name_rule = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s*\(")

# V2 groups functions under banner comments inside a block comment, rather than V1's
# //---- rules:
#     /* ====...
#      * MODULE: vector
#      * ====... */
module_rule = re.compile(r"^ \* MODULE: (.*)$")


def fetch_headers():
  os.makedirs(cache_dir, exist_ok=True)
  for name in ("duckdb.h", "duckdb_v2.h"):
    url = raw_url.format(branch=branch, name=name)
    print(f"fetching: {url}")
    urllib.request.urlretrieve(url, os.path.join(cache_dir, name))


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


def v1_drift(shipped, on_branch, exposed):
  """What changed in the V1 surface itself between 1.5.5 and 2.0."""
  removed = sorted(set(shipped) - set(on_branch))
  added = sorted(set(on_branch) - set(shipped))
  changed = sorted(
    name for name in set(shipped) & set(on_branch)
    if shipped[name] != on_branch[name]
  )

  print(f"\nV1 drift into 2.0: {len(removed)} removed, {len(added)} added, "
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
                      help=f"download duckdb.h and duckdb_v2.h from {branch}")
  parser.add_argument("--json", metavar="DIR", help="write inventories as JSON into DIR")
  args = parser.parse_args()

  if args.fetch:
    fetch_headers()

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

  deprecated = sum(1 for f in v1_functions if f["deprecated"])
  print(f"V1 (shipped, bindings/libduckdb/duckdb.h): {len(v1_shipped)} functions "
        f"({deprecated} deprecated)")
  if v1_on_branch:
    print(f"V1 (on {branch}):                  {len(v1_on_branch)} functions")
  print(f"V2 (duckdb_v2.h on {branch}):      {len(v2)} functions")
  print(f"Node Neo exposes:                          {len(exposed)} of "
        f"{len(v1_shipped)} V1 functions")

  if v1_on_branch:
    v1_drift(v1_shipped, v1_on_branch, exposed)

  groups = {}
  for function in v1_functions:
    groups.setdefault(function["group"], []).append(function["function"])

  print(f"\n{'V1 group':<38} {'total':>6} {'in Neo':>7}")
  print("-" * 53)
  for title, names in groups.items():
    print(f"{title:<38} {len(names):6d} "
          f"{sum(1 for n in names if n in exposed):7d}")

  modules = v2_modules(v2_path)
  print(f"\n{'V2 module':<38} {'total':>6}")
  print("-" * 46)
  for title, names in modules.items():
    print(f"{title:<38} {len(names):6d}")

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
