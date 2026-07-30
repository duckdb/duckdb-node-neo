"""Build the combined C API coverage dataset across the six C-API-based DuckDB clients.

Run tools/capi-coverage/fetch_clients.py first, then:

    python3 tools/capi-coverage/build_coverage.py

Writes capi_coverage.csv and capi_coverage.json next to this script -- one row per
DUCKDB_C_API function, with a binding and a wrapper flag per client. See README.md for
what those two signals mean and why both are needed.
"""

import csv
import json
import os
import sys

from capi_functions import capi_functions
from node_neo import node_neo_coverage
import other_clients

out_dir = os.path.dirname(os.path.abspath(__file__))

# (key, label, support tier, detector, has a meaningful binding layer)
clients = [
  ("node_neo", "Node Neo", "primary", node_neo_coverage, True),
  ("go", "Go", "primary", other_clients.go, True),
  ("rust", "Rust", "primary", other_clients.rust, False),
  ("csharp", "C#", "secondary", other_clients.csharp, True),
  ("swift", "Swift", "tertiary", other_clients.swift, False),
  ("julia", "Julia", "tertiary", other_clients.julia, False),
]


def main():
  functions = capi_functions()
  canonical = set(f["function"] for f in functions)
  print("canonical DUCKDB_C_API functions: " + str(len(functions)))
  print("deprecated in the C API: " + str(sum(1 for f in functions if f["deprecated"])))
  print("")

  coverage = {}
  for key, label, tier, detect, selective in clients:
    if key == "node_neo":
      binding, wrapper, reason, symbol = detect()
      missing = canonical - set(binding)
      extra = set(binding) - canonical
      if missing or extra:
        print(
          "WARNING: Node Neo accounting is out of sync with duckdb.h -- "
          "missing " + str(sorted(missing)) + ", extra " + str(sorted(extra)),
          file=sys.stderr,
        )
    else:
      binding, wrapper, reason, symbol = detect(canonical)

    coverage[key] = {
      "binding": binding,
      "wrapper": wrapper,
      "reason": reason,
      "symbol": symbol,
    }
    bound = sum(1 for v in binding.values() if v)
    wrapped = sum(1 for v in wrapper.values() if v)
    note = "" if selective else "   (auto-generated or absent: not comparable)"
    print(
      label.ljust(10)
      + tier.ljust(11)
      + ("binding " + str(bound) + "/" + str(len(functions))).ljust(16)
      + "wrapper " + str(wrapped) + "/" + str(len(functions))
      + note
    )

  rows = []
  for f in functions:
    name = f["function"]
    row = {
      "function": name,
      "group": f["group"],
      "deprecated_in_c_api": f["deprecated"],
      "header_line": f["header_line"],
    }
    for key, label, tier, detect, selective in clients:
      c = coverage[key]
      row[key + "_binding"] = bool(c["binding"].get(name, False))
      row[key + "_wrapper"] = bool(c["wrapper"].get(name, False))
      row[key + "_symbol"] = c["symbol"].get(name) or ""
      row[key + "_reason"] = c["reason"].get(name) or ""
    rows.append(row)

  csv_path = os.path.join(out_dir, "capi_coverage.csv")
  with open(csv_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)

  json_path = os.path.join(out_dir, "capi_coverage.json")
  with open(json_path, "w") as f:
    json.dump(rows, f, indent=1)

  print("")
  print("wrote " + str(len(rows)) + " rows to:")
  print("  " + csv_path)
  print("  " + json_path)
  return 0


if __name__ == "__main__":
  sys.exit(main())
