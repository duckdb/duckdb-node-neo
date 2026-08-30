"""Generate GAPS.md: a human-readable summary of Node Neo's C API coverage gaps.

Run after build_coverage.py:

    python3 tools/capi-coverage/summarize_gaps.py

A "gap" is a C API function that Node Neo does not expose, that is not deprecated and
not deliberately skipped, and that at least one other C-API-based client does expose.
In Node Neo's own accounting those are exactly the functions marked `TODO:`.
"""

import collections
import csv
import datetime
import os
import re
import subprocess
import sys

out_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(out_dir, "capi_coverage.csv")
clients_dir = os.path.join(out_dir, "clients")
fetch_script = os.path.join(
  out_dir, "..", "..", "bindings", "scripts", "fetch_libduckdb_osx_universal.py"
)

# How "exposed by another client" is decided, per client. Go and C# hand-write a
# selective binding layer, so their binding column is the real signal. Rust and Julia
# auto-generate a complete one and Swift publishes no raw C layer at all, so for those
# three only idiomatic-layer use means anything. See README.md.
sources = [
  ("Go", "go_binding"),
  ("C#", "csharp_binding"),
  ("Rust", "rust_wrapper"),
  ("Swift", "swift_wrapper"),
  ("Julia", "julia_wrapper"),
]

clients = [
  ("node_neo", "Node Neo", "primary", "hand-written"),
  ("go", "Go", "primary", "hand-written"),
  ("rust", "Rust", "primary", "generated"),
  ("csharp", "C#", "secondary", "hand-written"),
  ("swift", "Swift", "tertiary", "none"),
  ("julia", "Julia", "tertiary", "generated"),
]

repo_labels = {
  "duckdb-go-bindings": "Go (duckdb/duckdb-go-bindings)",
  "duckdb-rs": "Rust (duckdb/duckdb-rs)",
  "DuckDB.NET": "C# (Giorgi/DuckDB.NET)",
  "duckdb-swift": "Swift (duckdb/duckdb-swift)",
  "DuckDB.jl": "Julia (duckdb/DuckDB.jl)",
}


def truthy(row, column):
  return row[column] == "True"


def covered_by(row):
  return [name for name, column in sources if truthy(row, column)]


def duckdb_version():
  """duckdb.h carries no version macro; the vendored release is named in the fetch script."""
  try:
    with open(fetch_script) as f:
      match = re.search(r"/download/v([0-9]+\.[0-9]+\.[0-9]+)/", f.read())
    if match:
      return match.group(1)
  except OSError:
    pass
  return "an unknown version"


def client_revisions():
  revisions = []
  for name, label in repo_labels.items():
    path = os.path.join(clients_dir, name)
    if not os.path.isdir(path):
      continue
    try:
      out = subprocess.run(
        ["git", "log", "-1", "--format=%h on %ad", "--date=short"],
        cwd=path, capture_output=True, text=True, check=True,
      ).stdout.strip()
      revisions.append((label, out))
    except (subprocess.CalledProcessError, OSError):
      continue
  return revisions


def main():
  if not os.path.exists(csv_path):
    print("missing " + csv_path + " -- run build_coverage.py first", file=sys.stderr)
    return 1

  with open(csv_path) as f:
    rows = list(csv.DictReader(f))

  today = datetime.date.today().isoformat()
  live = [r for r in rows if not truthy(r, "deprecated_in_c_api")]

  todo = [r for r in rows if r["node_neo_reason"].startswith("TODO:")]
  # TODO and deprecated are disjoint categories in Node Neo's accounting; assert rather
  # than assume, since the whole framing of this document depends on it.
  mislabelled = [r["function"] for r in todo if truthy(r, "deprecated_in_c_api")]
  if mislabelled:
    print("WARNING: TODO functions marked deprecated: " + str(mislabelled), file=sys.stderr)

  gaps = [r for r in todo if covered_by(r)]
  unclaimed = [r for r in todo if not covered_by(r)]

  by_area = collections.defaultdict(list)
  for row in gaps:
    by_area[row["node_neo_reason"][len("TODO:"):].strip()].append(row)

  unclaimed_areas = collections.Counter(
    r["node_neo_reason"][len("TODO:"):].strip() for r in unclaimed
  )

  L = []
  w = L.append

  w("# Node Neo C API coverage gaps")
  w("")
  w("_Generated " + today + " against DuckDB " + duckdb_version() + " ("
    + str(len(rows)) + " `DUCKDB_C_API` functions)._")
  w("")
  w("This is a generated summary of [capi_coverage.csv](capi_coverage.csv). Regenerate both")
  w("with `build_coverage.py` then `summarize_gaps.py`; see [README.md](README.md) for how")
  w("coverage is detected and what its limits are.")
  w("")

  w("## What counts as a gap")
  w("")
  w("A **gap** is a C API function that Node Neo does not expose, where:")
  w("")
  w("- it is **not deprecated** in the C API, and")
  w("- Node Neo has **not deliberately skipped** it (`destroyed in finalizer`,")
  w("  `consolidated into open`, and similar), and")
  w("- **at least one other C-API-based client does expose it** — so it is demonstrably")
  w("  bindable, and there is a reference implementation to work from.")
  w("")
  w("In Node Neo's own accounting those are exactly the functions marked `TODO:`, which is")
  w("what this document reports. Functions no client exposes are listed separately at the")
  w("end: they are unbuilt C API surface generally, not somewhere Node Neo trails its peers.")
  w("")
  w("\"Exposed by another client\" is read differently per client, because the six do not")
  w("share an architecture. Go and C# hand-write a selective binding layer, so a binding")
  w("existing there is a real signal. Rust (`libduckdb-sys`) and Julia (`src/api.jl`)")
  w("auto-generate a complete one, and Swift publishes no raw C layer at all — for those")
  w("three only use from the idiomatic layer counts.")
  w("")

  w("## Where Node Neo stands")
  w("")
  w("| Client | Tier | Binding layer | Bindings | Excl. deprecated |")
  w("|---|---|---|---:|---:|")
  for key, label, tier, kind in clients:
    bound = sum(1 for r in rows if truthy(r, key + "_binding"))
    bound_live = sum(1 for r in live if truthy(r, key + "_binding"))
    note = {
      "hand-written": "hand-written",
      "generated": "generated (complete by construction)",
      "none": "none published",
    }[kind]
    if kind == "none":
      cells = "n/a | n/a"
    else:
      cells = (
        str(bound) + "/" + str(len(rows)) + " (" + str(round(100 * bound / len(rows))) + "%)"
        + " | " + str(bound_live) + "/" + str(len(live))
        + " (" + str(round(100 * bound_live / len(live))) + "%)"
      )
    w("| " + label + " | " + tier + " | " + note + " | " + cells + " |")
  w("")
  w("Only the hand-written rows are comparable to each other; the generated ones sit at")
  w("100% by construction and say nothing about intent.")
  w("")

  nn_todo = len(todo)
  w("Node Neo's " + str(nn_todo) + " unexposed-but-wanted functions split into **"
    + str(len(gaps)) + " gaps** (below) and **" + str(len(unclaimed))
    + " that no client has bound**.")
  w("")

  w("## The gaps — " + str(len(gaps)) + " functions across " + str(len(by_area)) + " areas")
  w("")
  w("Ordered by size. \"Exposed by\" counts how many functions in that area each client has.")
  w("")
  w("| Area | Gap | Exposed by |")
  w("|---|---:|---|")
  for area, group in sorted(by_area.items(), key=lambda kv: (-len(kv[1]), kv[0])):
    who = collections.Counter()
    for row in group:
      who.update(covered_by(row))
    listing = ", ".join(name + " " + str(count) for name, count in who.most_common())
    w("| " + area + " | " + str(len(group)) + " | " + listing + " |")
  w("")

  solo = collections.defaultdict(list)
  for area, group in by_area.items():
    who = set()
    for row in group:
      who.update(covered_by(row))
    if len(who) == 1:
      solo[who.pop()].append((area, len(group)))
  if solo:
    w("### Areas with a single reference implementation")
    w("")
    for client, areas in sorted(solo.items(), key=lambda kv: -sum(n for _, n in kv[1])):
      total = sum(n for _, n in areas)
      names = ", ".join(a for a, _ in sorted(areas, key=lambda x: -x[1]))
      plural = " area: " if len(areas) == 1 else " areas: "
      w("- **" + client + " only** — " + str(total) + " functions across "
        + str(len(areas)) + plural + names)
    w("")

  w("### Function detail")
  w("")
  for area, group in sorted(by_area.items(), key=lambda kv: (-len(kv[1]), kv[0])):
    w("#### " + area + " (" + str(len(group)) + ")")
    w("")
    w("| Function | Exposed by |")
    w("|---|---|")
    for row in sorted(group, key=lambda r: int(r["header_line"])):
      w("| `" + row["function"] + "` | " + ", ".join(covered_by(row)) + " |")
    w("")

  w("## Not a gap: unclaimed by every client — " + str(len(unclaimed)) + " functions")
  w("")
  w("Node Neo does not expose these, but neither does any other client. Being behind here")
  w("means being level with everyone, which is a different prioritisation signal.")
  w("")
  w("| Area | Functions |")
  w("|---|---:|")
  for area, count in unclaimed_areas.most_common():
    w("| " + area + " | " + str(count) + " |")
  w("")

  revisions = client_revisions()
  if revisions:
    w("## Client revisions compared")
    w("")
    w("Each client tracks its own DuckDB version, so some gaps are release lag rather than")
    w("a decision.")
    w("")
    for label, rev in revisions:
      w("- " + label + " — " + rev)
    w("")

  path = os.path.join(out_dir, "GAPS.md")
  with open(path, "w") as f:
    f.write("\n".join(L) + "\n")
  print("wrote " + path + " (" + str(len(gaps)) + " gaps across "
        + str(len(by_area)) + " areas)")
  return 0


if __name__ == "__main__":
  sys.exit(main())
