"""Generate GAPS.md: a human-readable summary of Node Neo's C API coverage gaps.

Run after build_coverage.py:

    python3 tools/capi-coverage/summarize_gaps.py

A "gap" is a C API function that Node Neo does not expose, that is not deprecated and
not deliberately skipped, and that at least one other C-API-based client does expose.

Node Neo marks every unexposed function that is neither deprecated nor deliberately
skipped as `TODO:`, whatever other clients do, so gaps are a subset of the TODOs. The
rest of the TODOs -- the ones no client exposes -- are reported separately.
"""

import collections
import csv
import datetime
import os
import re
import subprocess
import sys
import textwrap

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

  def para(text):
    """Emit a prose paragraph, wrapped so interpolated numbers can't leave ragged lines."""
    for line in textwrap.wrap(" ".join(text.split()), width=88):
      w(line)
    w("")

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
  w("- **at least one other C-API-based client exposes it**.")
  w("")
  para(
    "Node Neo marks every unexposed function meeting the first two conditions as `TODO:`,"
    " whatever other clients do, so gaps are a **subset** of the TODO list: of its "
    + str(len(todo)) + " TODOs, " + str(len(gaps)) + " are gaps and the other "
    + str(len(unclaimed)) + " are functions no client exposes. Those are listed at the"
    " end and are no less wanted — they simply carry no signal either way, because nobody"
    " has built them."
  )
  w("The third condition is a **usefulness signal**: another client having surfaced a")
  w("function means someone had a concrete reason to want it. It is not a claim that the")
  w("function is easy or even sensible to bind in Node Neo — that varies by language, and")
  w("some of these will be wrong for a JS API. Weigh the signal by its breadth: a function")
  w("several clients expose is better evidence of demand than one only a single client does.")
  w("")
  w("Which layer counts as \"exposes\" differs by client, because the six do not share an")
  w("architecture. Go and C# hand-write a selective binding layer, so a binding there is a")
  w("deliberate choice and is the signal. Rust (`libduckdb-sys`) and Julia (`src/api.jl`)")
  w("auto-generate a complete binding layer, and Swift publishes no raw C layer at all — for")
  w("those three only use from the idiomatic layer means anything.")
  w("")

  w("## Where Node Neo stands")
  w("")
  w("Both layers are shown, because which one carries meaning differs by client. In each")
  w("other client's row the **bold** figure is the one this document reads as that client")
  w("exposing a function; Node Neo is the subject of the comparison, so neither of its")
  w("columns is a signal.")
  w("")
  w("| Client | Tier | Binding layer | Bindings | Bindings excl. deprecated | Idiomatic layer |")
  w("|---|---|---|---:|---:|---:|")
  for key, label, tier, kind in clients:
    bound = sum(1 for r in rows if truthy(r, key + "_binding"))
    bound_live = sum(1 for r in live if truthy(r, key + "_binding"))
    wrapped = sum(1 for r in rows if truthy(r, key + "_wrapper"))
    note = {
      "hand-written": "hand-written, selective",
      "generated": "generated, complete by construction",
      "none": "none published",
    }[kind]

    def cell(n, total, emphasise):
      text = str(n) + "/" + str(total) + " (" + str(round(100 * n / total)) + "%)"
      return "**" + text + "**" if emphasise else text

    # Node Neo is the subject of the comparison, so neither column is "its signal".
    binding_is_signal = kind == "hand-written" and key != "node_neo"
    wrapper_is_signal = kind != "hand-written"

    if kind == "none":
      binding_cells = "n/a | n/a"
    else:
      binding_cells = (
        cell(bound, len(rows), binding_is_signal)
        + " | "
        + cell(bound_live, len(live), False)
      )
    w("| " + label + " | " + tier + " | " + note + " | " + binding_cells + " | "
      + cell(wrapped, len(rows), wrapper_is_signal) + " |")
  w("")
  w("Only the hand-written binding layers are comparable to one another; the generated ones")
  w("sit at 100% by construction and say nothing about intent, which is why the idiomatic")
  w("column is what counts for Rust, Julia and Swift.")
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

  w("### How strong the signal is")
  w("")
  w("How many other clients expose each gap. More clients means better evidence that the")
  w("function is worth having, not that it is more urgent or more tractable.")
  w("")
  breadth = collections.Counter(len(covered_by(r)) for r in gaps)
  w("| Exposed by | Gaps |")
  w("|---|---:|")
  for count in sorted(breadth, reverse=True):
    label = "1 client" if count == 1 else str(count) + " clients"
    w("| " + label + " | " + str(breadth[count]) + " |")
  w("")

  single = [r for r in gaps if len(covered_by(r)) == 1]
  if single:
    who = collections.Counter(covered_by(r)[0] for r in single)
    dominant, dominant_n = who.most_common(1)[0]
    para(
      "Most gaps rest on a single client — " + str(len(single)) + " of " + str(len(gaps))
      + ", and " + str(dominant_n) + " of those on " + dominant + " alone. Those are the"
      " weakest evidence here: one project's judgement, made for one language's users."
      " The multi-client rows are the better-evidenced ones."
    )

  strong = [r for r in gaps if len(covered_by(r)) >= 3]
  if strong:
    w("Gaps exposed by three or more clients:")
    w("")
    w("| Function | Area | Exposed by |")
    w("|---|---|---|")
    for row in sorted(strong, key=lambda r: (r["node_neo_reason"], int(r["header_line"]))):
      w("| `" + row["function"] + "` | "
        + row["node_neo_reason"][len("TODO:"):].strip() + " | "
        + ", ".join(covered_by(row)) + " |")
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

  w("## The rest of the TODO list — " + str(len(unclaimed)) + " functions no client exposes")
  w("")
  w("These are also on Node Neo's TODO list, and are just as wanted or unwanted as anything")
  w("above; they simply carry no signal from other clients, because none has exposed them")
  w("either. Being behind here means being level with everyone.")
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
