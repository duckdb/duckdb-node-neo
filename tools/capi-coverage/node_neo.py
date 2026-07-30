"""Parse Node Neo's own C API accounting.

Both duckdb_node_bindings.cpp and duckdb.d.ts carry one comment block per C API
function:

    // DUCKDB_C_API <declaration>
    <a definition/export, or a reason the function is not exposed>

The .cpp is the richer source: it records a reason for every unexposed function,
whereas the .d.ts omits deprecation markers.
"""

import os
import re

root = os.path.join(os.path.dirname(__file__), "..", "..")

cpp_path = os.path.join(root, "bindings", "src", "duckdb_node_bindings.cpp")
dts_path = os.path.join(
  root, "bindings", "pkgs", "@duckdb", "node-bindings", "duckdb.d.ts"
)
api_src_dir = os.path.join(root, "api", "src")

marker_rule = re.compile(r"^//\s*DUCKDB_C_API\s+(.*)$")


def blocks(path):
  """Yield (function_name, body_lines) for each DUCKDB_C_API accounting block."""
  with open(path) as f:
    lines = f.read().split("\n")

  found = []
  name = None
  body = []
  for line in lines:
    match = marker_rule.match(line.strip())
    if match:
      if name:
        found.append((name, body))
      decl = match.group(1)
      name_match = re.search(r"\b(duckdb_[A-Za-z0-9_]+)\s*\(", decl)
      name = name_match.group(1) if name_match else decl
      body = []
    elif name is not None:
      body.append(line)
  if name:
    found.append((name, body))
  return found


def classify(body, kind):
  """Return (exposed, reason, symbol) for one accounting block."""
  # A "// function foo(...)" line documents the binding's shape, but it may also be
  # a commented-out placeholder -- so remember it without treating it as proof of
  # exposure. Only a real definition/export line decides.
  doc_symbol = None

  for line in body:
    s = line.strip()
    if not s:
      continue

    if s.startswith("// not exposed"):
      return False, s[len("// not exposed"):].lstrip(": ").strip() or None, None
    if re.fullmatch(r"//\s*deprecated\b.*", s):
      return False, "deprecated", None
    if re.match(r"//\s*TODO\b", s, re.I):
      return False, "TODO: " + re.sub(r"^//\s*TODO:?\s*", "", s, flags=re.I), None

    symbol_match = re.match(r"//\s*(?:export\s+)?function\s+([A-Za-z0-9_]+)", s)
    if symbol_match:
      doc_symbol = symbol_match.group(1)
      continue
    if s.startswith("//"):
      continue

    # first non-comment line in the block
    if kind == "dts":
      match = re.match(r"export function\s+([A-Za-z0-9_]+)", s)
      if match:
        return True, None, match.group(1)
    else:
      match = re.match(
        r"(?:Napi::Value|void|bool|static\s+\S+|template\b.*)\s*([A-Za-z0-9_]+)\s*\(", s
      )
      if match:
        return True, None, doc_symbol or match.group(1)
    break

  return False, None, None


def read_api_sources():
  out = []
  for dirpath, _, filenames in os.walk(api_src_dir):
    for filename in filenames:
      if filename.endswith(".ts"):
        with open(os.path.join(dirpath, filename), errors="replace") as f:
          out.append(f.read())
  return "\n".join(out)


def node_neo_coverage():
  """Return (binding, wrapper, reason, symbol) dicts keyed by C API function name."""
  parsed = {}
  for kind, path in (("cpp", cpp_path), ("dts", dts_path)):
    for name, body in blocks(path):
      exposed, reason, symbol = classify(body, kind)
      parsed.setdefault(name, {})[kind] = (exposed, reason, symbol)

  binding = {}
  reason = {}
  symbol = {}
  for name, per_file in parsed.items():
    cpp_exposed, cpp_reason, cpp_symbol = per_file.get("cpp", (False, None, None))
    dts_exposed, dts_reason, dts_symbol = per_file.get("dts", (False, None, None))
    binding[name] = cpp_exposed
    symbol[name] = cpp_symbol or dts_symbol
    if not cpp_exposed:
      reason[name] = cpp_reason or dts_reason

  # The idiomatic layer calls bindings as `duckdb.<snake_name>(`, and re-exports a
  # handful verbatim from api/src/duckdb.ts.
  api_text = read_api_sources()
  used = set(re.findall(r"\bduckdb\.([a-z0-9_]+)\s*\(", api_text))
  for match in re.finditer(
    r"export\s*\{([^}]*)\}\s*from\s*'@duckdb/node-bindings'", api_text, re.S
  ):
    used.update(re.findall(r"[a-z0-9_]+", match.group(1)))

  wrapper = {
    name: bool(symbol.get(name)) and symbol[name] in used for name in binding
  }
  return binding, wrapper, reason, symbol


def consistency_report():
  """Disagreements between the .cpp and .d.ts accountings, for CI-style checking."""
  parsed = {}
  for kind, path in (("cpp", cpp_path), ("dts", dts_path)):
    for name, body in blocks(path):
      parsed.setdefault(name, {})[kind] = classify(body, kind)

  issues = []
  for name, per_file in parsed.items():
    if "cpp" not in per_file:
      issues.append(name + ": missing from duckdb_node_bindings.cpp")
    elif "dts" not in per_file:
      issues.append(name + ": missing from duckdb.d.ts")
    elif per_file["cpp"][0] != per_file["dts"][0]:
      issues.append(
        name
        + ": exposed in .cpp="
        + str(per_file["cpp"][0])
        + " but .d.ts="
        + str(per_file["dts"][0])
      )
  return issues


if __name__ == "__main__":
  binding, wrapper, reason, symbol = node_neo_coverage()
  print("accounted functions: " + str(len(binding)))
  print("exposed: " + str(sum(1 for v in binding.values() if v)))
  print("reached from api layer: " + str(sum(1 for v in wrapper.values() if v)))
  issues = consistency_report()
  print("cpp/dts disagreements: " + str(len(issues)))
  for issue in issues:
    print("  " + issue)
