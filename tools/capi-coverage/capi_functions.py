"""Extract the canonical DUCKDB_C_API function list from bindings/libduckdb/duckdb.h.

This is the source of truth every client is measured against. Yields one entry per
function, carrying the `//---- Name ----//` group it sits in and whether the C API
marks it deprecated.
"""

import os
import re

header_path = os.path.join(
  os.path.dirname(__file__), "..", "..", "bindings", "libduckdb", "duckdb.h"
)

# //===--------------------------------------------------------------------===//
# // Section Name
# //===--------------------------------------------------------------------===//
section_rule = re.compile(r"^//={3}-{10,}={3}//$")

# //----------------------------------------------------------------------------//
# // Group Name
# //----------------------------------------------------------------------------//
group_rule = re.compile(r"^//-{20,}$")


def is_banner(lines, i, rule):
  """True if a three-line banner comment starts at line i."""
  return (
    i + 2 < len(lines)
    and rule.match(lines[i].strip())
    and rule.match(lines[i + 2].strip())
    and lines[i + 1].strip().startswith("//")
  )


def banner_title(lines, i):
  return lines[i + 1].strip().lstrip("/").strip()


def preceding_doc(lines, i):
  """The /*! ... */ doc comment immediately above line i, if any."""
  if i <= 0 or lines[i - 1].strip() != "*/":
    return ""
  doc = []
  k = i - 2
  while k >= 0 and not lines[k].strip().startswith("/*!"):
    doc.append(lines[k])
    k -= 1
  return "\n".join(reversed(doc))


def capi_functions(path=None):
  with open(path or header_path) as f:
    lines = f.read().split("\n")

  functions = []
  section = None
  group = None
  # Deprecated declarations sit inside `#ifndef DUCKDB_API_NO_DEPRECATED`. Track the
  # full #if stack so nested unrelated conditionals pop correctly.
  if_stack = []
  deprecated_depth = 0

  i = 0
  while i < len(lines):
    line = lines[i]
    stripped = line.strip()

    if stripped.startswith("#if"):
      is_deprecated_guard = "DUCKDB_API_NO_DEPRECATED" in stripped
      if_stack.append(is_deprecated_guard)
      if is_deprecated_guard:
        deprecated_depth += 1
    elif stripped.startswith("#endif"):
      if if_stack and if_stack.pop():
        deprecated_depth -= 1

    if is_banner(lines, i, section_rule):
      section = banner_title(lines, i)
      i += 3
      continue

    if is_banner(lines, i, group_rule):
      group = banner_title(lines, i)
      i += 3
      continue

    if line.startswith("DUCKDB_C_API"):
      # Three declarations wrap their return type and name onto separate lines,
      # so accumulate until the terminating semicolon.
      decl_lines = [line]
      j = i
      while ";" not in decl_lines[-1]:
        j += 1
        decl_lines.append(lines[j])
      decl = re.sub(r"\s+", " ", " ".join(x.strip() for x in decl_lines))

      match = re.search(r"\b(duckdb_[A-Za-z0-9_]+)\s*\(", decl)
      if not match:
        raise ValueError("unparsed declaration at duckdb.h:" + str(i + 1) + ": " + decl)

      doc = preceding_doc(lines, i)
      functions.append({
        "function": match.group(1),
        "group": group,
        "section": section,
        "header_line": i + 1,
        "declaration": decl.replace("DUCKDB_C_API ", "", 1),
        "deprecated": (
          deprecated_depth > 0
          or "DEPRECATION NOTICE" in doc
          or "**DEPRECATED**" in doc
        ),
      })
      i = j + 1
      continue

    i += 1

  names = [f["function"] for f in functions]
  if len(names) != len(set(names)):
    raise ValueError("duplicate function names in duckdb.h")

  return functions


if __name__ == "__main__":
  fns = capi_functions()
  print("functions: " + str(len(fns)))
  print("deprecated: " + str(sum(1 for f in fns if f["deprecated"])))
  print("groups: " + str(len(set(f["group"] for f in fns))))
