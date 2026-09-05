# DuckDB C API V2: survey and comparison with V1

Working notes for migrating Node Neo to the C API that ships with DuckDB 2.0. Nothing here
is a commitment; this is the fact-finding pass that a migration plan gets built on.

Sources, as of 2026-09-05:

- `duckdb_v2.h` on the [`v2.0-cyanoptera`](https://github.com/duckdb/duckdb/tree/v2.0-cyanoptera)
  branch — 12,811 lines, 527 functions.
- `duckdb.h` on the same branch — the V1 surface as it will ship in 2.0.
- `bindings/libduckdb/duckdb.h` — the V1 surface Node Neo builds against today (DuckDB 1.5.5).
- The three PRs that landed V2: [#24702](https://github.com/duckdb/duckdb/pull/24702) (part 1:
  errors, environment, database, connection, context, SQL statements, streaming results, logical
  types, values, data chunks, vectors), [#25184](https://github.com/duckdb/duckdb/pull/25184)
  (part 2: scalar and aggregate functions, column data collections),
  [#25340](https://github.com/duckdb/duckdb/pull/25340) (part 3: table, copy and cast functions,
  file system, replacement scans, arrow, catalog, logging).

Both PR descriptions carry the same caveat — nothing is set in stone, more follow-ups are
expected — so treat every count below as a snapshot. `compare_api.py` re-derives all of them
from the headers.

Companion to [`../capi-coverage`](../capi-coverage/README.md), which measures Node Neo against the
other five C-API clients. `compare_api.py` reuses that tool's `capi_functions.py` (the canonical
V1 function list) and `node_neo.py` (our exposure of it, and the recorded reason for each
non-exposure), so the two cannot drift apart on the V1 side. What is new here is parsing
`duckdb_v2.h` — which groups functions under `MODULE:` banners rather than V1's `//----` rules —
and diffing the V1 surface against itself across the 2.0 boundary.

## Reproducing this

```bash
python3 tools/capi-v2/compare_api.py --fetch   # pull duckdb.h + duckdb_v2.h from the branch
python3 tools/capi-v2/compare_api.py           # print the comparison report
python3 tools/capi-v2/fetch_libduckdb_v2.py    # pull the 2.0 preview library for this platform
cd tools/capi-v2 && cc -std=c11 -I libduckdb-v2 smoke_v2.c -L libduckdb-v2 -lduckdb \
    -Wl,-rpath,@loader_path/libduckdb-v2 -o smoke_v2 && ./smoke_v2
```

`smoke_v2.c` opens an in-memory database, runs a query and reads a chunk back through V2 — the
shortest illustration of the calling convention the bindings have to adopt. `coexist_probe.c`
builds the same way, and checks whether one database file opened through both V1 and V2 is
detected.

## Headline findings

**V1 keeps working in 2.0, but the clock is now running.** Comparing the V1 header we build against
with the V1 header on the 2.0 branch: zero functions removed, two added
(`duckdb_create_timestamp_tz_ns`, `duckdb_get_timestamp_tz_ns`), and thirteen "signature changes"
that are all the cosmetic `()` → `(void)`. Nothing in the header carries `DUCKDB_DEPRECATED` — the
macro is defined but never applied. The preview `libduckdb` exports all 546 V1 symbols alongside all
527 V2 symbols from one library, so V1 and V2 can be mixed inside one addon, and an incremental,
module-by-module migration is possible rather than a big-bang rewrite.

What the header does not show is the schedule. Per the DuckDB team (maxxen, 2026-09-05): **V1 is
deprecated in 2.0 and probably removed entirely in 3.0, likely about a year out.** So 2.0 is not a
forced migration, but it is the start of a fixed window rather than an open-ended coexistence — see
[Migration approach](#migration-approach).

The one thing that notices is our own signature check: `checkFunctionSignatures.mjs` compares
declaration text exactly, so those thirteen `(void)` edits make it report a mismatch until
`bindingsSigs.json` / `headerSigs.json` / `typeDefsSigs.json` are regenerated. It warns rather than
exits non-zero, and nothing invokes it — not `pnpm run build`, not CI — so this is a manual step
that will not announce itself.

**The preview binaries exist, under a different naming scheme.** The install page only links the
CLI, and `duckdb-binaries-<platform>.zip` (the 1.4/1.5 preview convention) 404s for
`v2.0-cyanoptera`. The libraries are published as tarballs instead:

```
https://artifacts.duckdb.org/v2.0-cyanoptera/duckdb-shared-libs-<suffix>.tar.gz
```

All seven suffixes Node Neo targets are live: `osx-universal`, `linux-amd64`, `linux-arm64`,
`linux-amd64-musl`, `linux-arm64-musl`, `windows-amd64`, `windows-arm64`. Each tarball contains
`libduckdb.{dylib,so,dll}`, `duckdb.h`, `duckdb_v2.h`, `duckdb_extension.h` and
`duckdb_extension_v2.h`. `fetch_libduckdb_v2.py` fetches one; it is deliberately separate from
`bindings/scripts/fetch_libduckdb_*.py`, which points at GitHub releases and cannot reach these.
Note the shape difference — tarball, not zip, and no `libduckdb/` prefix inside — so the existing
fetch scripts need more than a URL swap when 2.0 releases. The release workflow already zips
`duckdb_v2.h` next to `duckdb.h`, so the final release artifacts will carry both headers.

**V2 is a redesign, not a rename.** Function-by-function translation does not exist for most of
the surface. Roughly: a third of what Node Neo uses maps mechanically, a third needs restructuring
around new call sequences, and a third has no counterpart yet.

**The appender is not in V2, and is not coming.** Node Neo exposes 31 appender functions and both
packages have appender APIs and test suites. Nothing in `duckdb_v2.h` matches, and per the DuckDB
team (maxxen, 2026-09-05) that is deliberate: "probably not, this can now be implemented client-side
using columndatacollections and replacement scans/table functions." So this becomes work we own — a
V2 `DuckDBAppender` built on `column_data_collection` plus `replacement_scan_set_collection`, rather
than a thin wrapper over C functions. Note the shape difference that makes it more than a rename: a
collection surfaced through a replacement scan is a *virtual table*, so appending to an *existing*
table means an `INSERT INTO … SELECT * FROM` over it, not a direct append.

**One class of hazard in our function-info handling goes away.** V2 gives every function family its
own info-handle type (`duckdb_v2_scalar_function_bind_info_handle`,
`duckdb_v2_table_function_exec_info_handle`, and so on), where V1 shares one `duckdb_function_info`
across all of them. `type_tags.h` stays either way — a tag is the only thing that distinguishes one
`Napi::External` from another at runtime — but the six function-info tags stop carrying extra load;
see §12. Separately, the `duckdb_cpp` "stable C++ API" over V2, mentioned in all three PRs, turns out
to live at `tools/cpp/` rather than `src/include/`, which is why it is not in the packaged headers.
The team expects to add it to the release tarballs "soon", along with a CMake module for
`FetchContent` (maxxen, 2026-09-05). As a C++ N-API addon we are its natural consumer — see
[Migration approach](#migration-approach) for what that would take.

## The two surfaces, side by side

| | V1 (1.5.5) | V1 (on 2.0) | V2 |
|---|---:|---:|---:|
| Functions | 546 | 548 | 527 |
| Exposed by Node Neo | 314 | — | — |

Node Neo's V1 coverage, by header section — this is the actual migration surface, not the 546:

| V1 group | total | in Neo | V2 status |
|---|---:|---:|---|
| Open Connect | 17 | 12 | restructured (environment is new and mandatory) |
| Configuration | 5 | 4 | restructured (`option` handles; enumeration now needs a database) |
| Error Data | 5 | 0 | replaced by `error_info` |
| Query Execution | 18 | 12 | restructured (parse → iterate → bind → execute → step) |
| Safe Fetch Functions | 24 | 0 | gone (was deprecated) |
| Helpers | 7 | 1 | mostly gone (no `duckdb_vector_size`, no malloc/free) |
| Date Time Timestamp Helpers | 13 | 13 | **absent** |
| Hugeint and Uhugeint Helpers | 4 | 4 | **absent** |
| Decimal Helpers | 2 | 2 | **absent** |
| Prepared Statements | 13 | 12 | restructured (4 functions; metadata moved to `schema`) |
| Bind Values to Prepared Statements | 25 | 24 | replaced by parameter arrays at execute time |
| Execute Prepared Statements | 2 | 2 | `prepared_statement_execute` |
| Extract Statements | 4 | 3 | replaced by `parse_sql` + statement iterator |
| Pending Result Interface | 8 | 7 | replaced by `result_step` + `result_wait` |
| Value Interface | 77 | 74 | present, but every constructor needs a connection or context |
| Logical Type Interface | 30 | 28 | collapsed into generic construct-by-name/id/text + `get_param` |
| Data Chunk Interface | 7 | 6 | close mapping |
| Vector Interface | 19 | 14 | restructured around `vector_view` |
| Validity Mask Functions | 4 | 4 | `vector_flat_get_validity_mutable` + `vector_set_null` |
| Scalar Functions | 33 | 26 | close mapping, per-phase info handles |
| Selection Vector Interface | 3 | 0 | folded into `vector_view.sel` |
| Aggregate Functions | 16 | 0 | present (49 functions) |
| Table Functions (+ Bind/Init/exec) | 33 | 33 | close mapping, plus filter and projection pushdown |
| Replacement Scans | 4 | 0 | present (15 functions) |
| Profiling Info | 5 | 0 | **absent** |
| Appender | 40 | 31 | **absent** |
| Table Description | 8 | 0 | present as `catalog` |
| Arrow Interface | 19 | 0 | present, redesigned (importer/exporter) |
| Threading Information | 8 | 0 | **absent** |
| Streaming Result Interface | 2 | 1 | subsumed — every result streams |
| Cast Functions | 12 | 0 | present |
| Expression Interface | 4 | 0 | present (9 functions) |
| File System Interface | 16 | 0 | present |
| Config Options Interface | 9 | 0 | folded into `option` |
| Copy Functions | 36 | 0 | present (72 functions) |
| Catalog Interface | 7 | 0 | present |
| Logging | 6 | 0 | reduced to `context_log` |
| Geometry Helpers | 1 | 1 | folded into logical type params |

## Taxonomy of differences

### 1. Naming

Every function is `duckdb_v2_*`; every type is `duckdb_v2_*_handle`; every enum is
`DUCKDB_V2_*`. Mechanical, but it means V1 and V2 can coexist in one translation unit, which is
what makes incremental migration viable.

### 2. Return convention

Everything returns a status. Products go to out-params.

```c
/* V1 */ duckdb_logical_type duckdb_create_list_type(duckdb_logical_type type);
/* V1 */ idx_t               duckdb_column_count(duckdb_result *result);
/* V1 */ duckdb_state        duckdb_query(duckdb_connection, const char *, duckdb_result *);

/* V2 */ DUCKDB_V2_ERROR duckdb_v2_schema_get_count(
             duckdb_v2_schema_handle schema, idx_t *out_count, duckdb_v2_error_info_handle *err);
```

This affects essentially all 314 call sites in `duckdb_node_bindings.cpp`. It is the single
largest mechanical cost of the migration, and it is exactly the kind of boilerplate a
`duckdb_cpp.hpp` (or our own wrapper) would absorb.

### 3. Error reporting

V1 has three mechanisms — `duckdb_state`, `duckdb_result_error`/`duckdb_appender_error` per
object, and the 1.5-era `duckdb_error_data`. Node Neo implements none of the third and reads
errors off each object.

V2 has one: a `DUCKDB_V2_ERROR` code plus an optional `duckdb_v2_error_info_handle` written to a
trailing `err` out-param, which the caller owns and destroys. The codes are a real taxonomy
(`IO_FILE_NOT_FOUND`, `INPUT_OUT_OF_RANGE`, `RESOURCE_IN_USE`, `TYPE_CONVERSION`, `QUERY_BINDER`,
…) rather than `DuckDBSuccess`/`DuckDBError`, and `error_info_get_text` /
`error_info_get_raw_message` split the formatted message from the raw one.

Upside for the API package: `DuckDBError` could finally carry a typed code instead of a string.

### 4. Strings

V1 returns `const char *` (borrowed) or `char *` (owned, freed with `duckdb_free`), and takes
null-terminated input.

V2 uses `duckdb_v2_str { const char *ptr; idx_t len; }` — a borrowed view, never owning,
explicitly not null-terminated and possibly containing interior nulls. `duckdb_v2_identifier_t`
is the same layout, tagging a string as a case-insensitively-matched SQL identifier.

For rendering, V2 uses caller-supplied buffers with a size-query pass:
`logical_type_to_text(type, NULL, 0, &length, err)` reports the length,
`logical_type_to_text(type, buf, capacity, &length, err)` writes it. Nothing is allocated on the
caller's behalf.

This is a good fit for N-API — `Napi::String::New(env, ptr, len)` is a direct match, and it
removes our `duckdb_free` bookkeeping — but every string-handling helper in
`conversion_helpers.h` needs rewriting.

Separately, `duckdb_v2_bytes` is the transparent 16-byte in-vector storage for VARCHAR / BLOB /
BIT / BIGNUM, replacing `duckdb_string_t` and its `duckdb_string_is_inlined` /
`duckdb_string_t_data` accessors with directly-readable union fields.

### 5. Ownership

V1's convention is implicit and per-function. V2 documents owned versus borrowed on every
out-param, and the distinction is enforced: `schema_get_field` hands back a **borrowed** logical
type that must not be destroyed, while `logical_type_get_param` hands back an **owned** value that
must be. Getting this wrong aborts the process — the first draft of `smoke_v2.c` did exactly that.

`externals.h` currently attaches a finalizer per external. Under V2 that model needs to know, per
handle, whether this particular handle owns its resource, and borrowed handles need their parent's
lifetime pinned. This is the subtlest part of the migration and the easiest to get quietly wrong.

### 6. New required root: the environment

```c
duckdb_v2_create_environment(&env, &err);
duckdb_v2_open(env, path, options, option_count, &db, &err);
```

There is no `duckdb_v2_open` without an environment. Databases under one environment share a
cache, so the environment subsumes `duckdb_instance_cache` — which has no V2 counterpart — and
makes what was opt-in (`DuckDBInstanceCache`) structural. Destroying an environment while a
database is still open returns `ERROR_RESOURCE_IN_USE`.

### 7. Configuration

V1: build a `duckdb_config` blob of string key/value pairs, pass it to `duckdb_open_ext`.

V2: build individual `duckdb_v2_option_handle`s and pass an array to `duckdb_v2_open`; set them
later per-database (`database_option_set`) or per-connection with a scope
(`connection_option_set(conn, option, scope, err)`). An option handle resolved through a get also
carries description, default, target scope and aliases — richer than V1's
`duckdb_get_config_flag`.

One regression for us: V1 enumerates available options with no database open
(`duckdb_config_count` / `duckdb_get_config_flag`), which is what `configurationOptionDescriptions.ts`
relies on. V2 enumerates via `database_option_get_count` / `database_option_get_by_index`, so a
database is required.

### 8. Query execution

The biggest restructuring. V1:

```c
duckdb_query(conn, sql, &result);                      /* materialized */
duckdb_prepare(conn, sql, &stmt);
duckdb_bind_int32(stmt, 1, 42);                        /* one call per parameter, by index */
duckdb_execute_prepared(stmt, &result);
duckdb_pending_prepared(stmt, &pending);               /* or, for async */
duckdb_pending_execute_task(pending);                  /* pump until ready */
```

V2:

```c
duckdb_v2_parse_sql(conn, sql, &iterator, &err);       /* also replaces extract_statements */
duckdb_v2_statement_iterator_next(iterator, &stmt, &err);
duckdb_v2_statement_bind(conn, stmt, &schema, &parameters, &err);   /* optional: metadata */
duckdb_v2_statement_execute(conn, stmt, names, values, count, &result, &err);
```

Three consequences:

- **Parameters are arrays at execute time.** Two parallel arrays of `duckdb_v2_identifier_t` names
  and `duckdb_v2_value_handle` values, replacing all 25 `duckdb_bind_*` functions. Named and
  positional parameters unify. Our `DuckDBPreparedStatement` bind-then-execute shape does not
  survive as-is; the value-based path (`duckdb_bind_value`) is the closest existing thing.
- **Every result streams.** There is no materialized result and no `duckdb_row_count`.
  `duckdb_v2_result_step` returns `CHUNK`, `WAITING`, `FINISHED` or `CANCELLED`. A result is a
  live cursor: while it is open the connection refuses new queries with `ERROR_RESOURCE_IN_USE`,
  its transaction stays open, and side-effecting statements only take effect once drained. That is
  a real behavioral change for `DuckDBMaterializedResult` and for how the API package holds
  results across awaits.
- **The pending-result interface is gone, and `WAITING` replaces it.** This is arguably better
  suited to us than V1's task pump: `result_step` returning `WAITING` is a natural yield point,
  with `result_wait` as the blocking fallback on a worker thread.

Result metadata moves from per-column calls (`duckdb_column_name`, `duckdb_column_type`) to a
`duckdb_v2_schema_handle` obtained once, with `schema_get_field` returning name and type together.
`duckdb_v2_prepared_statement_*` is only four functions, because everything else moved to the
statement and schema.

### 9. Logical types

V1 has a constructor and a family of accessors per kind — `duckdb_create_list_type`,
`duckdb_create_struct_type`, `duckdb_decimal_width`, `duckdb_struct_type_child_name`,
`duckdb_enum_dictionary_value`, and so on: 30 functions, 28 of which we expose.

V2 has three constructors and two accessors:

```c
duckdb_v2_connection_create_type_from_id(conn, type_id, param_names, param_values, count, &type, &err);
duckdb_v2_connection_create_type_from_name(conn, qname, param_names, param_values, count, &type, &err);
duckdb_v2_connection_create_type_from_text(conn, str("STRUCT(a INTEGER, b VARCHAR)"), &type, &err);

duckdb_v2_logical_type_get_param_count(type, &count, &err);
duckdb_v2_logical_type_get_param(type, index, &out_name, &out_value, &err);
```

Type structure is expressed as **value parameters**: DECIMAL has 2 (width, scale as UTINYINT);
LIST 1 (element type, as a TYPE value); ARRAY 2; MAP 2; STRUCT one per field, named; UNION one per
member; ENUM one per dictionary entry, as VARCHAR. Child types cross the boundary wrapped as TYPE
values, unwrapped with `value_get_type`.

This is a much smaller surface, and it generalizes to extension types for free — but `DuckDBType.ts`
and the `types/` tree are built directly on the V1 per-kind accessors, so this is a rewrite rather
than a retarget. Also note that construction now requires a connection or context, where V1's
`duckdb_create_list_type` needed neither.

### 10. Values

Reading is a close mapping (`value_get_int`, `value_get_varchar`, `value_get_child`, …).

Creating is not: **every constructor except `value_create_null` requires a connection or a
context**, in `_with_connection` / `_with_context` pairs. `duckdb_create_int32(42)` becomes
`duckdb_v2_value_create_int_with_connection(conn, 42, &value, &err)`.

`createValue.ts` and `DuckDBValue` construct values with no connection in hand. Threading a
connection (or context) through that layer is a design change in the API package, not just the
bindings.

The context variant matters for UDFs: inside a bind or exec callback we hold a
`duckdb_v2_context_handle`, not a connection, and the header is explicit that catalog-touching
context calls belong in bind-phase callbacks rather than exec-phase worker callbacks.

### 11. Vectors

V1: `duckdb_vector_get_data` plus `duckdb_vector_get_validity`, with flat vectors assumed.

V2: one unified view.

```c
struct duckdb_v2_vector_view {
    const void            *data;
    const uint64_t        *validity;
    const duckdb_v2_sel_t *sel;
    idx_t                  count;
};
```

Non-flat vector types become first-class — dictionary, constant and sequence vectors, with
`vector_flatten` when a reader wants flat storage anyway. The header commits to storage-tier
mappings that V1 left implicit: DECIMAL width ≤ 4 stores int16, ≤ 9 int32, ≤ 18 int64, ≤ 38
int128; ENUM stores uint8 / uint16 / uint32 by dictionary size. `DuckDBVector.ts` already encodes
those rules, so this is confirmation rather than change.

Handling `sel` is new work — `getRowsFromChunks` and friends index `data` directly today — but it
is also an opportunity: reading a dictionary vector without flattening it is strictly less copying
than what V1 forces.

`duckdb_vector_size()` has no V2 counterpart, and there is no `DUCKDB_V2_VECTOR_SIZE` constant.

### 12. User-defined functions

The closest-mapping area, and the one that narrows a real bug class for us.

V1 passes one opaque `duckdb_function_info` to exec, and one `duckdb_bind_info` / `duckdb_init_info`
to bind and init, across scalar, table, aggregate and cast functions. So our six function-info tags
— `ScalarFunctionBindInfoTypeTag`, `TableFunctionInfoTypeTag` and the rest — wrap only three
underlying C types between them, and the tag is the *only* thing separating a scalar bind info from
a table one. The structs behind those handles are unrelated and get `reinterpret_cast` without a
check, so a mix-up corrupts memory rather than failing.

V2 gives each phase of each family its own handle type — 30 distinct `*_info_handle` types — so the
C type system separates them and those tags become ordinary externals tags like the other sixteen.

To be clear about what this does *not* do: `type_tags.h` stays. Every external we hand to JS needs a
tag, because N-API gives us no other way to tell one `Napi::External` from another at runtime. What
changes is that the function-info tags stop being load-bearing against memory corruption, so the
special-case reasoning around them can go. If anything the tag count goes up, since V2 splits each
family into more phases.

New capability worth noting: table functions gain **filter pushdown** (claim-based, via the
expression interface) and projection pushdown, alongside progress reporting. Scalar functions gain
a bind phase that can set the return type from argument types.

Function registration also picks up a `function_signature` object (`add_parameter`,
`set_varargs`, `set_return_type`) and named parameters with defaults.

## Not in V2 (yet)

Ordered by what it would cost us:

- **Appender** (31 exposed functions, `DuckDBAppender.ts`, two test suites). No counterpart, and
  none planned — see the headline finding. We reimplement it over `column_data_collection`.
- **Date / time / timestamp / hugeint / decimal conversion helpers** (19 exposed functions, all of
  them). `duckdb_from_date`, `duckdb_to_timestamp`, `duckdb_double_to_decimal` and the rest have no
  V2 equivalent, and still none as of the latest branch fetch — the header contains no `static
  inline` helpers at all. The omission is deliberate: the team is "trying to avoid *convenience*
  functions", and expects clients to implement them, though some may yet ship as inline functions in
  the header or as documentation (maxxen, 2026-09-05, hedged — worth re-checking near release). For
  us these are pure computation over plain structs, so TypeScript implementations are
  straightforward, and would remove 19 native round-trips in the process.
- **`duckdb_vector_size`**, and the malloc/free helpers.
- **Profiling info** (not exposed today).
- **Task / threading control** — `duckdb_execute_tasks` and friends (not exposed today).
- **Instance cache** — subsumed by the environment, but the mapping is not one-to-one
  (`DuckDBInstanceCache` in the API package is a public type).

## What we would gain

- Typed error codes instead of message-string matching.
- Per-family function info handles, so the function-info type tags stop guarding against memory
  corruption and become ordinary externals tags.
- Dictionary and constant vector reading without a flatten.
- `WAITING`-based streaming that fits an async binding better than the pending-result pump.
- Table function filter and projection pushdown.
- Catalog / table description, cast functions, copy functions, replacement scans, file system and
  expression interfaces — all present in V1 too, all unexposed by us today, all with a smaller and
  more consistent surface in V2.
- `qname` (qualified-name parsing, rendering, comparison) and `identifier_render_quoted`, which
  would replace hand-rolled quoting in `sql.ts`.
- `column_data_collection` — build a set of chunks and register it as a table via a replacement
  scan. Not an appender, but a bulk-load path with no V1 equivalent.

## Migration approach

Decided (Jeff, 2026-09-05). Everything still open is a question for the DuckDB team, not for us.

### Bindings: V2 alongside V1, in the same package and binary

`@duckdb/node-bindings` gains the V2 surface next to the V1 one, in the same addon. One `libduckdb`
already exports both symbol sets, so this needs no second binary and no second native package.

The schedule for retiring V1 is now known: deprecated in 2.0, probably removed in 3.0, roughly a
year out (maxxen, 2026-09-05). Side-by-side is exactly the shape a year-long transition wants — but
it is a window, not an open-ended arrangement, so the V2 API needs to be complete enough for
consumers to finish migrating before 3.0, and our own V1 removal has a target rather than a
condition.

What this lands on:

- **The accounting convention extends to V2.** Every C API function appears in
  `duckdb_node_bindings.cpp` and `duckdb.d.ts` as a `// DUCKDB_C_API …` block followed by an
  implementation or a reason. V2 declarations get the same treatment. Since every V2 name is
  prefixed `duckdb_v2_`, the two surfaces separate cleanly by name — `capi-coverage/node_neo.py`
  and `build_coverage.py` intersect against the canonical V1 list and are unaffected.
- **`checkFunctionSignatures.mjs` needs to become two-header aware.** It reads one header, one
  `.d.ts` and one `.cpp`, and compares the three lists for exact equality; a second header's worth of
  declarations in the latter two would read as a mismatch. Worth wiring into `pnpm run build` at the
  same time — it currently only warns, and nothing runs it, which is a thin guard for a surface that
  is about to double.
- **The fetch scripts need `duckdb_v2.h`.** `fetch_libduckdb_*.py` extracts `duckdb.h` and the
  library from each release zip; the release workflow already zips `duckdb_v2.h` beside `duckdb.h`,
  so this is one more entry in each `files` list.

### Bindings shape: near 1:1, deviating where the language demands

Same principle as V1 — mirror the C API, and deviate only for memory management, error handling and
out parameters, as the V1 mapping already does. The places where "as feasible" will be tested:

- **Multiple out-params — precedented, just more common.** V1 already has this and already answers
  it: `duckdb_get_config_flag(index, out_name, out_description)` maps to
  `get_config_flag(index): ConfigFlag`, an object return. V2 needs the same treatment more often —
  `statement_bind` yields a result schema and a parameter schema, `result_step` a chunk and a status
  — but the shape is established, not new.
- **Owned versus borrowed — also precedented, and the mechanism already exists.** `externals.h`
  carries both `CreateExternalFor<X>` and `CreateExternalFor<X>WithoutFinalizer` for data chunks and
  logical types, plus holder structs for connections and databases. What changes under V2 is
  pervasiveness and enforcement: the C API documents ownership per out-param and aborts if a
  borrowed handle is destroyed, so this becomes something the accounting should record per function
  rather than something inferred per call site.
- **The `_with_connection` / `_with_context` pairs.** 37 value constructors exist in both forms, and
  the same split runs through data chunks, column data collections and the type constructors. A 1:1
  mapping exposes both; whether the TS layer keeps them as two functions or one with a union-typed
  first argument is the open judgement call.
- **`create_environment` maps onto the instance-cache pattern.** Not addon-owned state: the
  precedent is `DuckDBInstanceCache`, which the bindings expose 1:1 as `create_instance_cache()`
  while the API layer supplies a lazily-created `singleton` that `DuckDBInstance.fromCache()` routes
  through — and nothing stops a caller constructing more than one. A V2 environment gets the same
  shape: constructible for advanced use, with one auto-created default that ordinary opens go
  through. The one difference is that V1's cache is optional (`DuckDBInstance.create` bypasses it)
  whereas `duckdb_v2_open` requires an environment, so the default is implicit rather than opt-in.

### API: one package, V2 classes under a `v2` subpath

`@duckdb/node-api` gains a second set of classes for V2, exported from a `v2` path alongside the
existing ones. Rejected alternatives: a separate `@duckdb/node-api-v2` package, which is
unnecessary weight and would stop a consumer migrating one call site at a time; and reimplementing
the existing V1 classes over the V2 bindings, which the differences between the two C APIs would
make awkward — the survey's §8, §9 and §10 are the reason.

Neither package has an `exports` map today; both use plain `main`/`types`. Adding one to get
`@duckdb/node-api/v2` also closes the package to deep imports — which is intended. `index.ts`
already exports only what is meant to be referenced externally, and deep references were never
supported; if something turns out to be needed, the answer is to export it explicitly.

### The C++ API over V2: worth consuming, with two caveats

`duckdb_cpp` is the obvious thing for a C++ N-API addon to build on: it throws instead of returning
error codes, and its handles are move-only with documented owning/borrowed semantics — exactly the
boilerplate §2 and §5 describe. Consuming it would not change the TypeScript surface at all; it
changes how `duckdb_node_bindings.cpp` is written, so it is orthogonal to the 1:1 mapping decision.

Two things to know before planning around it:

- **It is not header-only**, despite the description. `tools/cpp/duckdb_cpp.hpp` is 5,703 lines of
  declarations that do not include `duckdb_v2.h` at all, and `tools/cpp/duckdb_cpp.cpp` is 5,742
  lines of implementation that includes both `duckdb_v2.h` and `duckdb_extension_v2.h`. Consuming it
  means adding that `.cpp` to `sources` in `binding.gyp` — fine in itself, but it means the release
  tarball has to ship the source file too, not just the header. The CMake module the team mentions
  does not help us: node-gyp is not CMake, and `FetchContent` has no equivalent here.
- **Exceptions are fine.** `duckdb_cpp` reports every failure by throwing, and `binding.gyp` already
  builds against `node_addon_api_except_all`, so C++ exceptions are enabled and the addon already
  converts them at the boundary. No build-configuration change needed.

Not a decision yet — it turns on the tarball question below. Worth settling before the V2 bindings
are far along, since retrofitting is more work than starting on it.

### Results: no materialized result class

The V2 C API has no materialized result, so the V2 API layer does not have one either.
`DuckDBMaterializedResult` is a thin subclass adding exactly three members — `rowCount`,
`chunkCount` and `getChunk(i)` — each backed by a V1-only function (`duckdb_row_count`,
`duckdb_result_chunk_count`, `duckdb_result_get_chunk`) with no V2 counterpart. `createResult`'s
branch on `duckdb_result_is_streaming` likewise has nothing left to decide, since every V2 result
streams. Both go away.

Materializing stays available as helpers over the streaming result — which is what we already do.
`DuckDBResultReader` accumulates chunks through `readAll()` / `readUntil(n)`, tracks
`currentRowCount` and `done`, and layers `getRows` / `getColumns` and the converters on top, all
over the streaming `DuckDBResult` base rather than over the materialized subclass. That pattern
carries over unchanged in shape.

One knock-on: `rowsChanged` is a synchronous getter today, straight from `duckdb_rows_changed`. V2
reports it out of `duckdb_v2_result_drain(result, &out_rows_changed, err)`, which consumes the
stream — so it becomes an async, state-changing call rather than a property.

### Value construction: keep both the connection and context forms

Keep `_with_connection` and `_with_context` as two functions rather than collapsing them into one
with a union-typed first argument. That follows the 1:1 principle, gives tighter types, and the two
forms likely belong to different scenarios in practice: a connection is what an external caller
holds, while a context is what arrives inside a bind / init / exec scope — and the header is
explicit that catalog-touching context calls belong in bind-phase callbacks rather than exec-phase
worker callbacks. Worth staying flexible: if the implementation turns up a concrete reason to
collapse them, revisit.

### A known limitation to document: one file, both APIs

Because the point of a `v2` subpath is to let a consumer migrate incrementally, both APIs will run
in one process — and a database file opened through both is **not** detected. Measured against the
preview build:

```
v1 open:                 ok
v1 open again:           ok (no conflict reported)
v2 open same file:       ok (NOT detected)
v2 open twice (control): refused with RESOURCE_IN_USE (code 3001)
```

(`coexist_probe.c`, run against the preview build.)

V2 detects its own double-open, because `duckdb_v2_open` rejects a file "already open under the same
environment" — but a V1 database of the same file is not under that environment, and V1's own
deduplication only happens through an explicit instance cache.

Not a blocker (Jeff, 2026-09-05): the same footgun exists today, since V1's plain `duckdb_open`
never deduplicated either. It needs documenting rather than solving. Worth writing down because
partial migration makes it easier to reach than it is today — the two halves of one app would each
naturally open "their" database — so the `v2` path's docs should say that a file should be opened
through one API or the other, not both.

## Answers from the DuckDB team

Asked 2026-09-05; answered by maxxen the same day. Paraphrased, with what each one lands on.

| Question | Answer | Lands on |
|---|---|---|
| Will there be an appender equivalent? | "Probably not" — implement client-side over column data collections plus replacement scans / table functions; `duckdb_cpp` has an example. | We own a V2 appender. Not a wrapper — a collection behind a replacement scan is a virtual table, so appending to an existing table is `INSERT INTO … SELECT * FROM`. |
| What about the V1 date/time/decimal/hugeint conversion helpers? | Some may be; the team is "trying to avoid *convenience* functions" and expects clients to implement them, but they may ship as inline header functions or as docs. | We implement them in TypeScript. Hedged, so re-check near release. |
| Will V1 be deprecated and retired? When? | "v1 will be deprecated in 2.0, and probably removed entirely in 3.0 (likely another year out)." | Side-by-side is a ~1-year window, not indefinite. |
| Will `duckdb_cpp` ship in 2.0? | Currently core-repo only; "most likely" added to the release tarballs soon, plus a proper CMake module for `FetchContent`. | Promising but uncommitted, and the CMake half does not help node-gyp. |

## Open questions

Follow-ups these answers raise, roughly in the order they will matter:

1. **Will the release tarball ship `duckdb_cpp.cpp`, not just `duckdb_cpp.hpp`?** The API is
   header-plus-source, and node-gyp cannot consume the planned CMake module. Both files in the
   tarball is what we need. *(DuckDB team.)*
2. **Is 2.0's V1 deprecation documentation, or `DUCKDB_DEPRECATED` attributes on the declarations?**
   If the latter, our build starts emitting a warning per V1 call — 314 of them — the moment we
   upgrade, which is worth knowing before it happens rather than after. *(DuckDB team.)*
3. **Which conversion helpers, specifically, survive?** The answer was hedged; the current header has
   none, and no `static inline` helpers at all. Determines how much we reimplement. *(DuckDB team.)*
4. **Do we build the V2 bindings on `duckdb_cpp` or on the raw C surface?** Turns on question 1.
   Worth settling early — retrofitting is more work than starting on it. *(Us.)*
5. **How much of the V2 API has to exist before 3.0?** Consumers need to finish migrating inside the
   window, which makes the appender reimplementation and anything else V1-only into scheduled work
   rather than open-ended. *(Us.)*
