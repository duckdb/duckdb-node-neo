# DuckDB C API V2: survey and comparison with V1

Working notes for migrating Node Neo to the C API that ships with DuckDB 2.0. Nothing here
is a commitment; this is the fact-finding pass that a migration plan gets built on.

Sources, as of 2026-11-21 (first surveyed 2026-09-05, and refreshed 2026-10-10 against the
`v2.0-cyanoptera` branch before 2.0 shipped):

- `duckdb_v2.h` at the [`v2.0.1`](https://github.com/duckdb/duckdb/tree/v2.0.1) tag (`8d78114`) —
  641 functions, 620 of them behind `DUCKDB_V2_API_ALLOW_UNSTABLE`. The branch tip on 2026-11-20
  (`b3a028e`) has the same header, so nothing further is queued for the next patch release yet.
- `duckdb.h` at the same tag — the V1 surface in 2.0.1, and what `bindings/libduckdb/` builds
  against since the 2.0.1 upgrade.
- `duckdb.h` at `v1.5.6` — the last V1 surface before 2.0, kept as the baseline for what 2.0 did to
  V1.
- The three PRs that landed V2: [#24702](https://github.com/duckdb/duckdb/pull/24702) (part 1:
  errors, environment, database, connection, context, SQL statements, streaming results, logical
  types, values, data chunks, vectors), [#25184](https://github.com/duckdb/duckdb/pull/25184)
  (part 2: scalar and aggregate functions, column data collections),
  [#25340](https://github.com/duckdb/duckdb/pull/25340) (part 3: table, copy and cast functions,
  file system, replacement scans, arrow, catalog, logging).
- The follow-ups that reshaped it, before and after the release — see
  [What changed between surveys](#what-changed-between-surveys).

All of those PRs carried the same caveat — nothing is set in stone, more follow-ups are expected —
and 2.0 made it formal: every V2 function ships marked unstable. It kept moving, too. Between the
October survey and 2.0.1: 131 functions removed, 185 added, 6 signatures changed, some of it inside a
patch release. Treat every count below as a snapshot. `compare_api.py` re-derives all of them from
the headers.

The earlier surveys measured preview artifacts that were rebuilt in place under one tag, so a local
copy could go stale without changing its name. Release tarballs do not, so a `libduckdb-v2/` fetched
for a version stays valid. `fetch_libduckdb_v2.py` and `compare_api.py --fetch` both take the version
from `bindings/scripts/fetch_libduckdb.py`, so they follow the bindings' DuckDB bump without edits.

Companion to [`../capi-coverage`](../capi-coverage/README.md), which measures Node Neo against the
other five C-API clients. `compare_api.py` reuses that tool's `capi_functions.py` (the canonical
V1 function list) and `node_neo.py` (our exposure of it, and the recorded reason for each
non-exposure), so the two cannot drift apart on the V1 side. What is new here is parsing
`duckdb_v2.h` and diffing the V1 surface against itself — functions and enums — across the 2.0
boundary.

Since 1.5.6, V1's `duckdb.h` uses the same generated layout as `duckdb_v2.h`, grouping functions
under `MODULE:` banners rather than the `//----` rules 1.5.5 had. `capi_functions.py` reads both, so
the V1 groups below are 1.5.6's 25 modules, not 1.5.5's 41 sections; the function set and all 48
deprecation flags are identical across the two releases.

## Reproducing this

```bash
python3 tools/capi-v2/compare_api.py --fetch   # pull duckdb.h + duckdb_v2.h for the bindings' release
python3 tools/capi-v2/compare_api.py           # print the comparison report
python3 tools/capi-v2/compare_api.py --fetch --ref v2.0-cyanoptera   # look ahead at the next patch
python3 tools/capi-v2/fetch_libduckdb_v2.py    # pull that release's library + every header
cd tools/capi-v2 && cc -std=c11 -I libduckdb-v2 smoke_v2.c -L libduckdb-v2 -lduckdb \
    -Wl,-rpath,@loader_path/libduckdb-v2 -o smoke_v2 && ./smoke_v2
```

`smoke_v2.c` attaches an in-memory database, runs a query and reads a chunk back through V2 — the
shortest illustration of the calling convention the bindings have to adopt. `coexist_probe.c`
builds the same way, and checks whether one database file opened through both V1 and V2 is
detected. So does `options_probe.c`, which checks whether V2 can enumerate configuration options
before any database exists. Every V2 probe defines `DUCKDB_V2_API_ALLOW_UNSTABLE` before including
`duckdb_v2.h`; without it, 2.0's header declares almost nothing (see the headline findings).

`chunk_probe.c` and `chunk_probe_v2.c` are a pair, asking the same question of each surface: will
a data chunk take a VARIANT column? The V1 one builds against the bindings' library, and its answer
changed in 2.0.1. See
[VARIANT in a data chunk](#variant-in-a-data-chunk-a-v1-defect-fixed-in-201).

```bash
cd tools/capi-v2 && cc -std=c11 -I ../../bindings/libduckdb chunk_probe.c \
    -L ../../bindings/libduckdb -lduckdb -Wl,-rpath,@loader_path/../../bindings/libduckdb \
    -o chunk_probe && ./chunk_probe
cd tools/capi-v2 && cc -std=c11 -I libduckdb-v2 chunk_probe_v2.c -L libduckdb-v2 -lduckdb \
    -Wl,-rpath,@loader_path/libduckdb-v2 -o chunk_probe_v2 && ./chunk_probe_v2
```

## Headline findings

**Every V2 function is unstable in 2.0, and V2 changes in patch releases.** Three days before the
release, [#27410](https://github.com/duckdb/duckdb/pull/27410) demoted all of V2 to `unstable`:
each declaration sits inside `#if DUCKDB_V2_API_ALLOW_UNSTABLE`, which defaults to 0, so a consumer
that includes `duckdb_v2.h` without defining it gets no V2 function at all. Opting in while
targeting an older V2 API version is an `#error`, since the unstable surface "is not part of any
released version". The October survey counted 26 unstable functions; 2.0.0 has 620 of 620.

The label is earned. 2.0.1, a patch release, added 21 V2 functions and changed one signature:
`duckdb_v2_parse_sql` gained an `apply_invalidation_policy` parameter in the middle of its argument
list (backported in [#27684](https://github.com/duckdb/duckdb/pull/27684)). Every probe here that
parses SQL needed the extra argument. That is new for Node Neo: V1's signatures are a stability
promise, so a DuckDB bump compiles unchanged and the signature check only warns. Once the
bindings call V2, a bump — a patch bump included — can stop them compiling until the call sites
follow. See [Migration approach](#migration-approach).

One inconsistency: the 21 functions 2.0.1 added — stats callbacks for scalar, aggregate and table
functions, and four `stats_*` accessors — came from `main` with `stable v2.0.0` lifecycle entries,
which the backport kept. They are the only V2 declarations *outside* the switch. Without it they
compile in, but cannot be used, since nothing that creates a function to attach them to is
declared. Worth raising upstream: v2.0.0 has no such functions, stable or not.

**V1 keeps compiling in 2.0, but the clock is now running.** Comparing the last V1 header before
2.0 (1.5.6) with 2.0.1's — the release confirmed what the October branch showed: zero functions
removed, two added (`duckdb_create_timestamp_tz_ns`, `duckdb_get_timestamp_tz_ns`), zero signature
changes, and three new enum values (`DUCKDB_TYPE_TIMESTAMP_TZ_NS = 42`,
`DUCKDB_ERROR_DATA_CORRUPTION = 43`, `DUCKDB_ERROR_RESOURCE_IN_USE = 44`). The thirteen `()` →
`(void)` edits the first survey counted here arrived with 1.5.6 instead — see below. Nothing in the
header carries `DUCKDB_DEPRECATED` — the macro is defined but never applied. The 2.0.1 `libduckdb`
exports all 548 V1 symbols alongside all 641 V2 symbols from one library, so V1 and V2 can be mixed
inside one addon, and an incremental, module-by-module migration is possible rather than a big-bang
rewrite.

What the header does not show is the schedule. Per the DuckDB team (maxxen, 2026-09-05): **V1 is
deprecated in 2.0 and probably removed entirely in 3.0, likely about a year out.** So 2.0 is not a
forced migration, but it is the start of a fixed window rather than an open-ended coexistence — see
[Migration approach](#migration-approach).

**But one V1 behavior changes underneath an unchanged signature: an open result now blocks its
connection.** Through 1.5.x, starting a statement on a connection with an unfinished streaming or
pending result silently aborts the unfinished one — and inside a transaction, an aborted write was
then lost by the `COMMIT` without an error. From 2.0
([#26314](https://github.com/duckdb/duckdb/pull/26314),
[#26747](https://github.com/duckdb/duckdb/pull/26747)) the *new* statement is refused with
`DUCKDB_ERROR_RESOURCE_IN_USE` until the open result is drained or destroyed, and destroying an
unfinished result that may write rolls it back. The V1 docs for `duckdb_destroy_result` and
`duckdb_destroy_pending` say so, and `open_result_probe.c` measures it — against 1.5.6:
`second query with a stream open: ok`; against the preview and 2.0.1: `error (type 44) Resource In
Use Error: connection has an open result; drain or destroy it before starting a new query`.

This landed on us harder than on most clients, because Node Neo did not expose
`duckdb_destroy_result` — results were destroyed in a finalizer, so on 2.0 a `DuckDBResult` that was
streamed, partly read and dropped kept its connection refusing queries until the garbage collector
happened to finalize it. [#516](https://github.com/duckdb/duckdb-node-neo/pull/516) added explicit
release ahead of the 2.0 upgrade: `destroy_result_sync` and `destroy_pending_sync` in the bindings,
and `closeSync()` on `DuckDBResult`, `DuckDBPendingResult` and `DuckDBResultReader` in the API. A
destroy requested while a fetch or `execute_pending` is in flight is deferred until it finishes.
`using` / `Symbol.dispose` support is tracked in
[#515](https://github.com/duckdb/duckdb-node-neo/issues/515). Still open: surfacing
`RESOURCE_IN_USE` as something a caller can recognize — `duckdb_result_error_type` is not exposed,
so the error arrives as a plain `Error` with DuckDB's message. The same rule is native to V2 (§8), so
the API shape chosen here carries over.

**Our signature check warned on `main` from the 1.5.6 bump until the accounting caught up.**
1.5.6's `duckdb.h` uses the same generated layout as `duckdb_v2.h`: it carries the 2.0-style
guards, the thirteen `()` → `(void)` edits, and a new declaration order (by module).
`checkFunctionSignatures.mjs` compares declaration text *in order*, so it reported both `.d.ts` and
`.cpp` as mismatched, even though the three sets agreed once `(void)` was normalized — no function
was missing or extra. It warns rather than exits non-zero, and nothing invokes it, so nothing
announced it. The `// DUCKDB_C_API …` blocks in `duckdb_node_bindings.cpp` and `duckdb.d.ts` now
follow the 1.5.6 header order and take the `(void)` edits, and the check is clean again (546 of
546). The script's guard handling needed no change, since it was already done for 2.0. The
`// #ifndef DUCKDB_API_NO_DEPRECATED` markers kept their spelling but were regrouped: the 48
deprecated functions now sit in 5 contiguous runs instead of 9. On 2.0.1 the check is clean at 548
of 548: the 2.0 upgrade accounted for the two new functions.

**The release ships tarballs, carrying both headers.** 2.0 replaced the
`libduckdb-<platform>.zip` release assets with `duckdb-shared-libs-<suffix>.tar.gz`, flat, each
holding `libduckdb.{dylib,so,dll}`, `duckdb.h`, `duckdb_v2.h`, `duckdb_extension.h`,
`duckdb_extension_v2.h` and `duckdb_static_extension.h` (registration for statically linked
extensions — not something Node Neo uses). The bindings fetch these since
[#512](https://github.com/duckdb/duckdb-node-neo/pull/512), extracting `duckdb.h` and the library;
`fetch_libduckdb_v2.py` fetches the same tarball and extracts all of it, for the probes here.
Neither `duckdb_cpp.hpp` nor `duckdb_cpp.cpp` is in it.

**V2 is a redesign, not a rename.** Function-by-function translation does not exist for most of
the surface. Roughly: a third of what Node Neo uses maps mechanically, a third needs restructuring
around new call sequences, and a third has no counterpart yet.

**The appender is not in V2, and is not coming.** Node Neo exposes 31 appender functions and both
packages have appender APIs and test suites. Nothing in `duckdb_v2.h` matches, and per the DuckDB
team (maxxen, 2026-09-05) that is deliberate: "probably not, this can now be implemented client-side
using columndatacollections and replacement scans/table functions."

So bulk append becomes ours to design rather than to wrap. The goal is the *scenario* — efficient
bulk append — not the V1 shape: an explicit appender class is one option, helpers that drive an
`INSERT INTO … SELECT * FROM` over a collection bound to the statement are another, and that choice
belongs to whoever builds this part of the V2 API rather than to this survey. Worth knowing going in:
a collection reaches SQL only through a statement that reads it, which is why appending to an
*existing* table is an `INSERT … SELECT` rather than a direct append. See
[How bulk append works without an appender](#how-bulk-append-works-without-an-appender) for the
mechanism and its sharp edges.

**One class of hazard in our function-info handling goes away.** V2 gives every function family its
own info-handle type (`duckdb_v2_scalar_function_bind_info_handle`,
`duckdb_v2_table_function_exec_info_handle`, and so on), where V1 shares one `duckdb_function_info`
across all of them. Since September there is also one deliberately *shared* handle,
`duckdb_v2_function_bind_info_handle`, passed to every family's bind callback — but it wraps one
polymorphic C++ class, so sharing it is safe in a way V1's sharing is not. `type_tags.h` stays
either way — a tag is the only thing that distinguishes one `Napi::External` from another at runtime
— but the six function-info tags stop carrying extra load; see §12. Separately, the `duckdb_cpp`
"stable C++ API" over V2, mentioned in all three PRs, turns out to live at `tools/cpp/` rather than
`src/include/`, which is why it is not in the packaged headers. The team expected to add it to the
release tarballs "soon", along with a CMake module for `FetchContent` (maxxen, 2026-09-05); 2.0.1's
tarballs still do not have it. As a C++ N-API addon we are its natural consumer — see
[Migration approach](#migration-approach) for what that would take.

## What changed between surveys

### 2026-10-10 to 2.0.1

`duckdb_v2.h` went from 587 functions to 641: 131 removed, 185 added, 6 signatures changed. The
removals are consolidation, not lost capability: 98 `_with_connection` / `_with_context`
constructors folded, pair by pair, into one function taking a factory, 17 `*_create_with_extension` /
`*_register` functions gave way to registration on the target, and 16 per-root option functions to a
config handle. New modules: `factory`, `custom_option`, `pragma`, `macro`, `function docs`, `log
sink`, `query metrics` and `stats`. Grouped by what it means for us:

| Change | PRs | Where it lands here |
|---|---|---|
| Every function marked unstable | [#27410](https://github.com/duckdb/duckdb/pull/27410) | Headline finding |
| A **factory**, borrowed from an instance, connection or context, creates values, types, data chunks, collections and functions — replacing every `_with_connection` / `_with_context` pair | [#26820](https://github.com/duckdb/duckdb/pull/26820) | §9, §10, §12 |
| A **config**, borrowed the same way, reads and writes options by scope; startup-only options move to `instance_create_with_options`, and an instance starts as soon as it is created | [#26820](https://github.com/duckdb/duckdb/pull/26820) | §6, §7 |
| Registration moves to the target: `connection_register_*`, `instance_register_*` and `extension_register_*` replace `*_register` | [#26820](https://github.com/duckdb/duckdb/pull/26820) | §12 |
| `instance_attach` takes the name before the path; `instance_detach` and `instance_set_default` take the name instead of the path | [#26820](https://github.com/duckdb/duckdb/pull/26820) | §6 |
| Function docs; `factory_create_common_type` (SQL's type unification) | [#26820](https://github.com/duckdb/duckdb/pull/26820) | §9, §12 |
| Custom options, pragma functions, macros (extensions only), vector casts, file system path helpers | [#26863](https://github.com/duckdb/duckdb/pull/26863) | §7, [What we would gain](#what-we-would-gain) |
| Log sinks | [#26872](https://github.com/duckdb/duckdb/pull/26872) | [What we would gain](#what-we-would-gain) |
| Profiling metrics | [#26874](https://github.com/duckdb/duckdb/pull/26874) | [What we would gain](#what-we-would-gain) |
| `statement_add_collection` returns, and upstream's appender moves onto it; `data_chunk_clear`; more file system helpers | [#26886](https://github.com/duckdb/duckdb/pull/26886) | [Bulk append](#how-bulk-append-works-without-an-appender) |
| `parse_sql` takes `apply_invalidation_policy`; stats callbacks for scalar, aggregate and table functions — both in 2.0.1, not 2.0.0 | [#27684](https://github.com/duckdb/duckdb/pull/27684) (backport) | Headline finding; §8, §12 |

### 2026-09-05 to 2026-10-10

`duckdb_v2.h` went from 527 functions to 587: 40 removed, 100 added, 43 signatures changed, the
`database` module gone and `instance`, `function` and `tokenizer` new. Grouped by what it means for
us rather than by module:

| Change | PRs | Where it lands here |
|---|---|---|
| `open` / `close` / `connect` / `disconnect` replaced by environment → instance → attach → connection; options set by name; `database_*` → `instance_*` | [#25751](https://github.com/duckdb/duckdb/pull/25751), [#25910](https://github.com/duckdb/duckdb/pull/25910) | §6, §7; option enumeration no longer needs a database |
| Unfinished results block their connection and roll back on destroy — in V1 too | [#26314](https://github.com/duckdb/duckdb/pull/26314), [#26747](https://github.com/duckdb/duckdb/pull/26747) | Headline finding; §8 |
| Every struct passed by `const` pointer, never by value; function names take `identifier_t` | [#26230](https://github.com/duckdb/duckdb/pull/26230) | §4 |
| One shared `function_bind_info` for every family's bind; Python-style parameter kinds; four-part argument counts | [#26264](https://github.com/duckdb/duckdb/pull/26264) | §12 |
| Scalar type resolution split from bind | [#26548](https://github.com/duckdb/duckdb/pull/26548) | §12 |
| Arrow-producing results replace `result_to_arrow_stream` | [#25801](https://github.com/duckdb/duckdb/pull/25801) | §8 |
| Parse-time statement metadata; SQL tokenizer | [#25612](https://github.com/duckdb/duckdb/pull/25612), [#25609](https://github.com/duckdb/duckdb/pull/25609) | §8 |
| `connection_progress_get` replaces the `query_progress` handle; `error_info_get_raw_message` → `_get_raw_text`; `validate_utf8`, and identifiers validated as UTF-8 | [#25984](https://github.com/duckdb/duckdb/pull/25984), [#25851](https://github.com/duckdb/duckdb/pull/25851), [#26088](https://github.com/duckdb/duckdb/pull/26088) | §3, §4 |
| Table-function order preservation ([#26230](https://github.com/duckdb/duckdb/pull/26230)), partitioning and batch claims; multi-file functions; copy-to statistics | [#26254](https://github.com/duckdb/duckdb/pull/26254), [#26584](https://github.com/duckdb/duckdb/pull/26584) for multi-file | §12 — mostly behind the new unstable switch |

What did **not** change across either refresh, re-measured rather than assumed: VARIANT columns in
V2 data chunks and collections work (`chunk_probe_v2.c`); the appender and the conversion helpers
are absent; a database opened through both APIs is not detected (`coexist_probe.c`); and nothing in
either header is marked `DUCKDB_DEPRECATED`. One answer did change, on the V1 side: as of 2.0.1 a V1
data chunk takes a VARIANT column too — see
[VARIANT in a data chunk](#variant-in-a-data-chunk-a-v1-defect-fixed-in-201).

## The two surfaces, side by side

| | V1 (1.5.6) | V1 (2.0.1) | V2 (2.0.1) |
|---|---:|---:|---:|
| Functions | 546 | 548 | 641 |
| Exposed by Node Neo | 319 | 323 | — |

The first survey measured 527 V2 functions and 314 exposed. The five exposed by October are the
catalog functions from [#500](https://github.com/duckdb/duckdb-node-neo/pull/500); the four since
are `duckdb_destroy_result` and `duckdb_destroy_pending`
([#516](https://github.com/duckdb/duckdb-node-neo/pull/516)) and 2.0's two TIMESTAMP_TZ_NS value
functions.

Node Neo's V1 coverage, by 2.0.1 header module — this is the actual migration surface, not the 548:

| V1 module | total | in Neo | V2 status |
|---|---:|---:|---|
| database | 12 | 9 | restructured: environment → instance → attach; startup options at instance creation; instance cache gone, double-attach detected per environment |
| connection | 10 | 7 | `connection_create` / `interrupt` / `progress_get`; **no** `get_table_names`, no client context outside callbacks, connection id only in log sink records |
| query | 32 | 15 | restructured (parse → iterate → bind → execute → step); error data → `error_info`; safe fetch gone; `vector_size` and malloc/free gone |
| prepared_statement | 45 | 42 | restructured: binds become parameter arrays at execute time; metadata split between `sql_statement` and `schema` |
| pending | 7 | 7 | replaced by `result_step` + `result_wait` |
| datetime_helpers | 19 | 19 | **absent** (date/time/timestamp, hugeint and decimal conversions) |
| value | 103 | 76 | present; every constructor takes a factory, borrowed from an instance, connection or context |
| logical_type | 31 | 29 | collapsed into generic construct-by-name/id/text through a factory + `get_param` |
| data_chunk | 7 | 6 | close mapping |
| vector | 26 | 18 | restructured around `vector_view`; selection vectors folded into `view.sel` |
| scalar_function | 33 | 26 | close mapping; type resolution split into its own callback |
| table_function | 36 | 33 | close mapping, plus filter / projection pushdown, order preservation, partitioning |
| catalog | 7 | 5 | **absent** — V2 describes tables (`connection_describe_table`) but has no catalog-entry lookup |
| appender | 40 | 31 | **absent** |
| aggregate_function | 16 | 0 | present (51 functions) |
| arrow | 19 | 0 | present, redesigned (importer / exporter, plus Arrow-producing results) |
| cast_function | 12 | 0 | present (14 functions) |
| config_option | 14 | 0 | present, split: custom option registration in `custom_option` (17), profiling in `query metrics` (11); reading and writing options is `configuration` |
| copy_function | 33 | 0 | present (79 functions) |
| expression | 4 | 0 | present (9 functions) |
| file_system | 16 | 0 | present (24 functions) |
| log_storage | 6 | 0 | present as log sinks (12 functions), plus `*_log` on instance, connection and context |
| replacement_scan | 4 | 0 | present (15 functions) |
| table_description | 8 | 0 | present, in V2's `catalog` module |
| threading | 8 | 0 | **absent** |

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

This affects essentially all 323 call sites in `duckdb_node_bindings.cpp`. It is the single
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
`error_info_get_raw_text` split the formatted message from the body without its
`"<Type> Error: "` prefix. (The second was `error_info_get_raw_message` until September; the header
now notes that no type name is exposed, so the prefix cannot be reconstructed from the code.)

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

Since [#26230](https://github.com/duckdb/duckdb/pull/26230), V2 never passes a struct by value:
every `duckdb_v2_str`, `duckdb_v2_identifier_t`, hugeint, interval and so on goes in as a `const`
pointer, so the FFI does not depend on the platform's struct-passing convention. Inputs are borrowed
for the call and copied. (A few plain `const char *` inputs remain, `parse_sql`'s SQL among them.)
Identifiers are now validated as UTF-8 on the way in
([#26088](https://github.com/duckdb/duckdb/pull/26088)), and `validate_utf8` is exposed for callers
who want to check a buffer themselves.

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

### 6. New roots: the environment and the instance

Reworked since the first survey ([#25751](https://github.com/duckdb/duckdb/pull/25751), renamed in
[#25910](https://github.com/duckdb/duckdb/pull/25910), simplified in
[#26820](https://github.com/duckdb/duckdb/pull/26820)). `duckdb_v2_open` used to create an
instance, apply options and attach the main database all at once. Those are now separate steps:

```c
duckdb_v2_environment_create(&env, &err);
duckdb_v2_instance_create_with_options(env, names, values, count,   /* startup-only options; */
                                       &instance, &err);            /* started, nothing attached */
duckdb_v2_instance_attach(instance, NULL, &path, NULL, true, &err); /* ATTACH; NULL name = derived */
duckdb_v2_connection_create(instance, &conn, &err);
```

(`instance_create` is the same without options.)

What this changes:

- **An instance is not a database.** It is what V1 calls a `duckdb_database` — the engine, its
  buffer pool, catalog and transaction manager — but it starts empty. Databases are *attached* to
  it exactly like SQL `ATTACH`, any number of them, with `(KEY value)` options through an
  `attach_options` handle, and one chosen as the default for new connections. `instance_detach`
  and `instance_set_default` complete the set, both by attach name; a null name derives it from the
  path, as SQL `ATTACH` without `AS` does. A connection can be created with nothing attached, and
  then sees only the system and temp catalogs.
- **The instance starts when it is created.** In October it started on first attach or connection,
  and `instance_set_option` staged startup configuration until then. That staging is gone:
  startup-only options such as `access_mode` or `enable_external_access` are text name / value pairs
  to `instance_create_with_options`, and every later change is a config write (§7).
- **Double-open detection is per environment.** A path already attached on *any* instance of the
  environment is refused with `ERROR_RESOURCE_IN_USE`. That subsumes the deduplication
  `duckdb_instance_cache` provides — which has no V2 counterpart — but as a refusal rather than a
  hand-back of the existing database. A second environment does not see the first's attachments
  (`coexist_probe.c`; see [A known limitation to
  document](#a-known-limitation-to-document-one-file-both-apis)).
- **Teardown is reference-counted.** `instance_destroy` always succeeds; the instance lives on while
  any connection references it. `environment_destroy` refuses with `ERROR_RESOURCE_IN_USE` while any
  instance is alive, and `environment_get_instance_count` exists to find the leak.

### 7. Configuration

V1: build a `duckdb_config` blob of string key/value pairs, pass it to `duckdb_open_ext`.

V2: startup-only options are text name / value pairs to `instance_create_with_options`. Everything
else goes through a `duckdb_v2_config_handle`
([#26820](https://github.com/duckdb/duckdb/pull/26820)), borrowed from an instance (which reads and
writes `GLOBAL` only), a connection (which reads its session value before the global one, and writes
either) or — inside callbacks — a context. One set of functions serves all three: `config_set_option_text` takes the text SQL `SET` accepts and `config_set_option_value` a
value, each with a `DEFAULT`, `GLOBAL` or `SESSION` scope; `config_get_option_value` returns the
current value and the scope it came from; and `config_get_option_count` / `_by_index` / `_by_name`
return option handles carrying the description, default value, default scope, supported scopes and
aliases — richer than V1's `duckdb_get_config_flag`. The October survey's per-root
`instance_*_option`, `connection_*_option` and `context_*_option` functions are gone.

Registering *new* options, which V1's `config_option` module does, is in V2 now too:
`custom_option_*`, with an update callback that sees each new value and can replace it
([#26863](https://github.com/duckdb/duckdb/pull/26863)).

The first survey recorded a regression here: V1 enumerates options with no database open
(`duckdb_config_count` / `duckdb_get_config_flag`), which `configurationOptionDescriptions.ts`
relies on, and V2 then needed an open database. **That stays fixed.** `options_probe.c` lists 208
options through the config of an instance with nothing attached (192 in October). So the V2
equivalent costs an environment and an instance, not a database.

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
duckdb_v2_parse_sql(conn, sql, true, &iterator, &err); /* also replaces extract_statements */
duckdb_v2_statement_iterator_next(iterator, &stmt, &err);
duckdb_v2_statement_bind(conn, stmt, &schema, &parameters, &err);   /* optional: metadata */
duckdb_v2_statement_execute(conn, stmt, names, values, count, &result, &err);
```

`parse_sql`'s `true` is 2.0.1's `apply_invalidation_policy`
([#27684](https://github.com/duckdb/duckdb/pull/27684)): whether a parse error invalidates the connection's
transaction, as a failed statement would. Pass true when parsing to run, false when only
inspecting.

A parsed statement now carries parse-time metadata too
([#25612](https://github.com/duckdb/duckdb/pull/25612)): `sql_statement_get_type`, `_get_text`, and
`_get_parameter_count` / `_get_parameter_name`, which give the binding keys `statement_execute`
accepts ("1", "2", … for positional parameters, the identifier for named ones) without a bind. And
`statement_add_collection` binds a column data collection to a parsed statement as a named table —
what bulk append is now built from; see [below](#how-bulk-append-works-without-an-appender).

Three consequences:

- **Parameters are arrays at execute time.** Two parallel arrays of `duckdb_v2_identifier_t` names
  and `duckdb_v2_value_handle` values, replacing all 25 `duckdb_bind_*` functions. Named and
  positional parameters unify. Our `DuckDBPreparedStatement` bind-then-execute shape does not
  survive as-is; the value-based path (`duckdb_bind_value`) is the closest existing thing.
- **Every result streams.** There is no materialized result and no `duckdb_row_count`.
  `duckdb_v2_result_step` returns `CHUNK`, `WAITING`, `FINISHED` or `CANCELLED`. A result is a
  live cursor: while it is open the connection refuses new queries with `ERROR_RESOURCE_IN_USE`,
  its transaction stays open, and side-effecting statements only take effect once drained —
  destroying an unfinished result that may write rolls it back. That is a real behavioral change
  for `DuckDBMaterializedResult` and for how the API package holds results across awaits — and, as
  the headline finding records, 2.0 applies the same refusal to V1 results, so it arrives with the
  2.0 upgrade rather than with the V2 migration.
- **The pending-result interface is gone, and `WAITING` replaces it.** This is arguably better
  suited to us than V1's task pump: `result_step` returning `WAITING` is a natural yield point,
  with `result_wait` as the blocking fallback on a worker thread.

Result metadata moves from per-column calls (`duckdb_column_name`, `duckdb_column_type`) to a
`duckdb_v2_schema_handle` obtained once, with `schema_get_field` returning name and type together.
`duckdb_v2_prepared_statement_*` is only four functions, because everything else moved to the
statement and schema.

Arrow output is its own result type ([#25801](https://github.com/duckdb/duckdb/pull/25801)):
`statement_execute_arrow` and `prepared_statement_execute_arrow` return a
`duckdb_v2_arrow_result_handle` with the same `step` / `wait` / `drain` shape, yielding
`ArrowArray`s, or the whole thing as an `ArrowArrayStream` through `arrow_result_to_arrow_c_stream`.
That replaces `result_to_arrow_stream`. Node Neo exposes none of V1's Arrow interface today.

New and SQL-adjacent: a tokenizer ([#25609](https://github.com/duckdb/duckdb/pull/25609)).
`tokenize_sql` yields typed tokens (keyword, identifier, string / number literal, operator, comment,
terminator) as byte ranges, under whatever grammar extensions the connection has loaded, and
`token_iterator_ends_unterminated` reports input that stops inside an open string or comment —
the pieces of syntax highlighting and "is this statement complete?" in a REPL.

### 9. Logical types

V1 has a constructor and a family of accessors per kind — `duckdb_create_list_type`,
`duckdb_create_struct_type`, `duckdb_decimal_width`, `duckdb_struct_type_child_name`,
`duckdb_enum_dictionary_value`, and so on: 31 functions in 1.5.6's `logical_type` module, 29 of
which we expose.

V2 has three constructors and two accessors:

```c
duckdb_v2_factory_create_type_from_id(factory, type_id, param_names, param_values, count, &type, &err);
duckdb_v2_factory_create_type_from_name(factory, qname, param_names, param_values, count, &type, &err);
duckdb_v2_factory_create_type_from_text(factory, &text /* "STRUCT(a INTEGER, b VARCHAR)" */, &type, &err);

duckdb_v2_logical_type_get_param_count(type, &count, &err);
duckdb_v2_logical_type_get_param(type, index, &out_name, &out_value, &err);
```

Type structure is expressed as **value parameters**: DECIMAL has 2 (width, scale as UTINYINT);
LIST 1 (element type, as a TYPE value); ARRAY 2; MAP 2; STRUCT one per field, named; UNION one per
member; ENUM one per dictionary entry, as VARCHAR. Child types cross the boundary wrapped as TYPE
values, unwrapped with `value_get_type`.

This is a much smaller surface, and it generalizes to extension types for free — but `DuckDBType.ts`
and the `types/` tree are built directly on the V1 per-kind accessors, so this is a rewrite rather
than a retarget. Also note that construction now requires a factory, where V1's
`duckdb_create_list_type` needed nothing. An instance's factory is enough for built-in types;
catalog-registered types and qualified names need a connection's or a context's. Beside the three
constructors, `factory_create_type_with_alias` aliases a type, and `factory_create_common_type`
unifies two types the way SQL does.

### 10. Values

Reading is a close mapping (`value_get_int`, `value_get_varchar`, `value_get_child`, …).

Creating is not: **every constructor takes a factory**, borrowed from an instance, connection or
context. `duckdb_create_int32(42)` becomes `duckdb_v2_value_create_int(factory, 42, &value, &err)`.
In October these were `_with_connection` / `_with_context` pairs, with `value_create_null` alone
taking neither; [#26820](https://github.com/duckdb/duckdb/pull/26820) folded each pair into one function,
`value_create_null` included.

`createValue.ts` and `DuckDBValue` construct values with no connection in hand. That is still a
design change in the API package, not just the bindings, but a smaller one than in October: a
built-in value needs only an instance's factory, not a connection.

The context's factory matters for UDFs: inside a bind or exec callback we hold a
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

V2 gives each phase of each family its own handle type — 44 distinct `*_info_handle` types, up from
30 at the first survey and 37 in October (three stats phases, two log sink phases, the custom option
update and the pragma query are new) — so the C type system separates them and those tags become
ordinary externals tags like the other sixteen.

One handle is now shared on purpose. Since [#26264](https://github.com/duckdb/duckdb/pull/26264),
every family's bind callback receives a `duckdb_v2_function_bind_info_handle` alongside its own bind
info, and the per-family `*_bind_get_arg_count` / `_get_arg_type` / `_get_arg_value` /
`_get_user_data` / `_set_bind_data` getters collapsed into one shared set. That looks like V1's
problem returning, but it is not: upstream the handle is a `CV2FunctionBindInfo *`, a polymorphic
base class whose subclasses the getters reach through virtual calls, so one handle type over several
families is correct by construction. For us it is one more tag — one, not one per family.

To be clear about what this does *not* do: `type_tags.h` stays. Every external we hand to JS needs a
tag, because N-API gives us no other way to tell one `Napi::External` from another at runtime. What
changes is that the function-info tags stop being load-bearing against memory corruption, so the
special-case reasoning around them can go. If anything the tag count goes up, since V2 splits each
family into more phases.

New capability worth noting: table functions gain **filter pushdown** (claim-based, via the
expression interface) and projection pushdown, alongside progress reporting, and since September an
order-preservation flag that lets a function declare its output ordered or unordered and so enable
parallelism ([#26230](https://github.com/duckdb/duckdb/pull/26230)), plus — behind the unstable
switch below — partitioning, batch claiming and multi-file functions.

Scalar functions split type resolution from binding
([#26548](https://github.com/duckdb/duckdb/pull/26548)). A `resolve_types` callback runs before the
arguments are cast to the signature's parameter types and sets the return type; `bind` runs after
the cast and sets bind data. `scalar_function_bind_set_return_type` moved to
`scalar_function_resolve_types_set_return_type` accordingly. Worth knowing for our scalar UDFs: a V1
bind callback that sets the return type has to become a V2 `resolve_types` callback, not a bind one.

Creating a function and registering it are now separate from any connection
([#26820](https://github.com/duckdb/duckdb/pull/26820)): `scalar_function_create(factory, &fn,
&err)` builds one, and `connection_register_scalar_function` or `extension_register_scalar_function`
registers it — likewise for every family. Scalar, aggregate
and table functions, pragmas and macros can fill in function docs (description, categories,
examples), and since 2.0.1 the first three can set a stats callback that reports statistics about
their result to the optimizer.

Function registration uses a `function_signature` object (`add_parameter`, `set_return_type`).
Parameters now carry a Python-style kind — `POSITIONAL_ONLY`, `STANDARD`, `POSITIONAL_VARIADIC`
(`*args`), `NAMED_ONLY`, `NAMED_VARIADIC` (`**kwargs`) — with defaults on the non-variadic ones.
That replaced `set_varargs`, and is why the shared `function_bind_get_arg_count` reports four counts
(positional fixed / variadic, named fixed / variadic) rather than one; `function_bind_get_arg_name`
and `_get_arg_index` look arguments up by name. The exec-side `*_exec_get_arg_count` reports the
same four, and argument indices went from `uint32_t` to `idx_t` throughout.

**Everything is the unstable tier now.** In October, 26 of 587 functions sat behind
`DUCKDB_V2_API_ALLOW_UNSTABLE` (which defaults to 0, or to 1 if a consumer defines the older
`DUCKDB_EXTENSION_API_VERSION_UNSTABLE`): `data_chunk_get_capacity`, the `copy_to_statistics_*`
functions, batch claiming and multi-file functions. The open question then was whether to define the
switch just to reach `data_chunk_get_capacity`, the closest V2 has to V1's `duckdb_vector_size`.
[#27410](https://github.com/duckdb/duckdb/pull/27410) settled it: with 620 of 641 functions behind
the switch, the bindings define it or get nothing. The library exports every symbol regardless; the
switch only hides declarations.

## Not in V2 (yet)

Ordered by what it would cost us:

- **Appender** (31 exposed functions, `DuckDBAppender.ts`, two test suites). No counterpart, and
  none planned — see the headline finding. The bulk-append scenario is ours to cover, in whatever
  shape suits.
- **Date / time / timestamp / hugeint / decimal conversion helpers** (19 exposed functions, all of
  them). `duckdb_from_date`, `duckdb_to_timestamp`, `duckdb_double_to_decimal` and the rest have no
  V2 equivalent, and still none in 2.0.1 — the header contains no `static inline` helpers at all.
  The omission is deliberate: the team is "trying to avoid *convenience* functions", and expects
  clients to implement them, though some may yet ship as inline functions in the header or as
  documentation (maxxen, 2026-09-05, hedged; nothing shipped with 2.0 or 2.0.1). For
  us these are pure computation over plain structs, so TypeScript implementations are
  straightforward, and would remove 19 native round-trips in the process. Not a concern: they were
  exposed in V1 because they were in the C API, not because a native implementation was wanted.
- **Catalog entry lookup** (5 exposed functions, new since the first survey via
  [#500](https://github.com/duckdb/duckdb-node-neo/pull/500)). V1's `client_context_get_catalog` /
  `catalog_get_entry` / `catalog_entry_get_*` have no V2 counterpart: V2's `catalog` module is table
  description only (`connection_describe_table`), plus `qname` for names. This is the one gap that
  grew on our side rather than upstream's.
- **`duckdb_get_table_names`** and **`duckdb_client_context_get_connection_id`** (both exposed). V2
  hands out a context only inside callbacks, and has no referenced-tables call; a connection id
  appears only in the records a log sink receives.
  Both need a V2 answer for the scope target, and neither has an obvious one yet.
- **`duckdb_vector_size`** — nearest is `data_chunk_get_capacity` — and the malloc/free helpers.
- **Task / threading control** — `duckdb_execute_tasks` and friends (not exposed today).
- **Instance cache** — subsumed by the environment, but not one-to-one: the cache hands back the
  database already open for a path, while V2 refuses the second attach with `RESOURCE_IN_USE`
  (`DuckDBInstanceCache` in the API package is a public type).

## What we would gain

- Typed error codes instead of message-string matching.
- Per-family function info handles, so the function-info type tags stop guarding against memory
  corruption and become ordinary externals tags.
- Dictionary and constant vector reading without a flatten.
- `WAITING`-based streaming that fits an async binding better than the pending-result pump.
- Table function filter and projection pushdown.
- Catalog / table description, cast functions, copy functions, replacement scans, file system and
  expression interfaces, custom options, profiling metrics and log sinks — all present in V1 too, all
  unexposed by us today, all with a smaller and more consistent surface in V2. Pragma functions are
  new.
- `qname` (qualified-name parsing, rendering, comparison) and `identifier_render_quoted`, which
  would replace hand-rolled quoting in `sql.ts`.
- A SQL tokenizer aware of the connection's grammar, and parse-time statement metadata (type,
  text, parameter names) without a bind.
- Instance configuration and multiple attached databases on one engine — startup-only options set
  by name at creation, `ATTACH` options per database.
- `column_data_collection` — build a set of chunks and bind it to a statement as a table. Not an
  appender, but a bulk-load path with no V1 equivalent.

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
- **`checkFunctionSignatures.mjs` still needs to become two-header aware.** Its guard tracking is
  already done — see [Deprecation is a compile switch](#deprecation-is-a-compile-switch-not-a-warning)
  — but that was the V1-side half. The script still hardcodes one header
  (`libduckdb/duckdb.h`), one `.d.ts` and one `.cpp`, and compares the three lists for *exact ordered
  equality*. Reading `duckdb_v2.h` too is the easy part; the part that cannot be settled yet is where
  the V2 accounting lives, because ordered equality means the answer has to be decided rather than
  guessed: one `duckdb.d.ts` carrying both surfaces or a second file, one `.cpp` or two, three
  `*Sigs.json` files or six, and — if any of those stay shared — which header's declarations come
  first. Those follow from how the V2 surface is laid out in the bindings package, so this waits on
  that rather than on 2.0.
- **The signature check could be wired into `pnpm run build` — as a warning, never a failure**
  (Jeff, 2026-09-05). It only warns today, and nothing invokes it, so a mismatch goes unnoticed.

  A DuckDB version bump is *just* the bump: nothing else is required to land it. If the new version
  declares functions we have not accounted for, the check starts warning, and it keeps warning until
  a separate later change adds the `// DUCKDB_C_API …` placeholders with a `TODO: <area>` reason.
  Implementation is later still. So the warning is not a nag inside one change — it is a reminder
  that persists across changes until someone picks the work up, which is precisely why it should not
  fail: a blocking check would drag both of those later steps into the version bump. Not scheduled;
  recorded so the "should it gate CI?" question does not get reopened.
- **The fetch scripts need `duckdb_v2.h`.** `fetch_libduckdb_*.py` extracts `duckdb.h` and the
  library from each release tarball; the tarball already carries `duckdb_v2.h`, so this is one more
  entry in each `files` list.
- **The bindings must opt into the unstable surface, and absorb its churn.** `binding.gyp` has to
  define `DUCKDB_V2_API_ALLOW_UNSTABLE=1`, or no V2 function is declared. And since unstable means
  no promise even across a patch release — 2.0.1 changed `parse_sql`'s parameters — a DuckDB bump
  that changes a V2 function the bindings call cannot land without the matching binding change: the
  compiler enforces it, whatever the signature check does. V1 bumps are unaffected. Until functions
  start graduating to stable, reading the `compare_api.py` diff *before* a bump says whether it can
  land alone.

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
- **Factories, not pairs.** The `_with_connection` / `_with_context` pairs this bullet used to
  weigh are gone upstream: values, types, data chunks, collections and functions are each created
  through one `duckdb_v2_factory_handle`, borrowed from an instance, connection or context and never
  destroyed. A 1:1 mapping exposes `instance_get_factory`, `connection_get_factory` and
  `context_get_factory`, with the factory a borrowed external that pins its parent. The config
  handle (§7) has the same shape.
- **`environment_create` maps onto the instance-cache pattern.** Not addon-owned state: the
  precedent is `DuckDBInstanceCache`, which the bindings expose 1:1 as `create_instance_cache()`
  while the API layer supplies a lazily-created `singleton` that `DuckDBInstance.fromCache()` routes
  through — and nothing stops a caller constructing more than one. A V2 environment gets the same
  shape: constructible for advanced use, with one auto-created default that ordinary instances are
  created under. The one difference is that V1's cache is optional (`DuckDBInstance.create` bypasses
  it) whereas `duckdb_v2_instance_create` requires an environment, so the default is implicit rather
  than opt-in. The September rework makes the default matter more: double-attach detection is per
  environment, so the default is what lets two parts of one app notice they opened the same file.
  The V2 `DuckDBInstance` itself maps 1:1 onto the C instance — created empty, then attached to —
  and a create-and-attach-in-one-call convenience is a follow-up for once the 1:1 layer has shown
  it is wanted, not part of the first cut.

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

- **It is not header-only**, despite the description. At v2.0.1, `tools/cpp/duckdb_cpp.hpp` is
  7,463 lines of declarations and `tools/cpp/duckdb_cpp.cpp` 7,410 lines of implementation that
  includes `duckdb_extension_v2.h` (6,446 and 6,521 in October). For a
  client library — us — that header binds the `duckdb_v2_*` symbols at link time, so no extension
  machinery is involved; extensions get a separate 48-line `duckdb_cpp_extension.hpp` for their
  entrypoint. The `.cpp` also turns on `DUCKDB_V2_API_ALLOW_UNSTABLE` for itself, since parts of the
  C++ API wrap unstable functions. Consuming it means adding that `.cpp` to `sources` in
  `binding.gyp` — fine in itself, but it means the release tarball has to ship the source file too,
  not just the header. The 2.0.1 release tarballs ship neither. The CMake module
  the team mentions does not help us: node-gyp is not CMake, and `FetchContent` has no equivalent
  here.
- **Exceptions are fine.** `duckdb_cpp` reports every failure by throwing, and `binding.gyp` already
  builds against `node_addon_api_except_all`, so C++ exceptions are enabled and the addon already
  converts them at the boundary. No build-configuration change needed.

Not required, and not a decision to make now: it is only worth evaluating once the thing actually
ships. The outcome may well be that we borrow its patterns — the RAII shapes, the way it models
owned versus borrowed — and roll our own, which costs nothing to decide late. If we do want to
consume it directly, the tarball has to carry both files.

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

### Value construction: one factory parameter

Decided in September as "keep `_with_connection` and `_with_context` as two functions rather than
one with a union-typed first argument", and overtaken by
[#26820](https://github.com/duckdb/duckdb/pull/26820), which removed the pairs upstream: every constructor now
takes one factory, borrowed from whichever root the caller holds.
The 1:1 mapping is therefore one function per constructor. The reasoning behind the decision now
applies to where the factory comes from: an external caller holds an instance or connection, a
callback holds a context, and the header is explicit that catalog-touching context calls belong in
bind-phase callbacks rather than exec-phase worker callbacks. How the V2 API surfaces the factory is
open.

### A known limitation to document: one file, both APIs

Because the point of a `v2` subpath is to let a consumer migrate incrementally, both APIs will run
in one process — and a database file opened through both is **not** detected. Measured against the
preview build (2026-10-10, after the instance rework), and unchanged on 2.0.1:

```
v1 open:                           ok
v1 open again:                     ok (no conflict reported)
v2 attach same file:               ok (NOT detected) (code 0)
v2 attach again, same instance:    refused (code 3001)
v2 attach again, second instance:  refused (code 3001)
v2 attach again, second env:       ok (NOT detected) (code 0)
```

(`coexist_probe.c`.)

V2 detects its own double-attach across every instance of one environment, because
`duckdb_v2_instance_attach` rejects a path "already attached on this or any other instance of the
environment" — but a V1 database of the same file is under no environment, and neither is an
instance of a *second* environment. V1's own deduplication only happens through an explicit instance
cache. The rework did not change the cross-API answer; it widened V2's own detection from one
database handle to the whole environment.

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
| What about the V1 date/time/decimal/hugeint conversion helpers? | Some may be; the team is "trying to avoid *convenience* functions" and expects clients to implement them, but they may ship as inline header functions or as docs. | We implement them in TypeScript. Nothing shipped in 2.0 or 2.0.1. |
| Will V1 be deprecated and retired? When? | "v1 will be deprecated in 2.0, and probably removed entirely in 3.0 (likely another year out)." | Side-by-side is a ~1-year window, not indefinite. |
| Will `duckdb_cpp` ship in 2.0? | Currently core-repo only; "most likely" added to the release tarballs soon, plus a proper CMake module for `FetchContent`. | Promising but uncommitted, and the CMake half does not help node-gyp. Not in the 2.0.1 release tarballs. |

## The scope target

Everything Node Neo exposes through V1 today should be reachable through the V2 API well before V1
is removed in 3.0 (Jeff, 2026-09-05). Not a 3.0 problem to plan around now — a definition of done
for the V2 API.

Concretely that is the 323 exposed V1 functions plus the API classes over them. Parity is not
function-for-function, though, because the two surfaces do not correspond one to one: 31 V1 logical
type functions collapse into a handful of V2 calls, 25 bind functions become one parameter array, 7
pending functions become `result_step`. The [coverage table](#the-two-surfaces-side-by-side) above
is the checklist that survives that — every V1 group with something in the "in Neo" column needs its
scenario answered in V2, whether by a mapped call, a restructured one, or something we build (the
appender being the clearest case of the last).

`compare_api.py` is the natural place to measure that as the V2 bindings land.

## How bulk append works without an appender

Context for the first deferred decision below. This is what the replacement is actually made of, so
the shape question can be answered on evidence rather than from the function names.

### Column data collection

A buffer-managed set of data chunks sharing one fixed set of column types: append chunks in, scan
them back out in order. DuckDB owns the memory and **spills to disk** when the rows outgrow it, so a
collection can hold far more than RAM. It knows nothing about tables — it is purely a buffer.

- It must not be scanned while it is being appended to, or the reverse.
- It must not be appended to concurrently. For parallel producers, fill one collection per thread
  and `combine` them at the end, which consumes the source.
- Scanning *is* parallel: a shared scan state coordinates work while per-thread worker states track
  progress. Scans are zero-copy, so a scanned chunk is valid only until that worker state's next
  scan call.

### Getting the collection into SQL

A collection knows nothing about SQL, so something has to show it to a statement. There are two
ways.

**`statement_add_collection`**, back since October
([#26886](https://github.com/duckdb/duckdb/pull/26886)) and now what upstream uses: bind the
collection to a parsed statement under a name, as a common table expression. The statement —
SELECT, INSERT, UPDATE, DELETE or MERGE INTO — can then be bound, prepared or executed, and each
execution reads the collection as it is at that moment. Nothing is registered anywhere: the binding
belongs to the statement. Columns default to `col1..colN`, or can be named; the name shadows a
catalog table, as a CTE does; and the collection is **borrowed, not copied** — the caller keeps
ownership and must keep it alive.

**A replacement scan**, the only way at the time of the first survey: a callback that fires during
query planning for **each table reference the catalog could not resolve**, and claims the name one
of three mutually exclusive ways — `set_function_name`, `set_subquery`, or `set_collection`, *read
this collection instead*. Registered on a connection, it is invisible to every other connection. No
scan *function* is involved: `set_collection` is a built-in claim form, so there are no bind / init
/ exec callbacks and no thread handling to write. But a registered scan cannot be unregistered; see
the sharp edges.

### The appender that falls out of that

1. Create a collection with the target table's column types.
2. Parse `INSERT INTO target (cols…) SELECT * FROM <buffer_name>` once, and bind the collection to it
   as `<buffer_name>`.
3. Append chunks into the collection — no database contact at all.
4. Flush by executing that statement and draining the result, then clear the collection.

**Where table functions fit instead.** A bound collection or a replacement scan answers a *name*
(`FROM my_buffer`); a table function is an *explicit call* (`FROM my_fn(args)`). For appending, the
buffer should look like a table, so a name is the natural fit. Table functions are the answer when
arguments are wanted, or a general SQL-callable producer.

### Upstream ships a reference implementation

`tools/cpp/duckdb_cpp.{hpp,cpp}` in the DuckDB repo contains a working `Appender` class built on the
public V2 API alone, with two constructors — one targeting a table, one taking an arbitrary query.
Since [#26886](https://github.com/duckdb/duckdb/pull/26886) it binds its buffer with
`statement_add_collection` rather than registering a replacement scan. This is the example the
DuckDB team pointed at, and it is worth reading before designing ours.

### The sharp edges

These are the parts that should drive the design, and none of them are visible from the function
list:

- **Replacement scans cannot be unregistered — so an appender should not use one.** A registered
  scan lives as long as its connection. The October design left one behind on every appender, which
  upstream mitigated by having the orphaned scan decline once its appender was gone, and which was
  awkward for a JS API, where a `DuckDBAppender` that is simply garbage-collected would still leave
  a registration behind. Binding the collection to the statement leaves nothing behind, which is
  why upstream moved its appender onto `statement_add_collection`.
- **The borrow outlives the obvious scope.** The collection must outlive the statement, any
  statement prepared from it, and any result executing it; destroying it earlier means reading freed
  memory. The header is not consistent about *clearing* it: it says a prepared statement can run
  again after the collection is cleared and refilled, and also that the collection must not be
  cleared while the statement is live. Upstream's appender keeps the parsed statement for its whole
  life and clears the collection after each drained flush, so clearing between executions, with no
  result open, is the pattern that is exercised.
- **Column resolution is a bind, not an execute.** The table-targeting constructor parses
  `SELECT * FROM <table>` and binds it to get names and types without reading a row — a use of
  `statement_bind` worth copying.
- **Generated columns need the query form.** The table form lists every column, and the engine
  refuses an INSERT that names a generated one.
- **Flush error semantics are deliberate.** Rows are kept on `RESOURCE_IN_USE` and
  `RUNTIME_INTERRUPT` so the flush can be retried, and dropped on anything else so a retry does not
  re-run a failing statement over the same rows.

### VARIANT in a data chunk: a V1 defect, fixed in 2.0.1

Worth knowing before the shape question is answered, because it ran the opposite way to the rest
of this document: **through 2.0.0, V1 could not put a VARIANT column in a data chunk, and V2 could.**
2.0.1 fixed the V1 side.

`duckdb_create_data_chunk` would not accept a VARIANT logical type. The logical type itself built
— `duckdb_create_logical_type` returned it, type id 41 — and it was the chunk call that refused,
by **returning null**, not by reporting anything. Measured against libduckdb 1.5.5 directly
(`chunk_probe.c`), where the refusal was also all-or-nothing rather than partial:

```
[INTEGER]            handle=non-NULL columns=1
[VARIANT]            handle=NULL
[INTEGER, VARIANT]   handle=NULL
[] (zero types)      handle=non-NULL columns=0
```

Note the last line: a chunk with no columns is a legitimate thing to create, so zero columns is
not itself the signal. The null is.

That distinction cost us a wrong diagnosis, and is worth keeping straight. A null handle read back
through `duckdb_data_chunk_get_column_count` returns 0, so from JS the refusal *looks* like a
chunk that merely came back empty. The binding passed the call straight through without inspecting
the result, so that null reached JS wrapped in an External, where every subsequent call on it is a
hazard — not only the append that happened to crash:

```ts
const chunk = DuckDBDataChunk.create([INTEGER, VARIANT]);  // null handle, no error
chunk.setRows([[5, null]]);                                // writes nothing
appender.appendDataChunk(chunk);                           // native crash
```

Fixed in [#487](https://github.com/duckdb/duckdb-node-neo/pull/487), in the binding, which is the
only place the null is visible.

The refusal carried into 2.0.0, and is gone in 2.0.1. It was a symptom of
[duckdb/duckdb#24680](https://github.com/duckdb/duckdb/issues/24680):
`duckdb_create_logical_type(DUCKDB_TYPE_VARIANT)` built the type without its child type info. On
2.0.0 that stopped failing cleanly — the bare type crashed some calls outright, which is why the
bindings refuse to create one
([duckdb/duckdb-node-neo#524](https://github.com/duckdb/duckdb-node-neo/issues/524)). The fix
(backported in [#28072](https://github.com/duckdb/duckdb/pull/28072)) builds the full type, and the
same probe now gets a chunk for every input:

```
[INTEGER]            handle=non-NULL columns=1
[VARIANT]            handle=non-NULL columns=1
[INTEGER, VARIANT]   handle=non-NULL columns=2
[] (zero types)      handle=non-NULL columns=0
```

So on 2.0.1 the refusal in #524 guards against a bug that is no longer there.

On V1 it was a gap rather than a wall even before the fix, because the per-value path works:
`appendVariant`, `appendValue(v, VARIANT)`, `bindValue(…, VARIANT)` and a SQL `::variant` cast all
write one, with `typeForValue` inferring a scalar's type when no hint is given and nested content requiring both a
wrapper and a type (`variantValue(structValue({k: 'n'}), STRUCT({k: VARCHAR}))`).

**V2 never had the defect.** `duckdb_v2_data_chunk_create` takes a VARIANT column, and so does
`duckdb_v2_column_data_collection_create` — which is the one that matters, since the collection is
what bulk append actually fills. Measured by `chunk_probe_v2.c` against the preview library and
again against 2.0.1, borrowing the VARIANT logical type from a result schema:

```
[INTEGER]            -> code=0 chunk=non-NULL columns=1
[VARIANT]            -> code=0 chunk=non-NULL columns=1
[INTEGER, VARIANT]   -> code=0 chunk=non-NULL columns=2

column data collection:
[INTEGER, VARIANT]   -> code=0 collection=non-NULL
```

So this is not a V2 prerequisite and there is nothing to raise upstream. Losing the per-value
appender does not cost us VARIANT.

What it does do is move the remaining gap onto our side of the line. `DuckDBVariantVector` cannot
write — `setItem` throws for a non-null value, and is a deliberate no-op for `null` so that a
containing struct or list can iterate every child while reading. Under V1 that barely matters,
since the per-value path is there to fall back on. Under V2 the collection is the only write path,
so a VARIANT column in a bulk append needs that vector to learn to write. That is ours to
implement whenever we want it, not a dependency to wait on — which is why it is not in the
deferred decisions below.

### Net versus the V1 appender

More capable: the flush does not have to be a plain INSERT, so a column subset, `ON CONFLICT`, or a
buffer-driven UPDATE or MERGE all become reachable, and the buffer spills to disk rather than being
memory-bound. It also accepted a VARIANT column when V1's own chunk path did not — see
[VARIANT in a data chunk](#variant-in-a-data-chunk-a-v1-defect-fixed-in-201). The cost is
ownership — the lifetime rules and the error semantics become ours.

## Deferred decisions

Real choices, deliberately not made yet, each with the thing that should trigger it:

| Decision | Trigger |
|---|---|
| What shape bulk append takes — an appender class, or helpers driving `INSERT INTO … SELECT * FROM` over a replacement-scanned collection (see [how it works](#how-bulk-append-works-without-an-appender)) | Building that part of the V2 API |
| Whether to build the V2 bindings on `duckdb_cpp`, borrow its patterns, or ignore it | `duckdb_cpp` actually shipping in a release; it is not required either way |
| Whether any conversion helpers are worth keeping native rather than reimplementing in TypeScript | Only if upstream ships some after all — otherwise TypeScript, which is the better choice regardless |

## On each DuckDB release

2.0 has shipped, and the checklist this section held for it has been worked through against 2.0.1:
the V1 drift held exactly, `open_result_probe.c` gave the same answer, the conversion helpers and
`duckdb_cpp` did not ship, V1's deprecation is still the compile switch described below, and the
V2 diff is under [What changed between surveys](#what-changed-between-surveys). Because V2 is
unstable and moves within patch releases, the refresh is now per release rather than once:

1. **Re-run `compare_api.py --fetch`** after the bindings' bump — it follows
   `bindings/scripts/fetch_libduckdb.py` — and diff the V2 function list against the previous
   release's, not just the count; `--json DIR` writes `v2_functions.json` to diff. `--ref
   v2.0-cyanoptera` shows what the next patch release will bring before it ships. The V1 drift
   blocks compare against the bindings' own header, so they should be empty.
2. **Check the unstable set.** The report lists every V2 function outside
   `DUCKDB_V2_API_ALLOW_UNSTABLE`. A function graduating out of it is a change too — and the first
   ones to do so are the first part of V2 the bindings can rely on across releases.
3. **Re-run the probes** against `fetch_libduckdb_v2.py`'s library, and `chunk_probe.c` and
   `open_result_probe.c` against the bindings' library as well, since those measure V1.
4. **Re-check what the team hedged on**: conversion helpers (none in 2.0.1, and no `static inline`
   functions at all), `duckdb_cpp.hpp` *and* `duckdb_cpp.cpp` in the release tarballs (neither in
   2.0.1), and V1 deprecation still being a compile switch rather than an attribute.

Still outstanding from the 2.0 checklist, because it belongs to the V2 bindings work rather than to a
version bump: adding `duckdb_v2.h` to each `fetch_libduckdb_*.py` `files` list. The fetch scripts
already point at the release, and the three `*Sigs.json` files are clean at 548 of 548.

## Deprecation is a compile switch, not a warning

Answered from the headers rather than by asking. 2.0's V1 header expresses deprecation as
*conditional inclusion*, the way `DUCKDB_API_NO_DEPRECATED` always did — so a deprecated V1 will not
put a warning on any of our 323 call sites:

- `DUCKDB_DEPRECATED` (the `__attribute__((deprecated))` macro) is defined in both headers and
  applied to nothing.
- Deprecated declarations sit inside `#if (DUCKDB_API_VERSION_BELOW(1, 0, 0) ||
  DUCKDB_API_ALLOW_DEPRECATED)`. `DUCKDB_API_ALLOW_DEPRECATED` defaults to 1, and back-derives from
  the legacy `DUCKDB_API_NO_DEPRECATED` for consumers who define that instead. `duckdb_v2.h` carries
  the same switch under `DUCKDB_V2_API_ALLOW_DEPRECATED`, with the same default and the same
  back-derivation.
- Node Neo defines neither switch, so the defaults apply and everything compiles in. If 2.0 marks V1
  deprecated wholesale, the fix is at most defining `DUCKDB_API_ALLOW_DEPRECATED 1` explicitly.

New in 2.0's V1 header, and already in 1.5.6's: **every** declaration is now wrapped in a
`DUCKDB_API_VERSION_AT_LEAST(major, minor, patch)` guard, with the targeted version defaulting to
1.5.6 and settable by defining all three of `DUCKDB_API_VERSION_MAJOR` / `_MINOR` / `_PATCH`. (Even
in 2.0.1 the default is still 1.5.6, and the two functions 2.0 added sit behind
`AT_LEAST(1, 5, 6)`, so they compile in by default.) That is
how a consumer pins an older surface, and it is the likeliest shape for V1's own retirement: a switch
to flip, not a diagnostic to suppress.

### Consequences for our tooling — all handled

The guard rewrite is invisible at the C level and was load-bearing for the two things that parse
these headers. Both were fixed ahead of 2.0, which turned out to be just in time: 1.5.6 shipped the
same generated header, and both held. Recorded here because the reasoning is not obvious from the
diff.

1. **`checkFunctionSignatures.mjs` guard tracking.** It recognized exactly
   `#ifndef DUCKDB_API_NO_DEPRECATED` and `#ifndef DUCKDB_NO_EXTENSION_FUNCTIONS`, and stamps each
   extracted signature with an `ifndef` field. The 1.5.5 header has 9 such blocks; the 2.0 header has
   1, in the preamble, and guards declarations with `#if DUCKDB_API_VERSION_AT_LEAST(…)` instead — so
   header signatures came out unstamped while the accounting comments in `duckdb_node_bindings.cpp`
   and `duckdb.d.ts` still carried their `// #ifndef DUCKDB_API_NO_DEPRECATED` markers, a mismatch on
   top of the thirteen `(void)` edits that regenerating the `*Sigs.json` files alone would not have
   settled. It now accepts both spellings and stamps both with the legacy name, so those markers need
   no rewriting. Note this is only the V1-side fix: making the script read `duckdb_v2.h` as well is
   still outstanding, and is discussed under [Migration approach](#migration-approach).
2. **`capi-coverage`'s deprecated flag was resting on prose.** `capi_functions.py` derives it from
   three signals: guard depth, `DEPRECATION NOTICE` in the doc comment, and `**DEPRECATED**`. Against
   the 2.0 header the guard signal contributed nothing, and the doc-text fallback carried all 48 on
   its own — the same 48, so nothing was broken, but right for the wrong reason, and it would have
   dropped to zero silently if that prose were ever reworded. It now recognizes the new guard form
   too, so the guard carries all 48 against both headers and the fallback is back to being a
   fallback.
3. **`capi-coverage`'s groups came from banners that are gone.** The generated header groups
   functions under `MODULE:` banners instead of `//----` rules, so against 1.5.6 every function came
   out with no group, and `compare_api.py` crashed formatting the group table. `capi_functions.py`
   now reads the module banner as the group. The group *names* change as a result (`value` rather
   than `Value Interface`), which `capi_coverage.json` will pick up the next time it is rebuilt; the
   function set and deprecation flags are unchanged.
