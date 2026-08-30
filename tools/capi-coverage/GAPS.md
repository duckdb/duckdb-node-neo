# Node Neo C API coverage gaps

_Generated 2026-08-30 against DuckDB 1.5.5 (546 `DUCKDB_C_API` functions)._

This is a generated summary of [capi_coverage.csv](capi_coverage.csv). Regenerate both
with `build_coverage.py` then `summarize_gaps.py`; see [README.md](README.md) for how
coverage is detected and what its limits are.

## What counts as a gap

A **gap** is a C API function that Node Neo does not expose, where:

- it is **not deprecated** in the C API, and
- Node Neo has **not deliberately skipped** it (`destroyed in finalizer`,
  `consolidated into open`, and similar), and
- **at least one other C-API-based client does expose it** — so it is demonstrably
  bindable, and there is a reference implementation to work from.

In Node Neo's own accounting those are exactly the functions marked `TODO:`, which is
what this document reports. Functions no client exposes are listed separately at the
end: they are unbuilt C API surface generally, not somewhere Node Neo trails its peers.

"Exposed by another client" is read differently per client, because the six do not
share an architecture. Go and C# hand-write a selective binding layer, so a binding
existing there is a real signal. Rust (`libduckdb-sys`) and Julia (`src/api.jl`)
auto-generate a complete one, and Swift publishes no raw C layer at all — for those
three only use from the idiomatic layer counts.

## Where Node Neo stands

| Client | Tier | Binding layer | Bindings | Excl. deprecated |
|---|---|---|---:|---:|
| Node Neo | primary | hand-written | 306/546 (56%) | 300/498 (60%) |
| Go | primary | hand-written | 374/546 (68%) | 359/498 (72%) |
| Rust | primary | generated (complete by construction) | 546/546 (100%) | 498/498 (100%) |
| C# | secondary | hand-written | 282/546 (52%) | 256/498 (51%) |
| Swift | tertiary | none published | n/a | n/a |
| Julia | tertiary | generated (complete by construction) | 546/546 (100%) | 498/498 (100%) |

Only the hand-written rows are comparable to each other; the generated ones sit at
100% by construction and say nothing about intent.

Node Neo's 173 unexposed-but-wanted functions split into **68 gaps** (below) and **105 that no client has bound**.

## The gaps — 68 functions across 20 areas

Ordered by size. "Exposed by" counts how many functions in that area each client has.

| Area | Gap | Exposed by |
|---|---:|---|
| arrow | 8 | Go 8, C# 4, Rust 4 |
| table description | 8 | Go 8 |
| log storage | 6 | Go 6 |
| tasks | 6 | Julia 6 |
| error data | 5 | Go 5, Rust 5, C# 4 |
| profiling info | 5 | Go 5, Rust 4 |
| expression | 4 | Go 4 |
| replacement scan | 4 | Go 4, Julia 4 |
| scalar function set | 4 | Go 4, Rust 4 |
| selection vector | 3 | Go 3 |
| vector manipulation | 3 | Go 3 |
| appender columns | 2 | Go 2, Rust 2 |
| scalar function expression | 2 | Go 2 |
| utf8 | 2 | Go 2 |
| appender clear | 1 | Go 1, C# 1 |
| appender create query | 1 | Go 1 |
| appender default | 1 | Go 1, C# 1 |
| appender error data | 1 | Go 1, C# 1, Rust 1 |
| register logical type | 1 | Go 1 |
| value to string | 1 | Go 1 |

### Areas with a single reference implementation

- **Go only** — 31 functions across 10 areas: table description, log storage, expression, vector manipulation, selection vector, utf8, scalar function expression, value to string, register logical type, appender create query
- **Julia only** — 6 functions across 1 area: tasks

### Function detail

#### arrow (8)

| Function | Exposed by |
|---|---|
| `duckdb_connection_get_arrow_options` | Go |
| `duckdb_destroy_arrow_options` | Go, C#, Rust |
| `duckdb_result_get_arrow_options` | Go, C#, Rust |
| `duckdb_to_arrow_schema` | Go, C#, Rust |
| `duckdb_data_chunk_to_arrow` | Go, C#, Rust |
| `duckdb_schema_from_arrow` | Go |
| `duckdb_data_chunk_from_arrow` | Go |
| `duckdb_destroy_arrow_converted_schema` | Go |

#### table description (8)

| Function | Exposed by |
|---|---|
| `duckdb_table_description_create` | Go |
| `duckdb_table_description_create_ext` | Go |
| `duckdb_table_description_destroy` | Go |
| `duckdb_table_description_error` | Go |
| `duckdb_column_has_default` | Go |
| `duckdb_table_description_get_column_count` | Go |
| `duckdb_table_description_get_column_name` | Go |
| `duckdb_table_description_get_column_type` | Go |

#### log storage (6)

| Function | Exposed by |
|---|---|
| `duckdb_create_log_storage` | Go |
| `duckdb_destroy_log_storage` | Go |
| `duckdb_log_storage_set_write_log_entry` | Go |
| `duckdb_log_storage_set_extra_data` | Go |
| `duckdb_log_storage_set_name` | Go |
| `duckdb_register_log_storage` | Go |

#### tasks (6)

| Function | Exposed by |
|---|---|
| `duckdb_create_task_state` | Julia |
| `duckdb_execute_n_tasks_state` | Julia |
| `duckdb_finish_execution` | Julia |
| `duckdb_task_state_is_finished` | Julia |
| `duckdb_destroy_task_state` | Julia |
| `duckdb_execution_is_finished` | Julia |

#### error data (5)

| Function | Exposed by |
|---|---|
| `duckdb_create_error_data` | Go, Rust |
| `duckdb_destroy_error_data` | Go, C#, Rust |
| `duckdb_error_data_error_type` | Go, C#, Rust |
| `duckdb_error_data_message` | Go, C#, Rust |
| `duckdb_error_data_has_error` | Go, C#, Rust |

#### profiling info (5)

| Function | Exposed by |
|---|---|
| `duckdb_get_profiling_info` | Go, Rust |
| `duckdb_profiling_info_get_value` | Go |
| `duckdb_profiling_info_get_metrics` | Go, Rust |
| `duckdb_profiling_info_get_child_count` | Go, Rust |
| `duckdb_profiling_info_get_child` | Go, Rust |

#### expression (4)

| Function | Exposed by |
|---|---|
| `duckdb_destroy_expression` | Go |
| `duckdb_expression_return_type` | Go |
| `duckdb_expression_is_foldable` | Go |
| `duckdb_expression_fold` | Go |

#### replacement scan (4)

| Function | Exposed by |
|---|---|
| `duckdb_add_replacement_scan` | Go, Julia |
| `duckdb_replacement_scan_set_function_name` | Go, Julia |
| `duckdb_replacement_scan_add_parameter` | Go, Julia |
| `duckdb_replacement_scan_set_error` | Go, Julia |

#### scalar function set (4)

| Function | Exposed by |
|---|---|
| `duckdb_create_scalar_function_set` | Go, Rust |
| `duckdb_destroy_scalar_function_set` | Go, Rust |
| `duckdb_add_scalar_function_to_set` | Go, Rust |
| `duckdb_register_scalar_function_set` | Go, Rust |

#### selection vector (3)

| Function | Exposed by |
|---|---|
| `duckdb_create_selection_vector` | Go |
| `duckdb_destroy_selection_vector` | Go |
| `duckdb_selection_vector_get_data_ptr` | Go |

#### vector manipulation (3)

| Function | Exposed by |
|---|---|
| `duckdb_slice_vector` | Go |
| `duckdb_vector_copy_sel` | Go |
| `duckdb_vector_reference_vector` | Go |

#### appender columns (2)

| Function | Exposed by |
|---|---|
| `duckdb_appender_add_column` | Go, Rust |
| `duckdb_appender_clear_columns` | Go, Rust |

#### scalar function expression (2)

| Function | Exposed by |
|---|---|
| `duckdb_scalar_function_bind_get_argument_count` | Go |
| `duckdb_scalar_function_bind_get_argument` | Go |

#### utf8 (2)

| Function | Exposed by |
|---|---|
| `duckdb_valid_utf8_check` | Go |
| `duckdb_unsafe_vector_assign_string_element_len` | Go |

#### appender clear (1)

| Function | Exposed by |
|---|---|
| `duckdb_appender_clear` | Go, C# |

#### appender create query (1)

| Function | Exposed by |
|---|---|
| `duckdb_appender_create_query` | Go |

#### appender default (1)

| Function | Exposed by |
|---|---|
| `duckdb_append_default_to_chunk` | Go, C# |

#### appender error data (1)

| Function | Exposed by |
|---|---|
| `duckdb_appender_error_data` | Go, C#, Rust |

#### register logical type (1)

| Function | Exposed by |
|---|---|
| `duckdb_register_logical_type` | Go |

#### value to string (1)

| Function | Exposed by |
|---|---|
| `duckdb_value_to_string` | Go |

## Not a gap: unclaimed by every client — 105 functions

Node Neo does not expose these, but neither does any other client. Being behind here
means being level with everyone, which is a different prioritisation signal.

| Area | Functions |
|---|---:|
| copy function | 36 |
| file system | 16 |
| aggregate function | 12 |
| cast function | 12 |
| config option | 9 |
| scalar function init | 7 |
| catalog | 7 |
| aggregate function set | 4 |
| tasks | 2 |

## Client revisions compared

Each client tracks its own DuckDB version, so some gaps are release lag rather than
a decision.

- Go (duckdb/duckdb-go-bindings) — 4628e33 on 2026-08-26
- Rust (duckdb/duckdb-rs) — 199547d on 2026-08-28
- C# (Giorgi/DuckDB.NET) — e56cdb8 on 2026-08-28
- Swift (duckdb/duckdb-swift) — 2bc1adc on 2026-07-22
- Julia (duckdb/DuckDB.jl) — 0c11cb7 on 2026-07-24

