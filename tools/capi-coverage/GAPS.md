# Node Neo C API coverage gaps

_Generated 2026-09-05 against DuckDB 1.5.5 (546 `DUCKDB_C_API` functions)._

This is a generated summary of [capi_coverage.csv](capi_coverage.csv). Regenerate both
with `build_coverage.py` then `summarize_gaps.py`; see [README.md](README.md) for how
coverage is detected and what its limits are.

## What counts as a gap

A **gap** is a C API function that Node Neo does not expose, where:

- it is **not deprecated** in the C API, and
- Node Neo has **not deliberately skipped** it (`destroyed in finalizer`,
  `consolidated into open`, and similar), and
- **at least one other C-API-based client exposes it**.

Node Neo marks every unexposed function meeting the first two conditions as `TODO:`,
whatever other clients do, so gaps are a **subset** of the TODO list: of its 165 TODOs,
67 are gaps and the other 98 are functions no client exposes. Those are listed at the
end and are no less wanted — they simply carry no signal either way, because nobody has
built them.

The third condition is a **usefulness signal**: another client having surfaced a
function means someone had a concrete reason to want it. It is not a claim that the
function is easy or even sensible to bind in Node Neo — that varies by language, and
some of these will be wrong for a JS API. Weigh the signal by its breadth: a function
several clients expose is better evidence of demand than one only a single client does.

Which layer counts as "exposes" differs by client, because the six do not share an
architecture. Go and C# hand-write a selective binding layer, so a binding there is a
deliberate choice and is the signal. Rust (`libduckdb-sys`) and Julia (`src/api.jl`)
auto-generate a complete binding layer, and Swift publishes no raw C layer at all — for
those three only use from the idiomatic layer means anything.

## Where Node Neo stands

Both layers are shown, because which one carries meaning differs by client. In each
other client's row the **bold** figure is the one this document reads as that client
exposing a function; Node Neo is the subject of the comparison, so neither of its
columns is a signal.

| Client | Tier | Binding layer | Bindings | Bindings excl. deprecated | Idiomatic layer |
|---|---|---|---:|---:|---:|
| Node Neo | primary | hand-written, selective | 314/546 (58%) | 308/498 (62%) | 267/546 (49%) |
| Go | primary | hand-written, selective | **374/546 (68%)** | 359/498 (72%) | 271/546 (50%) |
| Rust | primary | generated, complete by construction | 546/546 (100%) | 498/498 (100%) | **217/546 (40%)** |
| C# | secondary | hand-written, selective | **282/546 (52%)** | 256/498 (51%) | 163/546 (30%) |
| Swift | tertiary | none published | n/a | n/a | **105/546 (19%)** |
| Julia | tertiary | generated, complete by construction | 546/546 (100%) | 498/498 (100%) | **182/546 (33%)** |

Only the hand-written binding layers are comparable to one another; the generated ones
sit at 100% by construction and say nothing about intent, which is why the idiomatic
column is what counts for Rust, Julia and Swift.

## The gaps — 67 functions across 19 areas

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
| appender create query | 1 | Go 1 |
| appender default | 1 | Go 1, C# 1 |
| appender error data | 1 | Go 1, C# 1, Rust 1 |
| register logical type | 1 | Go 1 |
| value to string | 1 | Go 1 |

### How strong the signal is

How many other clients expose each gap. More clients means better evidence that the
function is worth having, not that it is more urgent or more tractable.

| Exposed by | Gaps |
|---|---:|
| 3 clients | 9 |
| 2 clients | 16 |
| 1 client | 42 |

Most gaps rest on a single client — 42 of 67, and 36 of those on Go alone. Those are the
weakest evidence here: one project's judgement, made for one language's users. The
multi-client rows are the better-evidenced ones.

Gaps exposed by three or more clients:

| Function | Area | Exposed by |
|---|---|---|
| `duckdb_appender_error_data` | appender error data | Go, C#, Rust |
| `duckdb_destroy_arrow_options` | arrow | Go, C#, Rust |
| `duckdb_result_get_arrow_options` | arrow | Go, C#, Rust |
| `duckdb_to_arrow_schema` | arrow | Go, C#, Rust |
| `duckdb_data_chunk_to_arrow` | arrow | Go, C#, Rust |
| `duckdb_destroy_error_data` | error data | Go, C#, Rust |
| `duckdb_error_data_error_type` | error data | Go, C#, Rust |
| `duckdb_error_data_message` | error data | Go, C#, Rust |
| `duckdb_error_data_has_error` | error data | Go, C#, Rust |

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

## The rest of the TODO list — 98 functions no client exposes

These are also on Node Neo's TODO list, and are just as wanted or unwanted as anything
above; they simply carry no signal from other clients, because none has exposed them
either. Being behind here means being level with everyone.

| Area | Functions |
|---|---:|
| copy function | 36 |
| file system | 16 |
| aggregate function | 12 |
| cast function | 12 |
| config option | 9 |
| catalog | 7 |
| aggregate function set | 4 |
| tasks | 2 |

## Client revisions compared

Each client tracks its own DuckDB version, so some gaps are release lag rather than
a decision.

- Go (duckdb/duckdb-go-bindings) — 4628e33 on 2026-08-26
- Rust (duckdb/duckdb-rs) — 8c5daf8 on 2026-09-02
- C# (Giorgi/DuckDB.NET) — e56cdb8 on 2026-08-28
- Swift (duckdb/duckdb-swift) — 2bc1adc on 2026-07-22
- Julia (duckdb/DuckDB.jl) — 0c11cb7 on 2026-07-24

