import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';

// An index past the end reaches the C API three different ways. Most report it
// in the return value -- a null handle, or DUCKDB_TYPE_INVALID -- so the
// binding tests that and fetches the count only to build the message. The
// struct and union type accessors throw a C++ exception instead, and
// duckdb_enum_dictionary_value reads out of bounds and segfaults; those are
// checked before the call. Either way the caller gets the same RangeError.
suite('index bounds', () => {
  test('data chunk column', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    const chunk = duckdb.create_data_chunk([int_type, int_type]);
    expect(() => duckdb.data_chunk_get_vector(chunk, 2)).toThrowError(
      'column index 2 is out of range (count: 2)'
    );
    // A negative index arrives as a large unsigned one, so it lands here too.
    expect(() => duckdb.data_chunk_get_vector(chunk, -1)).toThrowError(
      'is out of range (count: 2)'
    );
    expect(() => duckdb.data_chunk_get_vector(chunk, 1)).not.toThrow();
  });

  test('result and prepared statement columns', async () => {
    const db = await duckdb.open();
    const con = await duckdb.connect(db);
    const result = await duckdb.query(con, `select 1 as a`);
    expect(duckdb.column_name(result, 0)).toBe('a');
    // These report out of range in the return value -- null, or type id 0,
    // which reads as INVALID and is indistinguishable from a real answer.
    expect(() => duckdb.column_name(result, 1)).toThrowError(
      'column index 1 is out of range (count: 1)'
    );
    expect(() => duckdb.column_type(result, 1)).toThrowError(
      'column index 1 is out of range (count: 1)'
    );
    expect(() => duckdb.column_logical_type(result, 1)).toThrowError(
      'column index 1 is out of range (count: 1)'
    );
    const prepared = await duckdb.prepare(con, `select 1 as a`);
    expect(() => duckdb.prepared_statement_column_type(prepared, 1)).toThrowError(
      'column index 1 is out of range (count: 1)'
    );
  });

  test('prepared statement parameters', async () => {
    const db = await duckdb.open();
    const con = await duckdb.connect(db);
    // Parameter indexes are 1-based, so the count is the largest valid one.
    const prepared = await duckdb.prepare(con, `select ?::integer`);
    expect(duckdb.nparams(prepared)).toBe(1);
    expect(duckdb.parameter_name(prepared, 1)).toBe('1');
    expect(() => duckdb.parameter_name(prepared, 2)).toThrowError(
      'parameter index 2 is out of range (count: 1)'
    );
    expect(() => duckdb.param_type(prepared, 2)).toThrowError(
      'parameter index 2 is out of range (count: 1)'
    );
  });

  test('the sentinel means out of range and nothing else', async () => {
    // The accessors above report an out-of-range index by returning null or
    // DUCKDB_TYPE_INVALID, which only works as a signal if nothing else
    // produces it. A failed query or prepare throws rather than handing back a
    // result to ask, and no column in range reports a sentinel -- including
    // ones with no natural name, and the types most likely to be special.
    const db = await duckdb.open();
    const con = await duckdb.connect(db);
    for (const sql of [
      `select 1 + 1`,
      `select ''`,
      `select null`,
      `select [] as a`,
      `select {'a': 1} as a`,
      `select union_value(k := 1) as a`,
      `select 'x'::enum('x','y') as a`,
      `select null::variant as a`,
      `select interval 1 day as a`,
    ]) {
      const result = await duckdb.query(con, sql);
      expect(duckdb.column_name(result, 0), sql).toBeTruthy();
      expect(duckdb.column_type(result, 0), sql).not.toBe(duckdb.Type.INVALID);
      expect(duckdb.column_logical_type(result, 0), sql).toBeTruthy();
    }
    await expect(duckdb.query(con, 'select nope')).rejects.toThrow();
    await expect(duckdb.prepare(con, 'select nope')).rejects.toThrow();
  });

  test('an appender reports its column types whatever state it is in', async () => {
    // Null from appender_column_type has to mean out of range only, so it must
    // not come back for a column in range after a failure or a close.
    const db = await duckdb.open();
    const con = await duckdb.connect(db);
    await duckdb.query(con, 'create table t(a integer, b varchar)');
    const appender = await duckdb.appender_create(con, null, 't');
    expect(duckdb.appender_column_type(appender, 0)).toBeTruthy();
    try {
      duckdb.append_varchar(appender, 'wrong type for column a');
      duckdb.appender_end_row(appender);
    } catch {
      // expected; what matters is the appender afterwards
    }
    expect(duckdb.appender_column_type(appender, 0)).toBeTruthy();
    await duckdb.appender_close_sync(appender);
    expect(duckdb.appender_column_type(appender, 0)).toBeTruthy();
  });

  test('enum dictionary value', () => {
    // This one segfaulted rather than raising.
    const enum_type = duckdb.create_enum_type(['alpha', 'beta']);
    expect(duckdb.enum_dictionary_value(enum_type, 1)).toBe('beta');
    expect(() => duckdb.enum_dictionary_value(enum_type, 2)).toThrowError(
      'enum value index 2 is out of range (count: 2)'
    );
  });

  test('struct type entry', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    const struct_type = duckdb.create_struct_type([int_type], ['a']);
    expect(duckdb.struct_type_child_name(struct_type, 0)).toBe('a');
    expect(() => duckdb.struct_type_child_name(struct_type, 1)).toThrowError(
      'struct entry index 1 is out of range (count: 1)'
    );
    expect(() => duckdb.struct_type_child_type(struct_type, 1)).toThrowError(
      'struct entry index 1 is out of range (count: 1)'
    );
  });

  test('union type member', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    const union_type = duckdb.create_union_type([int_type], ['a']);
    expect(duckdb.union_type_member_name(union_type, 0)).toBe('a');
    expect(() => duckdb.union_type_member_name(union_type, 1)).toThrowError(
      'union member index 1 is out of range (count: 1)'
    );
    expect(() => duckdb.union_type_member_type(union_type, 1)).toThrowError(
      'union member index 1 is out of range (count: 1)'
    );
  });

  test('struct vector child is not bounds-checked, but does not crash', () => {
    // The one indexed accessor left unchecked: a vector carries no child
    // count, so checking would mean materializing its logical type on every
    // call. Out of range still raises rather than crashing, just without
    // saying what went wrong.
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    const struct_type = duckdb.create_struct_type([int_type], ['a']);
    const chunk = duckdb.create_data_chunk([struct_type]);
    const vector = duckdb.data_chunk_get_vector(chunk, 0);
    expect(() => duckdb.struct_vector_get_child(vector, 0)).not.toThrow();
    expect(() => duckdb.struct_vector_get_child(vector, 1)).toThrow();
  });
});
