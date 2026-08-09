import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';

// A created vector owns its memory, unlike one belonging to a data chunk, and is
// destroyed when collected. Referencing a value into one turns that value into
// something the vector reading path can read, which is the only way to get at the
// contents of an ARRAY or UNION value: the value accessors reject both, since
// duckdb_get_list_child and duckdb_get_struct_child guard on the exact type id.

suite('create vector', () => {
  test('create', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    const vector = duckdb.create_vector(int_type, 1);
    expect(vector).toBeTruthy();
    expect(duckdb.get_type_id(duckdb.vector_get_column_type(vector))).toBe(
      duckdb.Type.INTEGER,
    );
  });

  test('reference a primitive value', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    const vector = duckdb.create_vector(int_type, 1);
    duckdb.vector_reference_value(vector, duckdb.create_int32(42));
    const data = duckdb.vector_get_data(vector, 4);
    expect(new DataView(data.buffer, data.byteOffset).getInt32(0, true)).toBe(
      42,
    );
  });

  test('reference a varchar value', () => {
    const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
    const vector = duckdb.create_vector(varchar_type, 1);
    duckdb.vector_reference_value(vector, duckdb.create_varchar('hello'));
    // Inlined strings live in the vector's data; reading them here only needs to
    // confirm the vector was populated.
    const data = duckdb.vector_get_data(vector, 16);
    expect(new DataView(data.buffer, data.byteOffset).getUint32(0, true)).toBe(
      5,
    );
  });

  test('reference an array value', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    // The type argument is the element type, not the array type.
    const value = duckdb.create_array_value(int_type, [
      duckdb.create_int32(1),
      duckdb.create_int32(2),
      duckdb.create_int32(3),
    ]);
    const vector = duckdb.create_vector(duckdb.get_value_type(value), 1);
    duckdb.vector_reference_value(vector, value);
    const child = duckdb.array_vector_get_child(vector);
    const data = duckdb.vector_get_data(child, 3 * 4);
    const view = new DataView(data.buffer, data.byteOffset);
    expect([
      view.getInt32(0, true),
      view.getInt32(4, true),
      view.getInt32(8, true),
    ]).toStrictEqual([1, 2, 3]);
  });

  test('reference a union value', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
    const union_type = duckdb.create_union_type(
      [int_type, varchar_type],
      ['i', 's'],
    );
    const value = duckdb.create_union_value(
      union_type,
      1,
      duckdb.create_varchar('hello'),
    );
    const vector = duckdb.create_vector(duckdb.get_value_type(value), 1);
    duckdb.vector_reference_value(vector, value);
    // A union is physically a struct whose first child is the tag.
    const tagChild = duckdb.struct_vector_get_child(vector, 0);
    const tagData = duckdb.vector_get_data(tagChild, 1);
    expect(new DataView(tagData.buffer, tagData.byteOffset).getUint8(0)).toBe(1);
  });

  test('reference a list value', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    const value = duckdb.create_list_value(int_type, [
      duckdb.create_int32(7),
      duckdb.create_int32(8),
    ]);
    const vector = duckdb.create_vector(duckdb.get_value_type(value), 1);
    duckdb.vector_reference_value(vector, value);
    expect(duckdb.list_vector_get_size(vector)).toBe(2);
    const child = duckdb.list_vector_get_child(vector);
    const data = duckdb.vector_get_data(child, 2 * 4);
    const view = new DataView(data.buffer, data.byteOffset);
    expect([view.getInt32(0, true), view.getInt32(4, true)]).toStrictEqual([
      7, 8,
    ]);
  });

  test('many created vectors are reclaimed', () => {
    // Created vectors are destroyed by their finalizer rather than explicitly,
    // so this only checks that churning through a lot of them is well behaved.
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    for (let i = 0; i < 10000; i++) {
      const vector = duckdb.create_vector(int_type, 1);
      duckdb.vector_reference_value(vector, duckdb.create_int32(i));
    }
    expect(true).toBe(true);
  });
});
