import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';

function isValid(validity: BigUint64Array, bit: number): boolean {
  return (validity[Math.floor(bit / 64)] & (1n << BigInt(bit % 64))) !== 0n;
}

function expectValidity(validity_bytes: Uint8Array, validity: BigUint64Array, bit: number, expected: boolean) {
  expect(duckdb.validity_row_is_valid(validity_bytes, bit)).toBe(expected);
  expect(isValid(validity, bit)).toBe(expected);
}

/**
 * Gets the bytes either in or referenced by a `duckdb_string_t`
 * that is at `string_byte_offset` of the given `DataView`.
 */
function getStringBytes(dv: DataView, string_byte_offset: number): Uint8Array {
  const length_in_bytes = dv.getUint32(string_byte_offset, true);
  if (length_in_bytes <= 12) {
    return new Uint8Array(dv.buffer, dv.byteOffset + string_byte_offset + 4, length_in_bytes);
  } else {
    return duckdb.get_data_from_pointer(dv.buffer, dv.byteOffset + string_byte_offset + 8, length_in_bytes);
  }
}

const decoder = new TextDecoder();

/**
 * Gets the UTF-8 string either in or referenced by a `duckdb_string_t`
 * that is at `string_byte_offset` of the given `DataView`.
 */
function getVarchar(dv: DataView, string_byte_offset: number): string {
  return decoder.decode(getStringBytes(dv, string_byte_offset));
}

suite('query', () => {
  test('basic select', async () => {
    const db = await duckdb.open();
    try {
      const con = await duckdb.connect(db);
      try {
        const res = await duckdb.query(con, 'select 17 as seventeen');
        try {
          expect(duckdb.result_statement_type(res)).toBe(duckdb.StatementType.SELECT);
          expect(duckdb.result_return_type(res)).toBe(duckdb.ResultType.QUERY_RESULT);
          expect(duckdb.rows_changed(res)).toBe(0);
          expect(duckdb.column_count(res)).toBe(1);
          expect(duckdb.column_name(res, 0)).toBe('seventeen');
          expect(duckdb.column_type(res, 0)).toBe(duckdb.Type.INTEGER);
          const col_0_logical_type = duckdb.column_logical_type(res, 0);
          try {
            expect(duckdb.get_type_id(col_0_logical_type)).toBe(duckdb.Type.INTEGER);
          } finally {
            duckdb.destroy_logical_type(col_0_logical_type);
          }
          const chunk = await duckdb.fetch_chunk(res);
          try {
            expect(duckdb.data_chunk_get_column_count(chunk)).toBe(1);
            expect(duckdb.data_chunk_get_size(chunk)).toBe(1);
            const vector = duckdb.data_chunk_get_vector(chunk, 0);
            const logical_type = duckdb.vector_get_column_type(vector);
            expect(duckdb.get_type_id(logical_type)).toBe(duckdb.Type.INTEGER);
            const validityBytes = duckdb.vector_get_validity(vector, 8);
            const validity = new BigUint64Array(validityBytes.buffer, 0, 1);
            const data = duckdb.vector_get_data(vector, 4);
            const dv = new DataView(data.buffer);
            expect(isValid(validity, 0)).toBe(true);
            const value = dv.getInt32(0, true);
            expect(value).toBe(17);
          } finally {
            duckdb.destroy_data_chunk(chunk);
          }
        } finally {
          duckdb.destroy_result(res);
        }
      } finally {
        await duckdb.disconnect(con);
      }
    } finally {
      await duckdb.close(db);
    }
  });
  test('basic error', async () => {
    const db = await duckdb.open();
    try {
      const con = await duckdb.connect(db);
      try {
        await expect(duckdb.query(con, 'selct 1')).rejects.toThrow('Parser Error');
      } finally {
        await duckdb.disconnect(con);
      }
    } finally {
      await duckdb.close(db);
    }
  });
  test('test_all_types()', async () => {
    const db = await duckdb.open();
    try {
      const con = await duckdb.connect(db);
      try {
        const res = await duckdb.query(con, 'from test_all_types()');
        try {
          expect(duckdb.result_statement_type(res)).toBe(duckdb.StatementType.SELECT);
          expect(duckdb.result_return_type(res)).toBe(duckdb.ResultType.QUERY_RESULT);
          expect(duckdb.column_count(res)).toBe(53);

          expect(duckdb.column_name(res, 0)).toBe('bool');
          expect(duckdb.column_type(res, 0)).toBe(duckdb.Type.BOOLEAN);
          const col_0_logical_type = duckdb.column_logical_type(res, 0);
          try {
            expect(duckdb.get_type_id(col_0_logical_type)).toBe(duckdb.Type.BOOLEAN);
          } finally {
            duckdb.destroy_logical_type(col_0_logical_type);
          }

          expect(duckdb.column_name(res, 27)).toBe('varchar');
          expect(duckdb.column_type(res, 27)).toBe(duckdb.Type.VARCHAR);
          const col_27_logical_type = duckdb.column_logical_type(res, 27);
          try {
            expect(duckdb.get_type_id(col_27_logical_type)).toBe(duckdb.Type.VARCHAR);
          } finally {
            duckdb.destroy_logical_type(col_27_logical_type);
          }

          expect(duckdb.column_name(res, 33)).toBe('int_array');
          expect(duckdb.column_type(res, 33)).toBe(duckdb.Type.LIST);
          const col_33_logical_type = duckdb.column_logical_type(res, 33);
          try {
            expect(duckdb.get_type_id(col_33_logical_type)).toBe(duckdb.Type.LIST);
            const child_type = duckdb.list_type_child_type(col_33_logical_type);
            try {
              expect(duckdb.get_type_id(child_type)).toBe(duckdb.Type.INTEGER);
            } finally {
              duckdb.destroy_logical_type(child_type);
            }
          } finally {
            duckdb.destroy_logical_type(col_33_logical_type);
          }

          expect(duckdb.column_name(res, 40)).toBe('struct');
          expect(duckdb.column_type(res, 40)).toBe(duckdb.Type.STRUCT);
          const col_40_logical_type = duckdb.column_logical_type(res, 40);
          try {
            expect(duckdb.get_type_id(col_40_logical_type)).toBe(duckdb.Type.STRUCT);
            expect(duckdb.struct_type_child_count(col_40_logical_type)).toBe(2);
            expect(duckdb.struct_type_child_name(col_40_logical_type, 0)).toBe('a');
            expect(duckdb.struct_type_child_name(col_40_logical_type, 1)).toBe('b');
            const member_type_0 = duckdb.struct_type_child_type(col_40_logical_type, 0);
            try {
              expect(duckdb.get_type_id(member_type_0)).toBe(duckdb.Type.INTEGER);
            } finally {
              duckdb.destroy_logical_type(member_type_0);
            }
            const member_type_1 = duckdb.struct_type_child_type(col_40_logical_type, 1);
            try {
              expect(duckdb.get_type_id(member_type_1)).toBe(duckdb.Type.VARCHAR);
            } finally {
              duckdb.destroy_logical_type(member_type_1);
            }
          } finally {
            duckdb.destroy_logical_type(col_40_logical_type);
          }

          expect(duckdb.column_name(res, 45)).toBe('fixed_int_array');
          expect(duckdb.column_type(res, 45)).toBe(duckdb.Type.ARRAY);
          const col_45_logical_type = duckdb.column_logical_type(res, 45);
          try {
            expect(duckdb.get_type_id(col_45_logical_type)).toBe(duckdb.Type.ARRAY);
            expect(duckdb.array_type_array_size(col_45_logical_type)).toBe(3);
            const child_type = duckdb.array_type_child_type(col_45_logical_type);
            try {
              expect(duckdb.get_type_id(child_type)).toBe(duckdb.Type.INTEGER);
            } finally {
              duckdb.destroy_logical_type(child_type);
            }
          } finally {
            duckdb.destroy_logical_type(col_45_logical_type);
          }

          expect(duckdb.column_name(res, 52)).toBe('list_of_fixed_int_array');
          expect(duckdb.column_type(res, 52)).toBe(duckdb.Type.LIST);
          const col_52_logical_type = duckdb.column_logical_type(res, 52);
          try {
            expect(duckdb.get_type_id(col_52_logical_type)).toBe(duckdb.Type.LIST);
            const child_type = duckdb.list_type_child_type(col_52_logical_type);
            try {
              expect(duckdb.get_type_id(child_type)).toBe(duckdb.Type.ARRAY);
              expect(duckdb.array_type_array_size(child_type)).toBe(3);
              const array_child_type = duckdb.array_type_child_type(child_type);
              try {
                expect(duckdb.get_type_id(array_child_type)).toBe(duckdb.Type.INTEGER);
              } finally {
                duckdb.destroy_logical_type(array_child_type);
              }
            } finally {
              duckdb.destroy_logical_type(child_type);
            }
          } finally {
            duckdb.destroy_logical_type(col_52_logical_type);
          }

          const chunk = await duckdb.fetch_chunk(res);
          try {
            expect(duckdb.data_chunk_get_column_count(chunk)).toBe(53);
            expect(duckdb.data_chunk_get_size(chunk)).toBe(3);

            // bool
            const bool_vector = duckdb.data_chunk_get_vector(chunk, 0);
            const bool_validity_bytes = duckdb.vector_get_validity(bool_vector, 8);
            const bool_validity = new BigUint64Array(bool_validity_bytes.buffer, 0, 1);
            const bool_data = duckdb.vector_get_data(bool_vector, 3);
            const bool_dv = new DataView(bool_data.buffer);

            expectValidity(bool_validity_bytes, bool_validity, 0, true);
            const bool_value0 = bool_dv.getUint8(0) !== 0;
            expect(bool_value0).toBe(false);

            expectValidity(bool_validity_bytes, bool_validity, 1, true);
            const bool_value1 = bool_dv.getUint8(1) !== 0;
            expect(bool_value1).toBe(true);

            expectValidity(bool_validity_bytes, bool_validity, 2, false);

            // varchar
            const varchar_vector = duckdb.data_chunk_get_vector(chunk, 27);
            const varchar_validity_bytes = duckdb.vector_get_validity(varchar_vector, 8);
            const varchar_validity = new BigUint64Array(varchar_validity_bytes.buffer, 0, 1);
            const varchar_data = duckdb.vector_get_data(varchar_vector, 3*16);
            const varchar_dv = new DataView(varchar_data.buffer);

            expectValidity(varchar_validity_bytes, varchar_validity, 0, true);
            expect(getVarchar(varchar_dv, 0*16)).toBe('🦆🦆🦆🦆🦆🦆');

            expectValidity(varchar_validity_bytes, varchar_validity, 1, true);
            expect(getVarchar(varchar_dv, 1*16)).toBe('goo\0se');

            expectValidity(varchar_validity_bytes, varchar_validity, 2, false);

            // int_array
            const int_array_vector = duckdb.data_chunk_get_vector(chunk, 33);
            const int_array_validity_bytes = duckdb.vector_get_validity(int_array_vector, 8);
            const int_array_validity = new BigUint64Array(int_array_validity_bytes.buffer, 0, 1);
            const int_array_entry_data = duckdb.vector_get_data(int_array_vector, 3 * 16);
            const int_array_entry_dv = new DataView(int_array_entry_data.buffer);
            const int_array_child = duckdb.list_vector_get_child(int_array_vector);
            const int_array_child_size = duckdb.list_vector_get_size(int_array_vector);
            const int_array_child_validity_bytes = duckdb.vector_get_validity(int_array_child, 8);
            const int_array_child_validity = new BigUint64Array(int_array_child_validity_bytes.buffer, 0, 1);
            const int_array_child_data = duckdb.vector_get_data(int_array_child, int_array_child_size * 4);
            const int_array_child_dv = new DataView(int_array_child_data.buffer);

            expectValidity(int_array_validity_bytes, int_array_validity, 0, true);
            const value0_offset = int_array_entry_dv.getBigUint64(0, true);
            const value0_length = int_array_entry_dv.getBigUint64(8, true);
            expect(value0_offset).toBe(0n);
            expect(value0_length).toBe(0n);

            expectValidity(int_array_validity_bytes, int_array_validity, 1, true);
            const value1_offset = int_array_entry_dv.getBigUint64(16, true);
            const value1_length = int_array_entry_dv.getBigUint64(24, true);
            expect(value1_offset).toBe(0n);
            expect(value1_length).toBe(5n);

            expectValidity(int_array_child_validity_bytes, int_array_child_validity, Number(value1_offset)+0, true);
            expect(int_array_child_dv.getInt32(Number(value1_offset)+0*4, true)).toBe(42);

            expectValidity(int_array_child_validity_bytes, int_array_child_validity, Number(value1_offset)+1, true);
            expect(int_array_child_dv.getInt32(Number(value1_offset)+1*4, true)).toBe(999);

            expectValidity(int_array_child_validity_bytes, int_array_child_validity, Number(value1_offset)+2, false);

            expectValidity(int_array_child_validity_bytes, int_array_child_validity, Number(value1_offset)+3, false);

            expectValidity(int_array_child_validity_bytes, int_array_child_validity, Number(value1_offset)+4, true);
            expect(int_array_child_dv.getInt32(Number(value1_offset)+4*4, true)).toBe(-42);

            expectValidity(int_array_validity_bytes, int_array_validity, 2, false);
            const value2_offset = int_array_entry_dv.getBigUint64(32, true);
            const value2_length = int_array_entry_dv.getBigUint64(40, true);
            expect(value2_offset).toBe(0n);
            expect(value2_length).toBe(0n);

            // struct
            const struct_vector = duckdb.data_chunk_get_vector(chunk, 40);
            const struct_validity_bytes = duckdb.vector_get_validity(struct_vector, 8);
            const struct_validity = new BigUint64Array(struct_validity_bytes.buffer, 0, 1);
            const struct_child0 = duckdb.struct_vector_get_child(struct_vector, 0);
            const struct_child0_validity_bytes = duckdb.vector_get_validity(struct_child0, 8);
            const struct_child0_validity = new BigUint64Array(struct_child0_validity_bytes.buffer, 0, 1);
            const struct_child0_data = duckdb.vector_get_data(struct_child0, 3*4);
            const struct_child0_dv = new DataView(struct_child0_data.buffer);
            const struct_child1 = duckdb.struct_vector_get_child(struct_vector, 1);
            const struct_child1_validity_bytes = duckdb.vector_get_validity(struct_child1, 8);
            const struct_child1_validity = new BigUint64Array(struct_child1_validity_bytes.buffer, 0, 1);
            const struct_child1_data = duckdb.vector_get_data(struct_child1, 3*16);
            const struct_child1_dv = new DataView(struct_child1_data.buffer);

            expectValidity(struct_validity_bytes, struct_validity, 0, true);
            expectValidity(struct_child0_validity_bytes, struct_child0_validity, 0, false);
            expectValidity(struct_child1_validity_bytes, struct_child1_validity, 0, false);

            expectValidity(struct_validity_bytes, struct_validity, 1, true);
            expectValidity(struct_child0_validity_bytes, struct_child0_validity, 1, true);
            expect(struct_child0_dv.getInt32(1*4, true)).toBe(42);
            expectValidity(struct_child1_validity_bytes, struct_child1_validity, 1, true);
            expect(struct_child1_dv.getInt32(1*16, true)).toBe(24);
            expect(getVarchar(struct_child1_dv, 1*16)).toBe('🦆🦆🦆🦆🦆🦆');

            expectValidity(struct_validity_bytes, struct_validity, 2, false);
            expectValidity(struct_child0_validity_bytes, struct_child0_validity, 2, false);
            expectValidity(struct_child1_validity_bytes, struct_child1_validity, 2, false);

            // fixed_int_array
            const fixed_int_array_vector = duckdb.data_chunk_get_vector(chunk, 45);
            const fixed_int_array_validity_bytes = duckdb.vector_get_validity(fixed_int_array_vector, 8);
            const fixed_int_array_validity = new BigUint64Array(fixed_int_array_validity_bytes.buffer, 0, 1);
            const fixed_int_array_child_vector = duckdb.array_vector_get_child(fixed_int_array_vector);
            const fixed_int_array_child_validity_bytes = duckdb.vector_get_validity(fixed_int_array_child_vector, 8);
            const fixed_int_array_child_validity = new BigUint64Array(fixed_int_array_child_validity_bytes.buffer, 0, 1);
            const fixed_int_array_child_data = duckdb.vector_get_data(fixed_int_array_child_vector, 3*3*4);
            const fixed_int_array_child_dv = new DataView(fixed_int_array_child_data.buffer);

            expectValidity(fixed_int_array_validity_bytes, fixed_int_array_validity, 0, true);
            expectValidity(fixed_int_array_child_validity_bytes, fixed_int_array_child_validity, 0*3+0, false);
            expectValidity(fixed_int_array_child_validity_bytes, fixed_int_array_child_validity, 0*3+1, true);
            expect(fixed_int_array_child_dv.getInt32((0*3+1)*4, true)).toBe(2);
            expectValidity(fixed_int_array_child_validity_bytes, fixed_int_array_child_validity, 0*3+2, true);
            expect(fixed_int_array_child_dv.getInt32((0*3+2)*4, true)).toBe(3);

            expectValidity(fixed_int_array_validity_bytes, fixed_int_array_validity, 1, true);
            expectValidity(fixed_int_array_child_validity_bytes, fixed_int_array_child_validity, 1*3+0, true);
            expect(fixed_int_array_child_dv.getInt32((1*3+0)*4, true)).toBe(4);
            expectValidity(fixed_int_array_child_validity_bytes, fixed_int_array_child_validity, 1*3+1, true);
            expect(fixed_int_array_child_dv.getInt32((1*3+1)*4, true)).toBe(5);
            expectValidity(fixed_int_array_child_validity_bytes, fixed_int_array_child_validity, 1*3+2, true);
            expect(fixed_int_array_child_dv.getInt32((1*3+2)*4, true)).toBe(6);

            expectValidity(fixed_int_array_validity_bytes, fixed_int_array_validity, 2, false);
            expectValidity(fixed_int_array_child_validity_bytes, fixed_int_array_child_validity, 2*3+0, false);
            expectValidity(fixed_int_array_child_validity_bytes, fixed_int_array_child_validity, 2*3+1, false);
            expectValidity(fixed_int_array_child_validity_bytes, fixed_int_array_child_validity, 2*3+2, false);

          } finally {
            duckdb.destroy_data_chunk(chunk);
          }
        } finally {
          duckdb.destroy_result(res);
        }
      } finally {
        await duckdb.disconnect(con);
      }
    } finally {
      await duckdb.close(db);
    }
  });
  test('create and insert', async () => {
    const db = await duckdb.open();
    try {
      const con = await duckdb.connect(db);
      try {
        const res = await duckdb.query(con, 'create table test_create_and_insert(i integer)');
        try {
          expect(duckdb.result_statement_type(res)).toBe(duckdb.StatementType.CREATE);
          expect(duckdb.result_return_type(res)).toBe(duckdb.ResultType.NOTHING);
          expect(duckdb.rows_changed(res)).toBe(0);
          expect(duckdb.column_count(res)).toBe(1);
          expect(duckdb.column_name(res, 0)).toBe('Count');
          expect(duckdb.column_type(res, 0)).toBe(duckdb.Type.BIGINT);
          const chunk = await duckdb.fetch_chunk(res);
          try {
            expect(duckdb.data_chunk_get_column_count(chunk)).toBe(0);
            expect(duckdb.data_chunk_get_size(chunk)).toBe(0);
          } finally {
            duckdb.destroy_data_chunk(chunk);
          }
        } finally {
          duckdb.destroy_result(res);
        }
        const res2 = await duckdb.query(con, 'insert into test_create_and_insert from range(17)');
        try {
          expect(duckdb.result_statement_type(res2)).toBe(duckdb.StatementType.INSERT);
          expect(duckdb.result_return_type(res2)).toBe(duckdb.ResultType.CHANGED_ROWS);
          expect(duckdb.rows_changed(res2)).toBe(17);
          expect(duckdb.column_count(res2)).toBe(1);
          expect(duckdb.column_name(res2, 0)).toBe('Count');
          expect(duckdb.column_type(res2, 0)).toBe(duckdb.Type.BIGINT);
          const chunk = await duckdb.fetch_chunk(res2);
          try {
            expect(duckdb.data_chunk_get_column_count(chunk)).toBe(1);
            expect(duckdb.data_chunk_get_size(chunk)).toBe(1);
            const vector = duckdb.data_chunk_get_vector(chunk, 0);
            const data = duckdb.vector_get_data(vector, 8);
            const dv = new DataView(data.buffer);
            const validity_bytes = duckdb.vector_get_validity(vector, 8);
            const validity = new BigUint64Array(validity_bytes.buffer, 0, 1);
            expectValidity(validity_bytes, validity, 0, true);
            const value = dv.getBigInt64(0, true);
            expect(value).toBe(17n);
          } finally {
            duckdb.destroy_data_chunk(chunk);
          }
        } finally {
          duckdb.destroy_result(res2);
        }
      } finally {
        await duckdb.disconnect(con);
      }
    } finally {
      await duckdb.close(db);
    }
  });
});
