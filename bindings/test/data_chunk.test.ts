import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { expectLogicalType } from './utils/expectLogicalType';
import { INTEGER, VARCHAR } from './utils/expectedLogicalTypes';

suite('data chunk', () => {
  test('create', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
    const chunk = duckdb.create_data_chunk([int_type, varchar_type]);
    expect(duckdb.data_chunk_get_column_count(chunk)).toBe(2);
    const vec0 = duckdb.data_chunk_get_vector(chunk, 0);
    expectLogicalType(duckdb.vector_get_column_type(vec0), INTEGER);
    const vec1 = duckdb.data_chunk_get_vector(chunk, 1);
    expectLogicalType(duckdb.vector_get_column_type(vec1), VARCHAR);
  });
  test('change size', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    const chunk = duckdb.create_data_chunk([int_type]);
    expect(duckdb.data_chunk_get_size(chunk)).toBe(0);
    duckdb.data_chunk_set_size(chunk, 42);
    expect(duckdb.data_chunk_get_size(chunk)).toBe(42);
    duckdb.data_chunk_reset(chunk);
    expect(duckdb.data_chunk_get_size(chunk)).toBe(0);
  });
  test('write vector validity bit-by-bit', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    const chunk = duckdb.create_data_chunk([int_type]);
    duckdb.data_chunk_set_size(chunk, 3);
    const vector = duckdb.data_chunk_get_vector(chunk, 0);
    duckdb.vector_ensure_validity_writable(vector);
    const validity = duckdb.vector_get_validity(vector, 8);
    expect(validity[0]).toBe(0b11111111);
    duckdb.validity_set_row_validity(validity, 1, false);
    expect(validity[0]).toBe(0b11111101);
    duckdb.validity_set_row_valid(validity, 1);
    expect(validity[0]).toBe(0b11111111);
    duckdb.validity_set_row_invalid(validity, 2);
    expect(validity[0]).toBe(0b11111011);
  });
  test('write vector validity bulk', () => {
    const source_buffer = new ArrayBuffer(8);
    const source_array = new BigUint64Array(source_buffer);
    source_array[0] = 0xfedcba9876543210n;

    const integer_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    const chunk = duckdb.create_data_chunk([integer_type]);
    duckdb.data_chunk_set_size(chunk, 3);
    const vector = duckdb.data_chunk_get_vector(chunk, 0);
    duckdb.vector_ensure_validity_writable(vector);
    duckdb.copy_data_to_vector_validity(vector, 0, source_buffer, 0, source_buffer.byteLength);

    const validity_bytes = duckdb.vector_get_validity(vector, 8);
    const validity_array = new BigUint64Array(validity_bytes.buffer, validity_bytes.byteOffset, 1);
    expect(validity_array[0]).toBe(0xfedcba9876543210n);
  });
  test('write integer vector', () => {
    const source_buffer = new ArrayBuffer(3 * 4);
    const source_dv = new DataView(source_buffer);
    source_dv.setInt32(0, 42, true);
    source_dv.setInt32(4, 12345, true);
    source_dv.setInt32(8, 67890, true);

    const integer_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    const chunk = duckdb.create_data_chunk([integer_type]);
    duckdb.data_chunk_set_size(chunk, 3);
    const vector = duckdb.data_chunk_get_vector(chunk, 0);
    duckdb.copy_data_to_vector(vector, 0, source_buffer, 0, source_buffer.byteLength);

    const vector_data = duckdb.vector_get_data(vector, 3 * 4);
    const vector_dv = new DataView(vector_data.buffer, vector_data.byteOffset, vector_data.byteLength);
    expect(vector_dv.getInt32(0, true)).toBe(42);
    expect(vector_dv.getInt32(4, true)).toBe(12345);
    expect(vector_dv.getInt32(8, true)).toBe(67890);
  });
  test('write string vector', () => {
    const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
    const chunk = duckdb.create_data_chunk([varchar_type]);
    duckdb.data_chunk_set_size(chunk, 3);
    const vector = duckdb.data_chunk_get_vector(chunk, 0);
    duckdb.vector_assign_string_element(vector, 0, 'ABC');
    duckdb.vector_assign_string_element(vector, 1, 'abcdefghijkl');
    duckdb.vector_assign_string_element(vector, 2, 'longer than twelve characters');
    const data = duckdb.vector_get_data(vector, 3 * 16);
    const dv = new DataView(data.buffer);
    expect(dv.getUint32(0, true)).toBe(3);
    expect([data[4], data[5], data[6]]).toStrictEqual([0x41, 0x42, 0x43]); // A, B, C
    expect(dv.getUint32(16, true)).toBe(12);
    expect([data[20], data[31]]).toStrictEqual([0x61, 0x6c]); // a, l
    expect(dv.getUint32(32, true)).toBe('longer than twelve characters'.length);
    expect([data[36], data[37], data[38], data[39]]).toStrictEqual([0x6c, 0x6f, 0x6e, 0x67]); // l, o, n, g
  });
  test('write blob vector', () => {
    const blob_type = duckdb.create_logical_type(duckdb.Type.BLOB);
    const chunk = duckdb.create_data_chunk([blob_type]);
    duckdb.data_chunk_set_size(chunk, 3);
    const vector = duckdb.data_chunk_get_vector(chunk, 0);
    duckdb.vector_assign_string_element_len(vector, 0, new Uint8Array([0xAB, 0xCD, 0xEF]));
    duckdb.vector_assign_string_element_len(vector, 1,
      new Uint8Array([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C]));
    duckdb.vector_assign_string_element_len(vector, 2,
      new Uint8Array([0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D])
    );
    const data = duckdb.vector_get_data(vector, 3 * 16);
    const dv = new DataView(data.buffer);
    expect(dv.getUint32(0, true)).toBe(3);
    expect([data[4], data[5], data[6]]).toStrictEqual([0xAB, 0xCD, 0xEF]);
    expect(dv.getUint32(16, true)).toBe(12);
    expect([data[20], data[31]]).toStrictEqual([0x01, 0x0C]);
    expect(dv.getUint32(32, true)).toBe(13);
    expect([data[36], data[37], data[38], data[39]]).toStrictEqual([0x11, 0x12, 0x13, 0x14]);
  });
  test('set list vector size', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    const list_type = duckdb.create_list_type(int_type);
    const chunk = duckdb.create_data_chunk([list_type]);
    duckdb.data_chunk_set_size(chunk, 3);
    const vector = duckdb.data_chunk_get_vector(chunk, 0);
    duckdb.list_vector_reserve(vector, 7); // can't easily verify that this worked
    duckdb.list_vector_set_size(vector, 5);
    expect(duckdb.list_vector_get_size(vector)).toBe(5);
  });
});
