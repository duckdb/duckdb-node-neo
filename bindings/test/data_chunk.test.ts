import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { expectLogicalType } from './utils/expectLogicalType';
import { INTEGER, VARCHAR } from './utils/expectedLogicalTypes';

suite('data chunk', () => {
  test('create', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
    try {
      const chunk = duckdb.create_data_chunk([int_type, varchar_type]);
      try {
        expect(duckdb.data_chunk_get_column_count(chunk)).toBe(2);
        const vec0 = duckdb.data_chunk_get_vector(chunk, 0);
        expectLogicalType(duckdb.vector_get_column_type(vec0), INTEGER);
        const vec1 = duckdb.data_chunk_get_vector(chunk, 1);
        expectLogicalType(duckdb.vector_get_column_type(vec1), VARCHAR);
      } finally {
        duckdb.destroy_data_chunk(chunk);
      }
    } finally {
      duckdb.destroy_logical_type(int_type);
      duckdb.destroy_logical_type(varchar_type);
    }
  });
  test('change size', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    try {
      const chunk = duckdb.create_data_chunk([int_type]);
      try {
        expect(duckdb.data_chunk_get_size(chunk)).toBe(0);
        duckdb.data_chunk_set_size(chunk, 42);
        expect(duckdb.data_chunk_get_size(chunk)).toBe(42);
        duckdb.data_chunk_reset(chunk);
        expect(duckdb.data_chunk_get_size(chunk)).toBe(0);
      } finally {
        duckdb.destroy_data_chunk(chunk);
      }
    } finally {
      duckdb.destroy_logical_type(int_type);
    }
  });
  test('write vector validity', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    try {
      const chunk = duckdb.create_data_chunk([int_type]);
      try {
        duckdb.data_chunk_set_size(chunk, 3);
        const vector = duckdb.data_chunk_get_vector(chunk, 0);
        duckdb.vector_ensure_validity_writable(vector);
        // const data = duckdb.vector_get_data(vector, 3 * 4);
        const validity = duckdb.vector_get_validity(vector, 8);
        expect(validity[0]).toBe(0b11111111);
        duckdb.validity_set_row_validity(validity, 1, false);
        expect(validity[0]).toBe(0b11111101);
        duckdb.validity_set_row_valid(validity, 1);
        expect(validity[0]).toBe(0b11111111);
        duckdb.validity_set_row_invalid(validity, 2);
        expect(validity[0]).toBe(0b11111011);
      } finally {
        duckdb.destroy_data_chunk(chunk);
      }
    } finally {
      duckdb.destroy_logical_type(int_type);
    }
  });
  test('write string vector', () => {
    const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
    try {
      const chunk = duckdb.create_data_chunk([varchar_type]);
      try {
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
      } finally {
        duckdb.destroy_data_chunk(chunk);
      }
    } finally {
      duckdb.destroy_logical_type(varchar_type);
    }
  });
  test('set list vector size', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    try {
      const list_type = duckdb.create_list_type(int_type);
      try {
        const chunk = duckdb.create_data_chunk([list_type]);
        try {
          duckdb.data_chunk_set_size(chunk, 3);
          const vector = duckdb.data_chunk_get_vector(chunk, 0);
          duckdb.list_vector_reserve(vector, 7); // can't easily verify that this worked
          duckdb.list_vector_set_size(vector, 5);
          expect(duckdb.list_vector_get_size(vector)).toBe(5);
        } finally {
          duckdb.destroy_data_chunk(chunk);
        }
      } finally {
        duckdb.destroy_logical_type(list_type);
      }
    } finally {
      duckdb.destroy_logical_type(int_type);
    }
  });
});
