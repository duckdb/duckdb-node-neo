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
    const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
    try {
      const chunk = duckdb.create_data_chunk([int_type, varchar_type]);
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
      duckdb.destroy_logical_type(varchar_type);
    }
  });
});
