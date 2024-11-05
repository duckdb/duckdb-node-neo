import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';

suite('constants', () => {
  test('sizeof_bool', () => {
    expect(duckdb.sizeof_bool).toBe(1);
  });
  test('library_version', () => {
    expect(duckdb.library_version()).toBe('v1.1.3');
  });
  test('vector_size', () => {
    expect(duckdb.vector_size()).toBe(2048);
  });
});
