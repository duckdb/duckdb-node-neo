import duckdb from 'duckdb';
import { expect, suite, test } from 'vitest';

suite('config', () => {
  test('config_count', () => {
    expect(duckdb.config_count()).toBe(78);
  });
  test('get_config_flag', () => {
    expect(duckdb.get_config_flag(0).name).toBe('access_mode');
    expect(duckdb.get_config_flag(77).name).toBe('http_logging_output');
  });
  test('get_config_flag out of bounds', () => {
    expect(() => duckdb.get_config_flag(-1)).toThrowError(/^Config option not found$/);
  });
});
