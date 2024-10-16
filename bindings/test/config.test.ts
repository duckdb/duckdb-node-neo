import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';

suite('config', () => {
  test('config_count', () => {
    expect(duckdb.config_count()).toBe(100);
  });
  test('get_config_flag', () => {
    expect(duckdb.get_config_flag(0).name).toBe('access_mode');
    expect(duckdb.get_config_flag(99).name).toBe('http_logging_output');
  });
  test('get_config_flag out of bounds', () => {
    expect(() => duckdb.get_config_flag(-1)).toThrowError(/^Config option not found$/);
  });
  test('create, set, and destroy', () => {
    const config = duckdb.create_config()
    expect(config).toBeTruthy();
    duckdb.set_config(config, 'custom_user_agent', 'my_user_agent');
    duckdb.destroy_config(config);
  });
});
