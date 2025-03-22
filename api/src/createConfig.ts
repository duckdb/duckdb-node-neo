import duckdb from '@duckdb/node-bindings';

export function createConfig(options?: Record<string, string>): duckdb.Config {
  const config = duckdb.create_config();
  // Set the default duckdb_api value for the api. Can be overridden.
  duckdb.set_config(config, 'duckdb_api', 'node-neo-api');
  if (options) {
    for (const optionName in options) {
      const optionValue = String(options[optionName]);
      duckdb.set_config(config, optionName, optionValue);
    }
  }
  return config;
}
