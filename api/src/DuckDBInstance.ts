import duckdb from '@duckdb/node-bindings';
import { DuckDBConnection } from './DuckDBConnection';

export class DuckDBInstance {
  private readonly db: duckdb.Database;
  constructor(db: duckdb.Database) {
    this.db = db;
  }
  public static async create(
    path?: string,
    options?: Record<string, string>
  ): Promise<DuckDBInstance> {
    const config = duckdb.create_config();
    // Set the default duckdb_api value for the api. Can be overridden.
    duckdb.set_config(config, 'duckdb_api', 'node-neo-api');
    if (options) {
      for (const optionName in options) {
        const optionValue = String(options[optionName]);
        duckdb.set_config(config, optionName, optionValue);
      }
    }
    return new DuckDBInstance(await duckdb.open(path, config));
  }
  public async connect(): Promise<DuckDBConnection> {
    return new DuckDBConnection(await duckdb.connect(this.db));
  }
}
