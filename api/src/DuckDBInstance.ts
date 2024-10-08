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
    if (options) {
      const config = duckdb.create_config();
      try {
        for (const optionName in options) {
          const optionValue = options[optionName];
          duckdb.set_config(config, optionName, optionValue);
        }
        return new DuckDBInstance(await duckdb.open(path, config));
      } finally {
        duckdb.destroy_config(config);
      }
    } else {
      return new DuckDBInstance(await duckdb.open(path));
    }
  }
  public dispose(): Promise<void> {
    return duckdb.close(this.db);
  }
  public async connect(): Promise<DuckDBConnection> {
    return new DuckDBConnection(await duckdb.connect(this.db));
  }
}
