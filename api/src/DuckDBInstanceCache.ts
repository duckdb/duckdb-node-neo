import duckdb from '@duckdb/node-bindings';
import { DuckDBInstance } from './DuckDBInstance';
import { createConfig } from './createConfig';

export class DuckDBInstanceCache {
  private readonly cache: duckdb.InstanceCache;

  constructor() {
    this.cache = duckdb.create_instance_cache();
  }

  public async getOrCreateInstance(
    path?: string,
    options?: Record<string, string>
  ): Promise<DuckDBInstance> {
    const config = createConfig(options);
    const db = await duckdb.get_or_create_from_cache(this.cache, path, config);
    return new DuckDBInstance(db);
  }

  private static singletonInstance: DuckDBInstanceCache;

  public static get singleton(): DuckDBInstanceCache {
    if (!DuckDBInstanceCache.singletonInstance) {
      DuckDBInstanceCache.singletonInstance = new DuckDBInstanceCache();
    }
    return DuckDBInstanceCache.singletonInstance;
  }
}
