import { DuckDBConnection, DuckDBInstance } from '../../../src';
import { runSql } from './runSql';

let instance: DuckDBInstance;
let connection: DuckDBConnection;

async function setupConnection() {
  instance = await DuckDBInstance.create();
  connection = await instance.connect();
}

export function benchFn(sql: string) {
  return () => runSql(connection, sql);
}

export function benchOpts(options?: {
  additionalSetup?: (connection: DuckDBConnection) => Promise<void>;
}) {
  const additionalSetup = options?.additionalSetup;
  const setup = additionalSetup
    ? async () => {
        await setupConnection();
        await additionalSetup(connection);
      }
    : setupConnection;
  return { setup };
}
