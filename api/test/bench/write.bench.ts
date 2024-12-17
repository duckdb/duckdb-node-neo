import { bench, describe } from "vitest";
import {
  DuckDBConnection,
  DuckDBInstance,
  DuckDBTimestampValue,
} from "../../src";

let instance: DuckDBInstance;
let connection: DuckDBConnection;

async function setup() {
  instance = await DuckDBInstance.create();
  connection = await instance.connect();

  await connection.run(`
    CREATE OR REPLACE TABLE test (
      timestamp TIMESTAMPTZ NOT NULL,
      value FLOAT NOT NULL,
    );
  `);
}

for (const batchSize of [1, 1000]) {
  describe(`batch write of size ${batchSize}`, () => {
    bench(
      `${batchSize} insert bind`,
      async () => {
        const query = await connection.prepare(
          "INSERT INTO test (timestamp, value) VALUES ($1, $2);"
        );

        for (let index = 0; index < batchSize; index++) {
          query.bindTimestamp(
            1,
            new DuckDBTimestampValue(BigInt(Date.now()) * 1000n)
          );
          query.bindFloat(2, Math.random() * 1_000_000);

          await query.run();
        }
      },
      {
        setup,
      }
    );
    bench(
      `${batchSize} row append`,
      async () => {
        const appender = await connection.createAppender("main", "test");

        for (let index = 0; index < batchSize; index++) {
          appender.appendTimestamp(
            new DuckDBTimestampValue(BigInt(Date.now()) * 1000n)
          );
          appender.appendFloat(Math.random() * 1_000_000);
          appender.endRow();
        }
        appender.close();
      },
      {
        setup,
      }
    );
  });
}
