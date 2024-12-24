import { bench, describe } from "vitest";
import {
  DuckDBConnection,
  DuckDBInstance,
  DuckDBIntervalValue,
  DuckDBPreparedStatement,
} from "../../src";

let instance: DuckDBInstance;
let connection: DuckDBConnection;
let prepared: DuckDBPreparedStatement;

const TOTAL_SIZE = 1_000_000n;
const SELECTION_SIZE = 1_000n;

async function setup() {
  instance = await DuckDBInstance.create();
  connection = await instance.connect();

  const tableSetupQuery = await connection.prepare(`
CREATE OR REPLACE TABLE test AS
SELECT 
    TIMESTAMP '2025-01-01' + seq::BIGINT * INTERVAL 1 MILLISECOND AS timestamp,
    RANDOM() * 1_000_000 AS value
FROM 
    range($1) AS seq(seq)
  `);

  tableSetupQuery.bindBigInt(1, TOTAL_SIZE);

  await tableSetupQuery.run();
}

/**
 * Randomly generate a BigInt amount of milliseconds to use as the query start point, allowing for
 * SELECTION_SIZE worth of data
 */
function startMS() {
  return BigInt(
    Math.floor(Math.random() * Number(TOTAL_SIZE - SELECTION_SIZE))
  );
}

describe(`Parameterised queries`, () => {
  const factory = (start: string, end: string) => `SELECT *
            FROM test
            WHERE timestamp BETWEEN TIMESTAMP '2025-01-01' + ${start}
                                AND TIMESTAMP '2025-01-01' + ${end};`;

  bench(
    `Multiple queries`,
    async () => {
      const s = startMS();
      const e = s + SELECTION_SIZE;
      const startInterval = `INTERVAL ${s} MILLISECONDS`;
      const endInterval = `INTERVAL ${e} MILLISECONDS`;

      const query = factory(startInterval, endInterval);

      await connection.runAndReadAll(query);
    },
    {
      setup,
    }
  );

  bench(
    `Prepared with re-use`,
    async () => {
      const startInterval = startMS();
      const endInterval = startInterval + SELECTION_SIZE;

      prepared.bindInterval(
        1,
        new DuckDBIntervalValue(0, 0, startInterval * 1000n)
      );
      prepared.bindInterval(
        2,
        new DuckDBIntervalValue(0, 0, endInterval * 1000n)
      );

      await prepared.runAndReadAll();
    },
    {
      setup: async () => {
        await setup();

        const query = factory("$1", "$2");
        prepared = await connection.prepare(query);
      },
    }
  );

  bench(
    `Prepared each time`,
    async () => {
      const startInterval = startMS();
      const endInterval = startInterval + SELECTION_SIZE;

      const query = factory("$1", "$2");
      prepared = await connection.prepare(query);

      prepared.bindInterval(
        1,
        new DuckDBIntervalValue(0, 0, startInterval * 1000n)
      );
      prepared.bindInterval(
        2,
        new DuckDBIntervalValue(0, 0, endInterval * 1000n)
      );

      await prepared.runAndReadAll();
    },
    {
      setup,
    }
  );
});
