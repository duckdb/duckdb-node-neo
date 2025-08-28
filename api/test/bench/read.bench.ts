import { bench, describe } from 'vitest';
import {
  DuckDBConnection,
  DuckDBInstance,
  DuckDBPendingResultState,
} from '../../src';

let instance: DuckDBInstance;
let connection: DuckDBConnection;

const TOTAL_SIZE = 1_000_000n;
const SELECTION_SIZE = 100_000n;

async function setup() {
  instance = await DuckDBInstance.create();
  connection = await instance.connect();

  const prepared = await connection.prepare(`
CREATE OR REPLACE TABLE test AS
SELECT 
    TIMESTAMP '2025-01-01' + seq::BIGINT * INTERVAL 1 MILLISECOND AS timestamp,
    RANDOM() * 1_000_000 AS value
FROM 
    range($1) AS seq(seq)
  `);

  prepared.bindBigInt(1, TOTAL_SIZE);

  await prepared.run();
}

type Example = {
  name: string;
  factory: (start: string, end: string) => string;
};

const examples: Example[] = [
  {
    name: 'Row Fetching',
    factory: (start, end) =>
      `SELECT *
       FROM test
       WHERE timestamp BETWEEN TIMESTAMP '2025-01-01' + ${start}
                           AND TIMESTAMP '2025-01-01' + ${end};`,
  },
  {
    name: 'Overall Aggregates',
    factory: (start, end) =>
      `SELECT mean("value"), min("value"), max("value")
       FROM test
       WHERE timestamp BETWEEN TIMESTAMP '2025-01-01' + ${start}
                           AND TIMESTAMP '2025-01-01' + ${end};`,
  },
  {
    name: 'Rolling Aggregates',
    factory: (start, end) =>
      `SELECT mean("value") OVER previous_second, 
       min("value") OVER previous_second, 
       max("value") OVER previous_second
       FROM test
       WHERE timestamp BETWEEN TIMESTAMP '2025-01-01' + ${start}
                           AND TIMESTAMP '2025-01-01' + ${end}
       WINDOW previous_second AS (
           ORDER BY "timestamp" ASC
           RANGE BETWEEN INTERVAL 1_000 MILLISECONDS PRECEDING
                     AND INTERVAL 0 MILLISECONDS FOLLOWING);`,
  },
];

function queryFactory(example: Example) {
  const s = BigInt(
    Math.floor(Math.random() * Number(TOTAL_SIZE - SELECTION_SIZE)),
  );
  const e = s + SELECTION_SIZE;
  const startInterval = `INTERVAL ${s} MILLISECONDS`;
  const endInterval = `INTERVAL ${e} MILLISECONDS`;

  const query = example.factory(startInterval, endInterval);

  return query;
}

for (const full of [false, true]) {
  for (const example of examples) {
    describe(`${example.name} - ${
      full ? 'Full Result' : 'Time to First Row'
    }`, () => {
      bench(
        `${example.name} - ${
          full ? 'runAndReadAll()' : 'runAndReadUntil(q, 1)'
        }`,
        async () => {
          const query = queryFactory(example);

          if (full) {
            await connection.runAndReadAll(query);
          } else {
            await connection.runAndReadUntil(query, 1);
          }
        },
        {
          setup,
          iterations: 20,
        },
      );
      bench(
        `${example.name} - ${
          full
            ? 'start runTask pending.readAll()'
            : 'start runTask pending.readUntil(1)'
        }`,
        async () => {
          const query = queryFactory(example);
          const pending = await connection.start(query);

          while (pending.runTask() !== DuckDBPendingResultState.RESULT_READY) {
            // Yield for minimal time
            await Promise.resolve();
          }

          if (full) {
            await pending.readAll();
          } else {
            await pending.readUntil(1);
          }
        },
        {
          setup,
          iterations: 20,
        },
      );
      bench(
        `${example.name} - ${
          full
            ? 'start runTask fetchChunks loop'
            : 'start runTask single fetchChunk'
        }`,
        async () => {
          const query = queryFactory(example);
          const pending = await connection.start(query);

          while (pending.runTask() !== DuckDBPendingResultState.RESULT_READY) {
            // Yield for minimal time
            await Promise.resolve();
          }

          const result = await pending.getResult();

          if (full) {
            while (true) {
              const chunk = await result.fetchChunk();
              // Last chunk will have zero rows.
              if (!chunk || chunk.rowCount === 0) {
                break;
              }
            }
          } else {
            await result.fetchChunk();
          }
        },
        {
          setup,
          iterations: 20,
        },
      );

      bench(
        `${example.name} - ${
          full ? 'streamAndReadAll()' : 'streamAndReadUntil(q, 1)'
        }`,
        async () => {
          const query = queryFactory(example);

          if (full) {
            await connection.streamAndReadAll(query);
          } else {
            await connection.streamAndReadUntil(query, 1);
          }
        },
        {
          setup,
          iterations: 20,
        },
      );
      bench(
        `${example.name} - ${
          full
            ? 'startStream runTask pending.readAll()'
            : 'startStream runTask pending.readUntil(1)'
        }`,
        async () => {
          const query = queryFactory(example);
          const pending = await connection.startStream(query);

          while (pending.runTask() !== DuckDBPendingResultState.RESULT_READY) {
            // Yield for minimal time
            await Promise.resolve();
          }

          if (full) {
            await pending.readAll();
          } else {
            await pending.readUntil(1);
          }
        },
        {
          setup,
          iterations: 20,
        },
      );
      bench(
        `${example.name} - ${
          full
            ? 'startStream runTask fetchChunks loop'
            : 'startStream runTask single fetchChunk'
        }`,
        async () => {
          const query = queryFactory(example);
          const pending = await connection.startStream(query);

          while (pending.runTask() !== DuckDBPendingResultState.RESULT_READY) {
            // Yield for minimal time
            await Promise.resolve();
          }

          const result = await pending.getResult();

          if (full) {
            while (true) {
              const chunk = await result.fetchChunk();
              // Last chunk will have zero rows.
              if (!chunk || chunk.rowCount === 0) {
                break;
              }
            }
          } else {
            await result.fetchChunk();
          }
        },
        {
          setup,
          iterations: 20,
        },
      );
      bench(
        `${example.name} - ${
          full ? 'run fetchChunks loop' : 'run single fetchChunk'
        }`,
        async () => {
          const query = queryFactory(example);
          const result = await connection.run(query);

          if (full) {
            while (true) {
              const chunk = await result.fetchChunk();
              // Last chunk will have zero rows.
              if (!chunk || chunk.rowCount === 0) {
                break;
              }
            }
          } else {
            // Just fetch one chunk
            await result.fetchChunk();
          }
        },
        {
          setup,
          iterations: 20,
        },
      );
      bench(
        `${example.name} - ${
          full ? 'stream fetchChunks loop' : 'stream single fetchChunk'
        }`,
        async () => {
          const query = queryFactory(example);
          const result = await connection.stream(query);

          if (full) {
            while (true) {
              const chunk = await result.fetchChunk();
              // Last chunk will have zero rows.
              if (!chunk || chunk.rowCount === 0) {
                break;
              }
            }
          } else {
            // Just fetch one chunk
            await result.fetchChunk();
          }
        },
        {
          setup,
          iterations: 20,
        },
      );
    });
  }
}
