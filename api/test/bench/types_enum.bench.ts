import { bench, describe } from 'vitest';
import { benchFn, benchOpts } from './util/benchUtils';

describe('types (enum)', () => {
  bench('enum (small)', benchFn(`select 'a'::small_enum from range(1000000)`), benchOpts({
    additionalSetup: async (connection) => {
      await connection.run(`create type small_enum as enum ('a', 'b')`);
    },
  }));
  bench('enum (medium)', benchFn(`select 'enum_0'::medium_enum from range(1000000)`), benchOpts({
    additionalSetup: async (connection) => {
      await connection.run(`create type medium_enum as enum (select 'enum_' || i from range(300) t(i))`);
    }
  }));
  bench.skip('enum (large)', benchFn(`select 'enum_0'::large_enum from range(1000000)`), benchOpts({
    additionalSetup: async (connection) => {
      await connection.run(`create type large_enum as enum (select 'enum_' || i from range(70000) t(i))`);
    }
  }));
});
