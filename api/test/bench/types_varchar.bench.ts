import { bench, describe } from 'vitest';
import { benchFn, benchOpts } from './util/benchUtils';

describe('types (varchar & blob)', () => {
  bench(
    'varchar (short)',
    benchFn(`select 'abcdefghijkl' from range(1000000)`),
    benchOpts(),
  );
  bench(
    'varchar (long)',
    benchFn(`select 'abcdefghijklmnopqrstuvwx' from range(1000000)`),
    benchOpts(),
  );
  bench(
    'blob (short)',
    benchFn(`select 'abcdefghijkl'::blob from range(1000000)`),
    benchOpts(),
  );
  bench(
    'blob (long)',
    benchFn(`select 'abcdefghijklmnopqrstuvwx'::blob from range(1000000)`),
    benchOpts(),
  );
});
