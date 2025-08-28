import { bench, describe } from 'vitest';
import { benchFn, benchOpts } from './util/benchUtils';

describe('types (bit)', () => {
  bench(
    'bit (small)',
    benchFn(`select '101010'::bit from range(1000000)`),
    benchOpts(),
  );
  bench(
    'bit (short)',
    benchFn(`select bitstring('0101011', 11 * 8) from range(1000000)`),
    benchOpts(),
  );
  bench(
    'bit (short + 1 = smallest long)',
    benchFn(`select bitstring('0101011', 11 * 8 + 1) from range(1000000)`),
    benchOpts(),
  );
  bench(
    'bit (long)',
    benchFn(`select bitstring('0101011', 11 * 8 + 12 * 8) from range(1000000)`),
    benchOpts(),
  );
});
