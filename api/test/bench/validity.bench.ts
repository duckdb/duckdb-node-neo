import { bench, describe } from 'vitest';
import { benchFn, benchOpts } from './util/benchUtils';

describe('validity', () => {
  bench(
    'odds null',
    benchFn(
      'SELECT CASE WHEN range % 2 = 0 THEN range ELSE NULL END asdf FROM range(1000000)',
    ),
    benchOpts(),
  );
});
