import { bench, describe } from 'vitest';
import { benchFn, benchOpts } from './util/benchUtils';

describe('types (list & array)', () => {
  bench('list[int]', benchFn('select [1] from range(1000000)'), benchOpts());
  bench(
    'list[varchar]',
    benchFn(`select ['a'] from range(1000000)`),
    benchOpts(),
  );
  bench(
    'array[int]',
    benchFn('select array_value(1) from range(1000000)'),
    benchOpts(),
  );
  bench(
    'array[varchar]',
    benchFn(`select array_value('a') from range(1000000)`),
    benchOpts(),
  );
});
