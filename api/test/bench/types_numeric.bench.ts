import { bench, describe } from 'vitest';
import { benchFn, benchOpts } from './util/benchUtils';

describe('types (numeric)', () => {
  bench(
    'tinyint',
    benchFn('select 1::tinyint from range(1000000)'),
    benchOpts(),
  );
  bench(
    'smallint',
    benchFn('select 1::smallint from range(1000000)'),
    benchOpts(),
  );
  bench(
    'integer',
    benchFn('select 1::integer from range(1000000)'),
    benchOpts(),
  );
  bench('bigint', benchFn('select 1::bigint from range(1000000)'), benchOpts());
  bench(
    'hugeint',
    benchFn('select 1::hugeint from range(1000000)'),
    benchOpts(),
  );

  bench(
    'utinyint',
    benchFn('select 1::utinyint from range(1000000)'),
    benchOpts(),
  );
  bench(
    'usmallint',
    benchFn('select 1::usmallint from range(1000000)'),
    benchOpts(),
  );
  bench(
    'uinteger',
    benchFn('select 1::uinteger from range(1000000)'),
    benchOpts(),
  );
  bench(
    'ubigint',
    benchFn('select 1::ubigint from range(1000000)'),
    benchOpts(),
  );
  bench(
    'uhugeint',
    benchFn('select 1::uhugeint from range(1000000)'),
    benchOpts(),
  );

  bench('float', benchFn('select 1::float from range(1000000)'), benchOpts());
  bench('double', benchFn('select 1::double from range(1000000)'), benchOpts());

  bench(
    'decimal (2 bytes)',
    benchFn('select 999.9::decimal(4,1) from range(1000000)'),
    benchOpts(),
  );
  bench(
    'decimal (4 bytes)',
    benchFn('select 99999.9999::decimal(9,4) from range(1000000)'),
    benchOpts(),
  );
  bench(
    'decimal (8 bytes)',
    benchFn('select 999999999999.999999::decimal(18,6) from range(1000000)'),
    benchOpts(),
  );
  bench(
    'decimal (16 bytes)',
    benchFn(
      'select 9999999999999999999999999999.9999999999::decimal(38,10) from range(1000000)',
    ),
    benchOpts(),
  );
});
