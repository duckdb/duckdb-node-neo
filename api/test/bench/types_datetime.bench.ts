import { bench, describe } from 'vitest';
import { benchFn, benchOpts } from './util/benchUtils';

describe('types (date)', () => {
  bench('date', benchFn(`select '2123-12-31'::date from range(1000000)`), benchOpts());
});

describe('types (time)', () => {
  bench('time', benchFn(`select '12:34:56.789123'::time from range(1000000)`), benchOpts());
  bench('timetz', benchFn(`select '12:34:56-15:59:59'::timetz from range(1000000)`), benchOpts());
});

describe('types (timestamp)', () => {
  bench('timestamp', benchFn(`select '2123-12-31 12:34:56.789123'::timestamp from range(1000000)`), benchOpts());
  bench('timestamp_s', benchFn(`select '2123-12-31 12:34:56'::timestamp_s from range(1000000)`), benchOpts());
  bench('timestamp_ms', benchFn(`select '2123-12-31 12:34:56.789'::timestamp_ms from range(1000000)`), benchOpts());
  bench('timestamp_ns', benchFn(`select '2123-12-31 12:34:56.789123'::timestamp_ns from range(1000000)`), benchOpts());
  bench('timestamptz', benchFn(`select '2123-12-31 12:34:56.789123'::timestamptz from range(1000000)`), benchOpts());
});

describe('types (interval)', () => {
  bench('interval', benchFn('select interval 1 minute from range(1000000)'), benchOpts());
});
