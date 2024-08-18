import { bench, describe } from 'vitest';
import { benchFn, benchOpts } from './util/benchUtils';

describe('types (bool)', () => {
  bench('bool', benchFn('select true from range(1000000)'), benchOpts());
});

describe('types (uuid)', () => {
  bench('uuid', benchFn('select uuid() from range(1000000)'), benchOpts());
});

describe('types (union)', () => {
  bench('union', benchFn(`select union_value(t := 'a') from range(1000000)`), benchOpts());
});
