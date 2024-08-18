import { bench, describe } from 'vitest';
import { benchFn, benchOpts } from './util/benchUtils';

describe('types (struct & map)', () => {
  bench('struct[int]', benchFn('select {a:1} from range(1000000)'), benchOpts());
  bench('struct[varchar]', benchFn(`select {a:'a'} from range(1000000)`), benchOpts());
  bench('map[int,int]', benchFn('select map {1:1} from range(1000000)'), benchOpts());
  bench('map[varchar,varchar]', benchFn(`select map {'a':'a'} from range(1000000)`), benchOpts());
});
