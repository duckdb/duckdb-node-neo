import { assert, beforeAll, describe, test } from 'vitest';
import {
  configurationOptionDescriptions,
  version,
} from '../src';
import {
  setDefaultTimezone,
} from './util/testHelpers';

describe('metadata', () => {
  beforeAll(setDefaultTimezone);

  test('should expose version', () => {
    const ver = version();
    assert.ok(ver.startsWith('v'), `version starts with 'v'`);
  });
  test('should expose configuration option descriptions', () => {
    const descriptions = configurationOptionDescriptions();
    assert.ok(descriptions['memory_limit'], `descriptions has 'memory_limit'`);
  });
});
