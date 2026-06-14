import { assert, beforeAll, describe, test } from 'vitest';
import {
  quotedIdentifier,
  quotedString,
} from '../src';
import {
  setDefaultTimezone,
} from './util/testHelpers';

describe('SQL utility functions', () => {
  beforeAll(setDefaultTimezone);

  test('quotedString', () => {
    // Basic string
    assert.equal(quotedString('hello'), "'hello'");

    // String with single quotes
    assert.equal(quotedString("it's"), "'it''s'");
    assert.equal(quotedString("'quoted'"), "'''quoted'''");

    // String with multiple single quotes
    assert.equal(
      quotedString("it's 'really' good"),
      "'it''s ''really'' good'",
    );

    // Empty string
    assert.equal(quotedString(''), "''");

    // String with special characters
    assert.equal(quotedString('hello\nworld'), "'hello\nworld'");
    assert.equal(quotedString('tab\there'), "'tab\there'");
  });

  test('quotedIdentifier', () => {
    // Basic identifier
    assert.equal(quotedIdentifier('table_name'), '"table_name"');

    // Identifier with double quotes
    assert.equal(quotedIdentifier('my"table'), '"my""table"');
    assert.equal(quotedIdentifier('"column"'), '"""column"""');

    // Identifier with multiple double quotes
    assert.equal(
      quotedIdentifier('my"special"table'),
      '"my""special""table"',
    );

    // Empty identifier
    assert.equal(quotedIdentifier(''), '""');

    // Identifier with special characters
    assert.equal(quotedIdentifier('table-name'), '"table-name"');
    assert.equal(quotedIdentifier('table.name'), '"table.name"');
    assert.equal(quotedIdentifier('table name'), '"table name"');
  });
});
