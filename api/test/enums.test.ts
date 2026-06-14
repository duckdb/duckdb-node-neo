import { assert, beforeAll, describe, test } from 'vitest';
import {
  ResultReturnType,
  StatementType,
} from '../src';
import {
  setDefaultTimezone,
} from './util/testHelpers';

describe('enums', () => {
  beforeAll(setDefaultTimezone);

  test('ReturnResultType enum', () => {
    assert.equal(ResultReturnType.INVALID, 0);
    assert.equal(ResultReturnType.CHANGED_ROWS, 1);
    assert.equal(ResultReturnType.NOTHING, 2);
    assert.equal(ResultReturnType.QUERY_RESULT, 3);

    assert.equal(ResultReturnType[ResultReturnType.INVALID], 'INVALID');
    assert.equal(
      ResultReturnType[ResultReturnType.CHANGED_ROWS],
      'CHANGED_ROWS',
    );
    assert.equal(ResultReturnType[ResultReturnType.NOTHING], 'NOTHING');
    assert.equal(
      ResultReturnType[ResultReturnType.QUERY_RESULT],
      'QUERY_RESULT',
    );
  });
  test('StatementType enum', () => {
    assert.equal(StatementType.INVALID, 0);
    assert.equal(StatementType.SELECT, 1);
    assert.equal(StatementType.INSERT, 2);
    assert.equal(StatementType.UPDATE, 3);
    assert.equal(StatementType.EXPLAIN, 4);
    assert.equal(StatementType.DELETE, 5);
    assert.equal(StatementType.PREPARE, 6);
    assert.equal(StatementType.CREATE, 7);
    assert.equal(StatementType.EXECUTE, 8);
    assert.equal(StatementType.ALTER, 9);
    assert.equal(StatementType.TRANSACTION, 10);
    assert.equal(StatementType.COPY, 11);
    assert.equal(StatementType.ANALYZE, 12);
    assert.equal(StatementType.VARIABLE_SET, 13);
    assert.equal(StatementType.CREATE_FUNC, 14);
    assert.equal(StatementType.DROP, 15);
    assert.equal(StatementType.EXPORT, 16);
    assert.equal(StatementType.PRAGMA, 17);
    assert.equal(StatementType.VACUUM, 18);
    assert.equal(StatementType.CALL, 19);
    assert.equal(StatementType.SET, 20);
    assert.equal(StatementType.LOAD, 21);
    assert.equal(StatementType.RELATION, 22);
    assert.equal(StatementType.EXTENSION, 23);
    assert.equal(StatementType.LOGICAL_PLAN, 24);
    assert.equal(StatementType.ATTACH, 25);
    assert.equal(StatementType.DETACH, 26);
    assert.equal(StatementType.MULTI, 27);

    assert.equal(StatementType[StatementType.INVALID], 'INVALID');
    assert.equal(StatementType[StatementType.SELECT], 'SELECT');
    assert.equal(StatementType[StatementType.INSERT], 'INSERT');
    assert.equal(StatementType[StatementType.UPDATE], 'UPDATE');
    assert.equal(StatementType[StatementType.EXPLAIN], 'EXPLAIN');
    assert.equal(StatementType[StatementType.DELETE], 'DELETE');
    assert.equal(StatementType[StatementType.PREPARE], 'PREPARE');
    assert.equal(StatementType[StatementType.CREATE], 'CREATE');
    assert.equal(StatementType[StatementType.EXECUTE], 'EXECUTE');
    assert.equal(StatementType[StatementType.ALTER], 'ALTER');
    assert.equal(StatementType[StatementType.TRANSACTION], 'TRANSACTION');
    assert.equal(StatementType[StatementType.COPY], 'COPY');
    assert.equal(StatementType[StatementType.ANALYZE], 'ANALYZE');
    assert.equal(StatementType[StatementType.VARIABLE_SET], 'VARIABLE_SET');
    assert.equal(StatementType[StatementType.CREATE_FUNC], 'CREATE_FUNC');
    assert.equal(StatementType[StatementType.DROP], 'DROP');
    assert.equal(StatementType[StatementType.EXPORT], 'EXPORT');
    assert.equal(StatementType[StatementType.PRAGMA], 'PRAGMA');
    assert.equal(StatementType[StatementType.VACUUM], 'VACUUM');
    assert.equal(StatementType[StatementType.CALL], 'CALL');
    assert.equal(StatementType[StatementType.SET], 'SET');
    assert.equal(StatementType[StatementType.LOAD], 'LOAD');
    assert.equal(StatementType[StatementType.RELATION], 'RELATION');
    assert.equal(StatementType[StatementType.EXTENSION], 'EXTENSION');
    assert.equal(StatementType[StatementType.LOGICAL_PLAN], 'LOGICAL_PLAN');
    assert.equal(StatementType[StatementType.ATTACH], 'ATTACH');
    assert.equal(StatementType[StatementType.DETACH], 'DETACH');
    assert.equal(StatementType[StatementType.MULTI], 'MULTI');
  });
});
