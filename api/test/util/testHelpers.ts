import { assert } from 'vitest';
import {
  DuckDBConnection,
  DuckDBDataChunk,
  DuckDBInstance,
  DuckDBResult,
  DuckDBTimestampTZValue,
  DuckDBValue,
  DuckDBVector,
} from '../../src';
import { ColumnNameAndType } from './testAllTypes';

/**
 * Pin the TIMESTAMPTZ display offset to a constant so tests don't depend on the
 * local timezone. Call from a `beforeAll` in each test file.
 */
export function setDefaultTimezone(): void {
  DuckDBTimestampTZValue.timezoneOffsetInMinutes = -330;
}

export async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

export async function withConnection(
  fn: (connection: DuckDBConnection) => Promise<void>,
) {
  const instance = await DuckDBInstance.create();
  const connection = await instance.connect();
  await fn(connection);
}

export function assertColumns(
  result: DuckDBResult,
  expectedColumns: readonly ColumnNameAndType[],
) {
  assert.strictEqual(
    result.columnCount,
    expectedColumns.length,
    'column count',
  );
  for (let i = 0; i < expectedColumns.length; i++) {
    const { name, type } = expectedColumns[i];
    assert.strictEqual(result.columnName(i), name, 'column name');
    assert.strictEqual(
      result.columnTypeId(i),
      type.typeId,
      `column type id (column: ${name})`,
    );
    assert.deepStrictEqual(
      result.columnType(i),
      type,
      `column type (column: ${name})`,
    );
  }
}

export function isVectorType<
  TValue extends DuckDBValue,
  TVector extends DuckDBVector<TValue>,
>(
  vector: DuckDBVector<any> | null,
  vectorType: new (...args: any[]) => TVector,
): vector is TVector {
  return vector instanceof vectorType;
}

export function getColumnVector<
  TValue extends DuckDBValue,
  TVector extends DuckDBVector<TValue>,
>(
  chunk: DuckDBDataChunk,
  columnIndex: number,
  vectorType: new (...args: any[]) => TVector,
): TVector {
  const columnVector = chunk.getColumnVector(columnIndex);
  if (!isVectorType<TValue, TVector>(columnVector, vectorType)) {
    assert.fail(`expected column ${columnIndex} to be a ${vectorType}`);
  }
  return columnVector;
}

export function assertVectorValues<TValue extends DuckDBValue>(
  vector: DuckDBVector<TValue> | null | undefined,
  values: readonly TValue[],
  vectorName: string,
) {
  if (!vector) {
    assert.fail(`${vectorName} unexpectedly null or undefined`);
  }
  assert.strictEqual(
    vector.itemCount,
    values.length,
    `expected vector ${vectorName} item count to be ${values.length} but found ${vector.itemCount}`,
  );
  for (let i = 0; i < values.length; i++) {
    const actual: TValue | null = vector.getItem(i);
    const expected = values[i];
    assert.deepStrictEqual(
      actual,
      expected,
      `expected vector ${vectorName}[${i}] to be ${expected} but found ${actual}`,
    );
  }
}

export function assertValues<
  TValue extends DuckDBValue,
  TVector extends DuckDBVector<TValue>,
>(
  chunk: DuckDBDataChunk,
  columnIndex: number,
  vectorType: new (...args: any[]) => TVector,
  values: readonly (TValue | null)[],
) {
  const vector = getColumnVector(chunk, columnIndex, vectorType);
  assertVectorValues(vector, values, `${columnIndex}`);
}

export function bigints(start: bigint, end: bigint) {
  return Array.from({ length: Number(end - start) + 1 }).map(
    (_, i) => start + BigInt(i),
  );
}
