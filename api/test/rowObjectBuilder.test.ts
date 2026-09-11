import { assert, beforeAll, describe, test } from 'vitest';
import {
  compileConvertingRowObjectBuilder,
  compileConvertingRowObjectBuilderSafe,
  compileRowObjectBuilder,
  compileRowObjectBuilderSafe,
  DuckDBValue,
  JSDuckDBValueConverter,
  JsonDuckDBValueConverter,
} from '../src';
import { setDefaultTimezone, withConnection } from './util/testHelpers';

// Minimal vector stand-in: the getItem fallback path only touches `getItem` and `type`.
function mockVectors(
  values: readonly unknown[],
  types: readonly unknown[] = []
): any[] {
  return values.map((v, c) => ({
    getItem: (_rowIndex: number) => v,
    type: types[c],
  }));
}

// All-false flat flags -> builder uses the getItem fallback for every column.
function noFlat(n: number): boolean[] {
  return new Array(n).fill(false);
}

// Chunk-at-once builder invocation: fills out[0..rowCount) from the given per-column
// accessors and returns the produced rows.
function buildRows(
  builder: any,
  rowCount: number,
  {
    items = [],
    validityBytes = [],
    validityOffset = [],
    vectors = [],
  }: {
    items?: any[];
    validityBytes?: any[];
    validityOffset?: any[];
    vectors?: any[];
  }
): any[] {
  const out: any[] = [];
  builder(out, 0, rowCount, items, validityBytes, validityOffset, vectors);
  return out;
}

// Build a single row via the getItem fallback path (no flat columns). Mock vectors
// ignore the row index, so a 1-row build reads the fixed mock values.
function callViaGetItem(builder: any, vectors: any[]) {
  return buildRows(builder, 1, { vectors })[0];
}

// Reference dynamic-key build, equivalent to the pre-optimization implementation.
function referenceRowObjects(
  columns: DuckDBValue[][],
  columnNames: readonly string[]
): Record<string, DuckDBValue>[] {
  const rowCount = columns.length > 0 ? columns[0].length : 0;
  const rows: Record<string, DuckDBValue>[] = [];
  for (let r = 0; r < rowCount; r++) {
    const o: Record<string, DuckDBValue> = {};
    for (let c = 0; c < columnNames.length; c++) {
      o[columnNames[c]] = columns[c][r];
    }
    rows.push(o);
  }
  return rows;
}

describe('compileRowObjectBuilder (unit)', () => {
  test('builds a fixed-shape literal for valid identifiers (getItem path)', () => {
    const builder = compileRowObjectBuilder(['a', 'b', 'c'], noFlat(3));
    const vectors = mockVectors([1, 2, 3]);
    assert.deepStrictEqual(callViaGetItem(builder, vectors), {
      a: 1,
      b: 2,
      c: 3,
    });
  });

  test('flat columns read inline from typed arrays (all valid)', () => {
    const builder = compileRowObjectBuilder(['a', 'b'], [true, true]);
    const items = [new Int32Array([10, 11]), new Float64Array([1.5, 2.5])];
    const rows = buildRows(builder, 2, {
      items,
      validityBytes: [null, null],
      validityOffset: [0, 0],
    });
    assert.deepStrictEqual(rows[1], { a: 11, b: 2.5 });
  });

  test('flat columns honor inline validity (nulls)', () => {
    const builder = compileRowObjectBuilder(['a'], [true]);
    const items = [new Int32Array([100, 200, 300])];
    // validity bytes: bit 0 valid, bit 1 invalid, bit 2 valid -> 0b00000101 = 5
    const validity = [new Uint8Array([0b00000101])];
    const rows = buildRows(builder, 3, {
      items,
      validityBytes: validity,
      validityOffset: [0],
    });
    assert.strictEqual(rows[0].a, 100);
    assert.strictEqual(rows[1].a, null);
    assert.strictEqual(rows[2].a, 300);
  });

  test('mixed flat + non-flat columns', () => {
    const builder = compileRowObjectBuilder(['flat', 'obj'], [true, false]);
    const items = [new Int32Array([7, 8]), null];
    const vectors = mockVectors([undefined, 'x']); // vectors[1].getItem -> 'x'
    const rows = buildRows(builder, 2, {
      items,
      validityBytes: [null, null],
      validityOffset: [0, 0],
      vectors,
    });
    assert.deepStrictEqual(rows[1], { flat: 8, obj: 'x' });
  });

  test('fills a pre-sized slice starting at base', () => {
    const builder = compileRowObjectBuilder(['a'], [true]);
    const out: any[] = ['keep'];
    out.length = 3;
    builder(out, 1, 2, [new Int32Array([5, 6])], [null], [0], []);
    assert.deepStrictEqual(out, ['keep', { a: 5 }, { a: 6 }]);
  });

  test('handles non-identifier / unusual column names', () => {
    const names = [
      'weird name',
      'a.b',
      '1col',
      'has"quote',
      'back\\slash',
      'emoji_😀',
      '',
    ];
    const builder = compileRowObjectBuilder(names, noFlat(names.length));
    const vectors = mockVectors([10, 20, 30, 40, 50, 60, 70]);
    assert.deepStrictEqual(callViaGetItem(builder, vectors), {
      'weird name': 10,
      'a.b': 20,
      '1col': 30,
      'has"quote': 40,
      'back\\slash': 50,
      'emoji_😀': 60,
      '': 70,
    });
  });

  test('zero columns yields empty literal', () => {
    const builder = compileRowObjectBuilder([], []);
    assert.deepStrictEqual(buildRows(builder, 1, {}), [{}]);
  });

  test('large width (1000 columns) compiles and stays fast', () => {
    const names = Array.from({ length: 1000 }, (_, c) => `col_${c}`);
    const builder = compileRowObjectBuilder(names, noFlat(names.length));
    const vectors = mockVectors(names.map((_, c) => c));
    const row = callViaGetItem(builder, vectors);
    assert.strictEqual(Object.keys(row).length, 1000);
    assert.strictEqual(row['col_0'], 0);
    assert.strictEqual(row['col_999'], 999);
  });

  test('converting builder applies the converter with type', () => {
    const builder = compileConvertingRowObjectBuilder<string>(['a', 'b']);
    const vectors = mockVectors([1, 2], ['T0', 'T1']);
    const converter = (value: unknown, type: unknown) => `${type}:${value}`;
    assert.deepStrictEqual(builder(vectors, 0, converter as any), {
      a: 'T0:1',
      b: 'T1:2',
    });
  });
});

describe('compileRowObjectBuilderSafe (no-eval fallback)', () => {
  test('falls back to dynamic-key build when new Function throws', () => {
    const OrigFunction = globalThis.Function;
    try {
      // Simulate a CSP / no-eval environment.
      (globalThis as any).Function = function () {
        throw new Error('eval blocked');
      };
      const builder = compileRowObjectBuilderSafe(
        ['x', 'weird name'],
        [false, false]
      );
      const vectors = mockVectors([1, 2]);
      assert.deepStrictEqual(callViaGetItem(builder, vectors), {
        x: 1,
        'weird name': 2,
      });
    } finally {
      globalThis.Function = OrigFunction;
    }
  });

  test('converting fallback applies converter when new Function throws', () => {
    const OrigFunction = globalThis.Function;
    try {
      (globalThis as any).Function = function () {
        throw new Error('eval blocked');
      };
      const builder = compileConvertingRowObjectBuilderSafe<string>(['a', 'b']);
      const vectors = mockVectors([1, 2], ['T0', 'T1']);
      const converter = (value: unknown, type: unknown) => `${type}:${value}`;
      assert.deepStrictEqual(builder(vectors, 0, converter as any), {
        a: 'T0:1',
        b: 'T1:2',
      });
    } finally {
      globalThis.Function = OrigFunction;
    }
  });
});

describe('getRowObjects (integration, builder path)', () => {
  beforeAll(setDefaultTimezone);

  test('duplicate column names round-trip via deduplicated names', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        'select i::int as a, i::int + 10 as b, (i + 100)::varchar as a from range(3) t(i)'
      );
      assert.deepStrictEqual(reader.deduplicatedColumnNames(), ['a', 'b', 'a:1']);
      assert.deepStrictEqual(reader.getRowObjects(), [
        { a: 0, b: 10, 'a:1': '100' },
        { a: 1, b: 11, 'a:1': '101' },
        { a: 2, b: 12, 'a:1': '102' },
      ]);
    });
  });

  test('non-identifier column names', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        'SELECT 1 AS "weird name", 2 AS "a.b", 3 AS "1col"'
      );
      assert.deepStrictEqual(reader.getRowObjects(), [
        { 'weird name': 1, 'a.b': 2, '1col': 3 },
      ]);
    });
  });

  test('nulls match reference build', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        'select case when i % 2 = 0 then null else i::int end as a, ' +
          'case when i % 3 = 0 then null else i::varchar end as b from range(6) t(i)'
      );
      const names = reader.deduplicatedColumnNames();
      assert.deepStrictEqual(
        reader.getRowObjects(),
        referenceRowObjects(reader.getColumns(), names)
      );
    });
  });

  test('complex types (STRUCT, LIST) match reference build', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        "select {'x': i::int, 'y': i::varchar} as s, " +
          '[i, i + 1, i + 2]::int[] as l from range(4) t(i)'
      );
      const names = reader.deduplicatedColumnNames();
      assert.deepStrictEqual(
        reader.getRowObjects(),
        referenceRowObjects(reader.getColumns(), names)
      );
      // The JS / Json converting variants must also match a column-major reference
      // built from the same per-type converters.
      assert.deepStrictEqual(
        reader.getRowObjectsJson(),
        referenceRowObjects(
          reader.convertColumns(JsonDuckDBValueConverter) as any,
          names
        ) as any
      );
      assert.deepStrictEqual(
        reader.getRowObjectsJS(),
        referenceRowObjects(
          reader.convertColumns(JSDuckDBValueConverter) as any,
          names
        ) as any
      );
    });
  });

  test('zero rows yields empty array', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        'select i::int as a from range(0) t(i)'
      );
      assert.deepStrictEqual(reader.getRowObjects(), []);
    });
  });

  test('single column', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        'select i::int as a from range(3) t(i)'
      );
      assert.deepStrictEqual(reader.getRowObjects(), [
        { a: 0 },
        { a: 1 },
        { a: 2 },
      ]);
    });
  });

  test('wide result (100 cols) matches reference build', async () => {
    await withConnection(async (connection) => {
      const cols = 100;
      const projection = Array.from(
        { length: cols },
        (_, c) => `i AS col_${c}`
      ).join(', ');
      const reader = await connection.runAndReadAll(
        `SELECT ${projection} FROM range(50) t(i)`
      );
      const names = reader.deduplicatedColumnNames();
      assert.deepStrictEqual(
        reader.getRowObjects(),
        referenceRowObjects(reader.getColumns(), names)
      );
    });
  });

  test('wide-result row objects are fast-properties (if natives syntax available)', async () => {
    let hasFastProperties: ((o: object) => boolean) | undefined;
    try {
      // Only available under `node --allow-natives-syntax`.
      hasFastProperties = eval('(o) => %HasFastProperties(o)') as (
        o: object
      ) => boolean;
    } catch {
      hasFastProperties = undefined;
    }
    if (!hasFastProperties) {
      return; // skipped without the flag
    }
    await withConnection(async (connection) => {
      const cols = 100;
      const projection = Array.from(
        { length: cols },
        (_, c) => `i AS col_${c}`
      ).join(', ');
      const reader = await connection.runAndReadAll(
        `SELECT ${projection} FROM range(10) t(i)`
      );
      const rows = reader.getRowObjects();
      assert.strictEqual(hasFastProperties!(rows[0]), true);
    });
  });
});
