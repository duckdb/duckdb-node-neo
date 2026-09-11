import { DuckDBValueConverter } from './DuckDBValueConverter';
import { DuckDBVector, FlatArray } from './DuckDBVector';
import { DuckDBValue } from './values';

// Compiles a function that builds a fixed-shape row object literal from a chunk's
// columns. Two compounding wins over a dynamic-key, per-cell-visitor build:
//
//   1. Fixed-shape object literal -> V8 keeps the object in fast-properties (hidden
//      class) mode instead of dictionary mode (which incrementally adding ~20+ keys
//      triggers, ~3-4x slower + more memory).
//   2. Per-column code specialization: for flat numeric columns the value is read
//      INLINE from the backing typed array with an inline Number validity test,
//      skipping the `getItem` virtual dispatch and the per-cell BigInt validity math.
//      Non-flat columns (VARCHAR, STRUCT, LIST, DATE, ...) fall back to `getItem`.
//
// Each generated read site references a single column (constant index), so it stays
// monomorphic even when the result mixes column types.
//
// The builder processes a WHOLE chunk in a single call: it loops internally over
// [0, rowCount) and writes each row object into `out[base + r]`. Doing the loop inside
// the compiled function (rather than calling a per-row builder from JS) removes one
// function-call boundary per row (rowCount calls per chunk) -- call overhead that shows
// up clearly in profiles -- and writing into a pre-sized array avoids `push` regrowth.
//
// It is invoked per chunk with parallel per-column arrays (prepared once per chunk by
// DuckDBDataChunk.appendToRowObjectsWithBuilder):
//   items[c]          - backing typed array for flat columns, else null
//   validityBytes[c]  - validity byte view for flat columns with nulls, else null
//   validityOffset[c] - validity bit offset for flat columns
//   vectors[c]        - the vector (used by the getItem fallback)

export type RowObjectBuilder = (
  out: Record<string, DuckDBValue>[],
  base: number,
  rowCount: number,
  items: readonly (FlatArray | null)[],
  validityBytes: readonly (Uint8Array | null)[],
  validityOffset: readonly number[],
  vectors: readonly DuckDBVector[]
) => void;

export type ConvertingRowObjectBuilder<T> = (
  vectors: readonly DuckDBVector[],
  rowIndex: number,
  converter: DuckDBValueConverter<T>
) => Record<string, T | null>;

function flatCellExpr(c: number): string {
  // A=items, V=validityBytes, O=validityOffset. All-valid (V[c]===null) is the common,
  // fast path; otherwise an inline byte-granular bit test (no BigInt, no method call).
  return (
    `(V[${c}]===null` +
    `?A[${c}][r]` +
    `:((V[${c}][(O[${c}]+r)>>3]&(1<<((O[${c}]+r)&7)))!==0?A[${c}][r]:null))`
  );
}

export function compileRowObjectBuilder(
  columnNames: readonly string[],
  flatFlags: readonly boolean[]
): RowObjectBuilder {
  const literal =
    '{' +
    columnNames
      .map(
        (name, c) =>
          `${JSON.stringify(name)}: ${
            flatFlags[c] ? flatCellExpr(c) : `g[${c}].getItem(r)`
          }`
      )
      .join(',') +
    '}';
  const body = `for(let r=0;r<rowCount;r++){out[base+r]=${literal};}`;
  // eslint-disable-next-line @typescript-eslint/no-implied-eval, no-new-func
  return new Function(
    'out',
    'base',
    'rowCount',
    'A',
    'V',
    'O',
    'g',
    body
  ) as RowObjectBuilder;
}

export function compileConvertingRowObjectBuilder<T>(
  columnNames: readonly string[]
): ConvertingRowObjectBuilder<T> {
  const body =
    'return {' +
    columnNames
      .map(
        (name, c) =>
          `${JSON.stringify(name)}: converter(vectors[${c}].getItem(rowIndex), vectors[${c}].type, converter)`
      )
      .join(',') +
    '};';
  // eslint-disable-next-line @typescript-eslint/no-implied-eval, no-new-func
  return new Function(
    'vectors',
    'rowIndex',
    'converter',
    body
  ) as ConvertingRowObjectBuilder<T>;
}

// `new Function` is dynamic code evaluation and is blocked under some CSP / locked-down
// runtimes. These *Safe variants fall back to a getItem-based build (slower, but
// identical output) when compilation throws.

export function compileRowObjectBuilderSafe(
  columnNames: readonly string[],
  flatFlags: readonly boolean[]
): RowObjectBuilder {
  try {
    return compileRowObjectBuilder(columnNames, flatFlags);
  } catch {
    const columnCount = columnNames.length;
    return (out, base, rowCount, _items, _validityBytes, _validityOffset, vectors) => {
      for (let r = 0; r < rowCount; r++) {
        const rowObject: Record<string, DuckDBValue> = {};
        for (let c = 0; c < columnCount; c++) {
          rowObject[columnNames[c]] = vectors[c].getItem(r);
        }
        out[base + r] = rowObject;
      }
    };
  }
}

export function compileConvertingRowObjectBuilderSafe<T>(
  columnNames: readonly string[]
): ConvertingRowObjectBuilder<T> {
  try {
    return compileConvertingRowObjectBuilder<T>(columnNames);
  } catch {
    const columnCount = columnNames.length;
    return (vectors, rowIndex, converter) => {
      const rowObject: Record<string, T | null> = {};
      for (let c = 0; c < columnCount; c++) {
        const vector = vectors[c];
        rowObject[columnNames[c]] = converter(
          vector.getItem(rowIndex),
          vector.type,
          converter
        );
      }
      return rowObject;
    };
  }
}
