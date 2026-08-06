import duckdb from '@duckdb/node-bindings';
import { DuckDBType } from '../DuckDBType';
import { DuckDBValueConverter } from '../DuckDBValueConverter';
import { DuckDBValue } from '../values';
import { DuckDBValidity } from './DuckDBValidity';
import { vectorRegistry } from './vectorRegistry';

/**
 * Fixed-width, native-endian typed arrays whose elements ARE the JS values of a flat
 * numeric vector (so element `i` equals `getItem(i)` when valid). These can be read
 * directly by the compiled row builder, skipping the `getItem` dispatch.
 */
export type FlatArray =
  | Int8Array
  | Uint8Array
  | Int16Array
  | Uint16Array
  | Int32Array
  | Uint32Array
  | Float32Array
  | Float64Array
  | BigInt64Array
  | BigUint64Array;

export abstract class DuckDBVector<TValue extends DuckDBValue = DuckDBValue> {
  public static standardSize(): number {
    return duckdb.vector_size();
  }
  public static create(
    vector: duckdb.Vector,
    itemCount: number,
    knownType?: DuckDBType
  ): DuckDBVector {
    if (!vectorRegistry.create) {
      throw new Error('DuckDBVector factory has not been registered');
    }
    return vectorRegistry.create(vector, itemCount, knownType);
  }
  public abstract get type(): DuckDBType;
  public abstract get itemCount(): number;
  public abstract getItem(itemIndex: number): TValue | null;
  public abstract setItem(itemIndex: number, value: TValue | null): void;
  public abstract flush(): void;
  public abstract slice(offset: number, length: number): DuckDBVector<TValue>;
  public toArray(): (TValue | null)[] {
    const items: (TValue | null)[] = [];
    for (let i = 0; i < this.itemCount; i++) {
      items.push(this.getItem(i));
    }
    return items;
  }
  /**
   * Bulk-write this vector's values into `target` starting at `targetOffset`.
   *
   * Writing by index into a (typically pre-sized) array avoids the per-cell visitor
   * closure and `push` of the old columnar path. Flat, typed-array-backed vectors
   * override this to inline the validity check and typed-array read, skipping the
   * `getItem` dispatch entirely.
   */
  public appendTo(target: (TValue | null)[], targetOffset: number): void {
    const itemCount = this.itemCount;
    for (let i = 0; i < itemCount; i++) {
      target[targetOffset + i] = this.getItem(i);
    }
  }
  /**
   * Bulk-convert this vector's values into `target` starting at `targetOffset`,
   * applying `converter` to each value. Hoists `type` out of the loop and avoids the
   * per-cell visitor closure.
   */
  public convertTo<T>(
    converter: DuckDBValueConverter<T>,
    target: (T | null)[],
    targetOffset: number
  ): void {
    const type = this.type;
    const itemCount = this.itemCount;
    for (let i = 0; i < itemCount; i++) {
      target[targetOffset + i] = converter(this.getItem(i), type, converter);
    }
  }
  /**
   * If this vector's values can be read directly from a backing typed array (element
   * `i` equals `getItem(i)` when valid), returns that array; otherwise null. Flat
   * numeric vectors override this; everything else falls back to `getItem`.
   */
  public flatReadArray(): FlatArray | null {
    return null;
  }
  /** The validity bitmap, when this vector exposes one for direct (flat) reads. */
  public getValidity(): DuckDBValidity | null {
    return null;
  }
}
