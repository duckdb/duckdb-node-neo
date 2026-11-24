import duckdb from '@duckdb/node-bindings';
import {
  DuckDBTimeTZType,
} from '../DuckDBType';
import {
  DuckDBTimeTZValue,
} from '../values';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  vectorData,
} from './dataAccessors';

export class DuckDBTimeTZVector extends DuckDBVector<DuckDBTimeTZValue> {
  private readonly items: BigUint64Array;
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  /** This vector's first row in the native vector; nonzero for a slice. */
  private readonly itemOffset: number;
  constructor(
    items: BigUint64Array,
    validity: DuckDBValidity,
    vector: duckdb.Vector,
    itemOffset = 0
  ) {
    super();
    this.itemOffset = itemOffset;
    this.items = items;
    this.validity = validity;
    this.vector = vector;
  }
  static fromRawVector(
    vector: duckdb.Vector,
    itemCount: number
  ): DuckDBTimeTZVector {
    const data = vectorData(
      vector,
      itemCount * BigUint64Array.BYTES_PER_ELEMENT
    );
    const items = new BigUint64Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBTimeTZVector(items, validity, vector);
  }
  public override get type(): DuckDBTimeTZType {
    return DuckDBTimeTZType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): DuckDBTimeTZValue | null {
    return this.validity.itemValid(itemIndex)
      ? DuckDBTimeTZValue.fromBits(this.items[itemIndex])
      : null;
  }
  public override setItem(itemIndex: number, value: DuckDBTimeTZValue | null) {
    if (value != null) {
      // Checked inline for bulk-write throughput; see dataAccessors.ts.
      if (BigInt.asUintN(64, value.bits) !== value.bits) {
        throw new Error(`bigint out of uint64 range`);
      }
      this.items[itemIndex] = value.bits;
      this.validity.setItemValid(itemIndex, true);
    } else {
      this.validity.setItemValid(itemIndex, false);
    }
  }
  public override flush() {
    duckdb.copy_data_to_vector(
      this.vector,
      this.itemOffset * this.items.BYTES_PER_ELEMENT,
      this.items.buffer as ArrayBuffer,
      this.items.byteOffset,
      this.items.byteLength
    );
    this.validity.flush(this.vector);
  }
  public override slice(offset: number, length: number): DuckDBTimeTZVector {
    return new DuckDBTimeTZVector(
      this.items.subarray(offset, offset + length),
      this.validity.slice(offset),
      this.vector,
      this.itemOffset + offset
    );
  }
}
