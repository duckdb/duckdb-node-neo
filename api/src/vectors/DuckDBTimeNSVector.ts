import duckdb from '@duckdb/node-bindings';
import {
  DuckDBTimeNSType,
} from '../DuckDBType';
import {
  DuckDBTimeNSValue,
} from '../values';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  vectorData,
} from './dataAccessors';

export class DuckDBTimeNSVector extends DuckDBVector<DuckDBTimeNSValue> {
  private readonly items: BigInt64Array;
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  /** This vector's first row in the native vector; nonzero for a slice. */
  private readonly itemOffset: number;
  constructor(
    items: BigInt64Array,
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
  ): DuckDBTimeNSVector {
    const data = vectorData(
      vector,
      itemCount * BigInt64Array.BYTES_PER_ELEMENT
    );
    const items = new BigInt64Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBTimeNSVector(items, validity, vector);
  }
  public override get type(): DuckDBTimeNSType {
    return DuckDBTimeNSType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): DuckDBTimeNSValue | null {
    return this.validity.itemValid(itemIndex)
      ? new DuckDBTimeNSValue(this.items[itemIndex])
      : null;
  }
  public override setItem(itemIndex: number, value: DuckDBTimeNSValue | null) {
    if (value != null) {
      // Checked inline for bulk-write throughput; see dataAccessors.ts.
      if (BigInt.asIntN(64, value.nanos) !== value.nanos) {
        throw new Error(`bigint out of int64 range`);
      }
      this.items[itemIndex] = value.nanos;
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
  public override slice(offset: number, length: number): DuckDBTimeNSVector {
    return new DuckDBTimeNSVector(
      this.items.subarray(offset, offset + length),
      this.validity.slice(offset),
      this.vector,
      this.itemOffset + offset
    );
  }
}
