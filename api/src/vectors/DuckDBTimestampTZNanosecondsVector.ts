import duckdb from '@duckdb/node-bindings';
import {
  DuckDBTimestampTZNanosecondsType,
} from '../DuckDBType';
import {
  DuckDBTimestampTZNanosecondsValue,
} from '../values';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  vectorData,
} from './dataAccessors';

export class DuckDBTimestampTZNanosecondsVector extends DuckDBVector<DuckDBTimestampTZNanosecondsValue> {
  private readonly items: BigInt64Array;
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  constructor(
    items: BigInt64Array,
    validity: DuckDBValidity,
    vector: duckdb.Vector
  ) {
    super();
    this.items = items;
    this.validity = validity;
    this.vector = vector;
  }
  static fromRawVector(
    vector: duckdb.Vector,
    itemCount: number
  ): DuckDBTimestampTZNanosecondsVector {
    const data = vectorData(
      vector,
      itemCount * BigInt64Array.BYTES_PER_ELEMENT
    );
    const items = new BigInt64Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBTimestampTZNanosecondsVector(items, validity, vector);
  }
  public override get type(): DuckDBTimestampTZNanosecondsType {
    return DuckDBTimestampTZNanosecondsType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(
    itemIndex: number
  ): DuckDBTimestampTZNanosecondsValue | null {
    return this.validity.itemValid(itemIndex)
      ? new DuckDBTimestampTZNanosecondsValue(this.items[itemIndex])
      : null;
  }
  public override setItem(
    itemIndex: number,
    value: DuckDBTimestampTZNanosecondsValue | null
  ) {
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
      0,
      this.items.buffer as ArrayBuffer,
      this.items.byteOffset,
      this.items.byteLength
    );
    this.validity.flush(this.vector);
  }
  public override slice(
    offset: number,
    length: number
  ): DuckDBTimestampTZNanosecondsVector {
    return new DuckDBTimestampTZNanosecondsVector(
      this.items.slice(offset, offset + length),
      this.validity.slice(offset, length),
      this.vector
    );
  }
}
