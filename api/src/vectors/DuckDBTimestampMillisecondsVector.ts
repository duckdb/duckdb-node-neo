import duckdb from '@duckdb/node-bindings';
import {
  DuckDBTimestampMillisecondsType,
} from '../DuckDBType';
import {
  DuckDBTimestampMillisecondsValue,
} from '../values';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  vectorData,
} from './dataAccessors';

export class DuckDBTimestampMillisecondsVector extends DuckDBVector<DuckDBTimestampMillisecondsValue> {
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
  ): DuckDBTimestampMillisecondsVector {
    const data = vectorData(
      vector,
      itemCount * BigInt64Array.BYTES_PER_ELEMENT
    );
    const items = new BigInt64Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBTimestampMillisecondsVector(items, validity, vector);
  }
  public override get type(): DuckDBTimestampMillisecondsType {
    return DuckDBTimestampMillisecondsType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(
    itemIndex: number
  ): DuckDBTimestampMillisecondsValue | null {
    return this.validity.itemValid(itemIndex)
      ? new DuckDBTimestampMillisecondsValue(this.items[itemIndex])
      : null;
  }
  public override setItem(
    itemIndex: number,
    value: DuckDBTimestampMillisecondsValue | null
  ) {
    if (value != null) {
      this.items[itemIndex] = value.millis;
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
  ): DuckDBTimestampMillisecondsVector {
    return new DuckDBTimestampMillisecondsVector(
      this.items.slice(offset, offset + length),
      this.validity.slice(offset, length),
      this.vector
    );
  }
}
