import duckdb from '@duckdb/node-bindings';
import {
  DuckDBTimestampSecondsType,
} from '../DuckDBType';
import {
  DuckDBTimestampSecondsValue,
} from '../values';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  vectorData,
} from './dataAccessors';

export class DuckDBTimestampSecondsVector extends DuckDBVector<DuckDBTimestampSecondsValue> {
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
  ): DuckDBTimestampSecondsVector {
    const data = vectorData(
      vector,
      itemCount * BigInt64Array.BYTES_PER_ELEMENT
    );
    const items = new BigInt64Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBTimestampSecondsVector(items, validity, vector);
  }
  public override get type(): DuckDBTimestampSecondsType {
    return DuckDBTimestampSecondsType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(
    itemIndex: number
  ): DuckDBTimestampSecondsValue | null {
    return this.validity.itemValid(itemIndex)
      ? new DuckDBTimestampSecondsValue(this.items[itemIndex])
      : null;
  }
  public override setItem(
    itemIndex: number,
    value: DuckDBTimestampSecondsValue | null
  ) {
    if (value != null) {
      this.items[itemIndex] = value.seconds;
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
  ): DuckDBTimestampSecondsVector {
    return new DuckDBTimestampSecondsVector(
      this.items.slice(offset, offset + length),
      this.validity.slice(offset, length),
      this.vector
    );
  }
}
