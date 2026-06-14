import duckdb from '@duckdb/node-bindings';
import {
  DuckDBTimestampType,
} from '../DuckDBType';
import {
  DuckDBTimestampValue,
} from '../values';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  vectorData,
} from './dataAccessors';

export class DuckDBTimestampVector extends DuckDBVector<DuckDBTimestampValue> {
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
  ): DuckDBTimestampVector {
    const data = vectorData(
      vector,
      itemCount * BigInt64Array.BYTES_PER_ELEMENT
    );
    const items = new BigInt64Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBTimestampVector(items, validity, vector);
  }
  public override get type(): DuckDBTimestampType {
    return DuckDBTimestampType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): DuckDBTimestampValue | null {
    return this.validity.itemValid(itemIndex)
      ? new DuckDBTimestampValue(this.items[itemIndex])
      : null;
  }
  public override setItem(
    itemIndex: number,
    value: DuckDBTimestampValue | null
  ) {
    if (value != null) {
      this.items[itemIndex] = value.micros;
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
  public override slice(offset: number, length: number): DuckDBTimestampVector {
    return new DuckDBTimestampVector(
      this.items.slice(offset, offset + length),
      this.validity.slice(offset, length),
      this.vector
    );
  }
}
