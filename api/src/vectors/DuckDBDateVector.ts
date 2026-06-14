import duckdb from '@duckdb/node-bindings';
import {
  DuckDBDateType,
} from '../DuckDBType';
import {
  DuckDBDateValue,
} from '../values';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  vectorData,
} from './dataAccessors';

export class DuckDBDateVector extends DuckDBVector<DuckDBDateValue> {
  private readonly items: Int32Array;
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  constructor(
    items: Int32Array,
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
  ): DuckDBDateVector {
    const data = vectorData(vector, itemCount * Int32Array.BYTES_PER_ELEMENT);
    const items = new Int32Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBDateVector(items, validity, vector);
  }
  public override get type(): DuckDBDateType {
    return DuckDBDateType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): DuckDBDateValue | null {
    return this.validity.itemValid(itemIndex)
      ? new DuckDBDateValue(this.items[itemIndex])
      : null;
  }
  public override setItem(itemIndex: number, value: DuckDBDateValue | null) {
    if (value != null) {
      this.items[itemIndex] = value.days;
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
  public override slice(offset: number, length: number): DuckDBDateVector {
    return new DuckDBDateVector(
      this.items.slice(offset, offset + length),
      this.validity.slice(offset, length),
      this.vector
    );
  }
}
