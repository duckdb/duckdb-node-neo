import duckdb from '@duckdb/node-bindings';
import {
  DuckDBIntervalType,
} from '../DuckDBType';
import {
  DuckDBIntervalValue,
} from '../values';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  getInt32,
  getInt64,
  setInt32,
  setInt64,
  vectorData,
} from './dataAccessors';

export class DuckDBIntervalVector extends DuckDBVector<DuckDBIntervalValue> {
  private readonly dataView: DataView;
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  private readonly _itemCount: number;
  constructor(
    dataView: DataView,
    validity: DuckDBValidity,
    vector: duckdb.Vector,
    itemCount: number
  ) {
    super();
    this.dataView = dataView;
    this.validity = validity;
    this.vector = vector;
    this._itemCount = itemCount;
  }
  static fromRawVector(
    vector: duckdb.Vector,
    itemCount: number
  ): DuckDBIntervalVector {
    const data = vectorData(vector, itemCount * 16);
    const dataView = new DataView(
      data.buffer,
      data.byteOffset,
      data.byteLength
    );
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBIntervalVector(dataView, validity, vector, itemCount);
  }
  public override get type(): DuckDBIntervalType {
    return DuckDBIntervalType.instance;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): DuckDBIntervalValue | null {
    if (!this.validity.itemValid(itemIndex)) {
      return null;
    }
    const itemStart = itemIndex * 16;
    const months = getInt32(this.dataView, itemStart);
    const days = getInt32(this.dataView, itemStart + 4);
    const micros = getInt64(this.dataView, itemStart + 8);
    return new DuckDBIntervalValue(months, days, micros);
  }
  public override setItem(
    itemIndex: number,
    value: DuckDBIntervalValue | null
  ) {
    if (value != null) {
      const itemStart = itemIndex * 16;
      setInt32(this.dataView, itemStart, value.months);
      setInt32(this.dataView, itemStart + 4, value.days);
      setInt64(this.dataView, itemStart + 8, value.micros);
      this.validity.setItemValid(itemIndex, true);
    } else {
      this.validity.setItemValid(itemIndex, false);
    }
  }
  public override flush() {
    duckdb.copy_data_to_vector(
      this.vector,
      0,
      this.dataView.buffer as ArrayBuffer,
      this.dataView.byteOffset,
      this.dataView.byteLength
    );
    this.validity.flush(this.vector);
  }
  public override slice(offset: number, length: number): DuckDBIntervalVector {
    return new DuckDBIntervalVector(
      new DataView(
        this.dataView.buffer,
        this.dataView.byteOffset + offset * 16,
        length * 16
      ),
      this.validity.slice(offset, length),
      this.vector,
      length
    );
  }
}
