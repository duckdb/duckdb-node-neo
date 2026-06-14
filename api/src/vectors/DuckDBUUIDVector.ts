import duckdb from '@duckdb/node-bindings';
import {
  DuckDBUUIDType,
} from '../DuckDBType';
import {
  DuckDBUUIDValue,
} from '../values';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  getInt128,
  setInt128,
  vectorData,
} from './dataAccessors';

export class DuckDBUUIDVector extends DuckDBVector<DuckDBUUIDValue> {
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
  ): DuckDBUUIDVector {
    const data = vectorData(vector, itemCount * 16);
    const dataView = new DataView(
      data.buffer,
      data.byteOffset,
      data.byteLength
    );
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBUUIDVector(dataView, validity, vector, itemCount);
  }
  public override get type(): DuckDBUUIDType {
    return DuckDBUUIDType.instance;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): DuckDBUUIDValue | null {
    return this.validity.itemValid(itemIndex)
      ? DuckDBUUIDValue.fromStoredHugeInt(
          getInt128(this.dataView, itemIndex * 16)
        )
      : null;
  }
  public override setItem(itemIndex: number, value: DuckDBUUIDValue | null) {
    if (value != null) {
      setInt128(this.dataView, itemIndex * 16, value.hugeint);
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
  public override slice(offset: number, length: number): DuckDBUUIDVector {
    return new DuckDBUUIDVector(
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
