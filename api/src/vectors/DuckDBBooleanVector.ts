import duckdb from '@duckdb/node-bindings';
import {
  DuckDBBooleanType,
} from '../DuckDBType';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  getBoolean,
  setBoolean,
  vectorData,
} from './dataAccessors';

export class DuckDBBooleanVector extends DuckDBVector<boolean> {
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
  ): DuckDBBooleanVector {
    const data = vectorData(vector, itemCount * duckdb.sizeof_bool);
    const dataView = new DataView(
      data.buffer,
      data.byteOffset,
      data.byteLength
    );
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBBooleanVector(dataView, validity, vector, itemCount);
  }
  public override get type(): DuckDBBooleanType {
    return DuckDBBooleanType.instance;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): boolean | null {
    return this.validity.itemValid(itemIndex)
      ? getBoolean(this.dataView, itemIndex * duckdb.sizeof_bool)
      : null;
  }
  public override setItem(itemIndex: number, value: boolean | null) {
    if (value != null) {
      setBoolean(this.dataView, itemIndex * duckdb.sizeof_bool, value);
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
  public override slice(offset: number, length: number): DuckDBBooleanVector {
    return new DuckDBBooleanVector(
      new DataView(
        this.dataView.buffer,
        this.dataView.byteOffset + offset * duckdb.sizeof_bool,
        length * duckdb.sizeof_bool
      ),
      this.validity.slice(offset, length),
      this.vector,
      length
    );
  }
}
