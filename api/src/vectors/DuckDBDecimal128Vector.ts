import duckdb from '@duckdb/node-bindings';
import {
  DuckDBDecimalType,
} from '../DuckDBType';
import {
  DuckDBDecimalValue,
} from '../values';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  getDecimal128,
  getInt128,
  setInt128,
  vectorData,
} from './dataAccessors';

export class DuckDBDecimal128Vector extends DuckDBVector<DuckDBDecimalValue> {
  private readonly decimalType: DuckDBDecimalType;
  private readonly dataView: DataView;
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  private readonly _itemCount: number;
  constructor(
    decimalType: DuckDBDecimalType,
    dataView: DataView,
    validity: DuckDBValidity,
    vector: duckdb.Vector,
    itemCount: number
  ) {
    super();
    this.decimalType = decimalType;
    this.dataView = dataView;
    this.validity = validity;
    this.vector = vector;
    this._itemCount = itemCount;
  }
  static fromRawVector(
    decimalType: DuckDBDecimalType,
    vector: duckdb.Vector,
    itemCount: number
  ): DuckDBDecimal128Vector {
    const data = vectorData(vector, itemCount * 16);
    const dataView = new DataView(
      data.buffer,
      data.byteOffset,
      data.byteLength
    );
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBDecimal128Vector(
      decimalType,
      dataView,
      validity,
      vector,
      itemCount
    );
  }
  public override get type(): DuckDBDecimalType {
    return this.decimalType;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): DuckDBDecimalValue | null {
    return this.validity.itemValid(itemIndex)
      ? getDecimal128(this.dataView, itemIndex * 16, this.decimalType)
      : null;
  }
  public getScaledValue(itemIndex: number): bigint | null {
    return this.validity.itemValid(itemIndex)
      ? getInt128(this.dataView, itemIndex * 16)
      : null;
  }
  public override setItem(itemIndex: number, value: DuckDBDecimalValue | null) {
    if (value != null) {
      setInt128(this.dataView, itemIndex * 16, value.value);
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
  public override slice(
    offset: number,
    length: number
  ): DuckDBDecimal128Vector {
    return new DuckDBDecimal128Vector(
      this.decimalType,
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
