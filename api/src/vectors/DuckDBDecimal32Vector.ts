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
  getDecimal32,
  getInt32,
  setInt32,
  vectorData,
} from './dataAccessors';

export class DuckDBDecimal32Vector extends DuckDBVector<DuckDBDecimalValue> {
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
  ): DuckDBDecimal32Vector {
    const data = vectorData(vector, itemCount * 4);
    const dataView = new DataView(
      data.buffer,
      data.byteOffset,
      data.byteLength
    );
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBDecimal32Vector(
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
      ? getDecimal32(this.dataView, itemIndex * 4, this.decimalType)
      : null;
  }
  public getScaledValue(itemIndex: number): number | null {
    return this.validity.itemValid(itemIndex)
      ? getInt32(this.dataView, itemIndex * 4)
      : null;
  }
  public override setItem(itemIndex: number, value: DuckDBDecimalValue | null) {
    if (value != null) {
      setInt32(this.dataView, itemIndex * 4, Number(value.value));
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
  public override slice(offset: number, length: number): DuckDBDecimal32Vector {
    return new DuckDBDecimal32Vector(
      this.decimalType,
      new DataView(
        this.dataView.buffer,
        this.dataView.byteOffset + offset * 4,
        length * 4
      ),
      this.validity.slice(offset, length),
      this.vector,
      length
    );
  }
}
