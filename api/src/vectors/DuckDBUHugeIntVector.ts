import duckdb from '@duckdb/node-bindings';
import {
  DuckDBUHugeIntType,
} from '../DuckDBType';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  getUInt128,
  setUInt128,
  vectorData,
} from './dataAccessors';

export class DuckDBUHugeIntVector extends DuckDBVector<bigint> {
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
  ): DuckDBUHugeIntVector {
    const data = vectorData(vector, itemCount * 16);
    const dataView = new DataView(
      data.buffer,
      data.byteOffset,
      data.byteLength
    );
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBUHugeIntVector(dataView, validity, vector, itemCount);
  }
  public override get type(): DuckDBUHugeIntType {
    return DuckDBUHugeIntType.instance;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): bigint | null {
    return this.validity.itemValid(itemIndex)
      ? getUInt128(this.dataView, itemIndex * 16)
      : null;
  }
  public getDouble(itemIndex: number): number | null {
    return this.validity.itemValid(itemIndex)
      ? duckdb.uhugeint_to_double(getUInt128(this.dataView, itemIndex * 16))
      : null;
  }
  public override setItem(itemIndex: number, value: bigint | null) {
    if (value != null) {
      setUInt128(this.dataView, itemIndex * 16, value);
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
  public override slice(offset: number, length: number): DuckDBUHugeIntVector {
    return new DuckDBUHugeIntVector(
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
