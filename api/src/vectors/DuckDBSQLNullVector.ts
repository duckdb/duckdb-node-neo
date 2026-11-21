import duckdb from '@duckdb/node-bindings';
import { DuckDBSQLNullType } from '../DuckDBType';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';

/** A vector of the SQLNULL type, whose items are all null. */
export class DuckDBSQLNullVector extends DuckDBVector<null> {
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  private readonly _itemCount: number;
  constructor(
    validity: DuckDBValidity,
    vector: duckdb.Vector,
    itemCount: number
  ) {
    super();
    this.validity = validity;
    this.vector = vector;
    this._itemCount = itemCount;
  }
  static fromRawVector(
    vector: duckdb.Vector,
    itemCount: number
  ): DuckDBSQLNullVector {
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBSQLNullVector(validity, vector, itemCount);
  }
  public override get type(): DuckDBSQLNullType {
    return DuckDBSQLNullType.instance;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(_itemIndex: number): null {
    return null;
  }
  public override setItem(itemIndex: number, value: null) {
    if (value !== null) {
      throw new Error(`Cannot set a non-null value in a SQLNULL vector`);
    }
    this.validity.setItemValid(itemIndex, false);
  }
  public override flush() {
    this.validity.flush(this.vector);
  }
  public override slice(offset: number, length: number): DuckDBSQLNullVector {
    return new DuckDBSQLNullVector(
      this.validity.slice(offset, length),
      this.vector,
      length
    );
  }
}
