import duckdb from '@duckdb/node-bindings';
import {
  DuckDBArrayType,
} from '../DuckDBType';
import {
  DuckDBArrayValue,
} from '../values';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';

export class DuckDBArrayVector extends DuckDBVector<DuckDBArrayValue> {
  private readonly arrayType: DuckDBArrayType;
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  private readonly childData: DuckDBVector;
  private readonly _itemCount: number;
  constructor(
    arrayType: DuckDBArrayType,
    validity: DuckDBValidity,
    vector: duckdb.Vector,
    childData: DuckDBVector,
    itemCount: number
  ) {
    super();
    this.arrayType = arrayType;
    this.validity = validity;
    this.vector = vector;
    this.childData = childData;
    this._itemCount = itemCount;
  }
  static fromRawVector(
    arrayType: DuckDBArrayType,
    vector: duckdb.Vector,
    itemCount: number
  ): DuckDBArrayVector {
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    const child_vector = duckdb.array_vector_get_child(vector);
    const childItemsPerArray =
      DuckDBArrayVector.itemSize(arrayType) * arrayType.length;
    const childData = DuckDBVector.create(
      child_vector,
      itemCount * childItemsPerArray,
      arrayType.valueType
    );
    return new DuckDBArrayVector(
      arrayType,
      validity,
      vector,
      childData,
      itemCount
    );
  }
  private static itemSize(arrayType: DuckDBArrayType): number {
    if (arrayType.valueType instanceof DuckDBArrayType) {
      return DuckDBArrayVector.itemSize(arrayType.valueType);
    } else {
      return 1;
    }
  }
  public override get type(): DuckDBArrayType {
    return this.arrayType;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): DuckDBArrayValue | null {
    if (!this.validity.itemValid(itemIndex)) {
      return null;
    }
    return new DuckDBArrayValue(
      this.childData
        .slice(itemIndex * this.arrayType.length, this.arrayType.length)
        .toArray()
    );
  }
  public override setItem(itemIndex: number, value: DuckDBArrayValue | null) {
    if (value != null) {
      const startIndex = itemIndex * this.arrayType.length;
      for (let i = 0; i < this.arrayType.length; i++) {
        this.childData.setItem(startIndex + i, value.items[i]);
      }
      this.validity.setItemValid(itemIndex, true);
    } else {
      const startIndex = itemIndex * this.arrayType.length;
      for (let i = 0; i < this.arrayType.length; i++) {
        this.childData.setItem(startIndex + i, null);
      }
      this.validity.setItemValid(itemIndex, false);
    }
  }
  public override flush() {
    this.childData.flush();
    this.validity.flush(this.vector);
  }
  public override slice(offset: number, length: number): DuckDBArrayVector {
    return new DuckDBArrayVector(
      this.arrayType,
      this.validity.slice(offset, length),
      this.vector,
      this.childData.slice(
        offset * this.arrayType.length,
        length * this.arrayType.length
      ),
      length
    );
  }
}
