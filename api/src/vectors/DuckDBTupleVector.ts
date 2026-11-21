import duckdb from '@duckdb/node-bindings';
import {
  DuckDBStructType,
} from '../DuckDBType';
import {
  DuckDBTupleValue,
  DuckDBValue,
} from '../values';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';

/**
 * A vector of a tuple type: a STRUCT whose entry names are all empty (see
 * `DuckDBStructType.isTuple`). Items are `DuckDBTupleValue`s.
 */
export class DuckDBTupleVector extends DuckDBVector<DuckDBTupleValue> {
  private readonly tupleType: DuckDBStructType;
  private readonly _itemCount: number;
  private readonly entryVectors: readonly DuckDBVector[];
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  constructor(
    tupleType: DuckDBStructType,
    itemCount: number,
    entryVectors: readonly DuckDBVector[],
    validity: DuckDBValidity,
    vector: duckdb.Vector
  ) {
    super();
    this.tupleType = tupleType;
    this._itemCount = itemCount;
    this.entryVectors = entryVectors;
    this.validity = validity;
    this.vector = vector;
  }
  static fromRawVector(
    tupleType: DuckDBStructType,
    vector: duckdb.Vector,
    itemCount: number
  ): DuckDBTupleVector {
    const entryCount = tupleType.entryCount;
    const entryVectors: DuckDBVector[] = [];
    for (let i = 0; i < entryCount; i++) {
      const child_vector = duckdb.struct_vector_get_child(vector, i);
      entryVectors.push(
        DuckDBVector.create(child_vector, itemCount, tupleType.entryTypes[i])
      );
    }
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBTupleVector(
      tupleType,
      itemCount,
      entryVectors,
      validity,
      vector
    );
  }
  public override get type(): DuckDBStructType {
    return this.tupleType;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): DuckDBTupleValue | null {
    if (!this.validity.itemValid(itemIndex)) {
      return null;
    }
    return new DuckDBTupleValue(
      this.entryVectors.map((entryVector) => entryVector.getItem(itemIndex))
    );
  }
  public getItemValue(
    itemIndex: number,
    entryIndex: number
  ): DuckDBValue | null {
    if (!this.validity.itemValid(itemIndex)) {
      return null;
    }
    return this.entryVectors[entryIndex].getItem(itemIndex);
  }
  public override setItem(itemIndex: number, value: DuckDBTupleValue | null) {
    if (value != null) {
      if (!(value instanceof DuckDBTupleValue)) {
        throw new Error(`Expected DuckDBTupleValue for ${this.tupleType}`);
      }
      const entryCount = this.tupleType.entryCount;
      if (value.items.length !== entryCount) {
        throw new Error(
          `Expected ${entryCount} items for ${this.tupleType}, got ${value.items.length}`
        );
      }
      for (let i = 0; i < entryCount; i++) {
        this.entryVectors[i].setItem(itemIndex, value.items[i]);
      }
      this.validity.setItemValid(itemIndex, true);
    } else {
      for (const entryVector of this.entryVectors) {
        entryVector.setItem(itemIndex, null);
      }
      this.validity.setItemValid(itemIndex, false);
    }
  }
  public setItemValue(
    itemIndex: number,
    entryIndex: number,
    value: DuckDBValue
  ) {
    return this.entryVectors[entryIndex].setItem(itemIndex, value);
  }
  public override flush() {
    for (const entryVector of this.entryVectors) {
      entryVector.flush();
    }
    this.validity.flush(this.vector);
  }
  public override slice(offset: number, length: number): DuckDBTupleVector {
    return new DuckDBTupleVector(
      this.tupleType,
      length,
      this.entryVectors.map((entryVector) => entryVector.slice(offset, length)),
      this.validity.slice(offset, length),
      this.vector
    );
  }
}
