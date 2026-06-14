import duckdb from '@duckdb/node-bindings';
import {
  DuckDBStructType,
  DuckDBType,
  DuckDBUTinyIntType,
  DuckDBUnionType,
} from '../DuckDBType';
import {
  DuckDBUnionValue,
} from '../values';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBStructVector } from './DuckDBStructVector';

// UNION = STRUCT with first entry named "tag"
export class DuckDBUnionVector extends DuckDBVector<DuckDBUnionValue> {
  private readonly unionType: DuckDBUnionType;
  private readonly structVector: DuckDBStructVector;
  constructor(unionType: DuckDBUnionType, structVector: DuckDBStructVector) {
    super();
    this.unionType = unionType;
    this.structVector = structVector;
  }
  static fromRawVector(
    unionType: DuckDBUnionType,
    vector: duckdb.Vector,
    itemCount: number
  ): DuckDBUnionVector {
    const entryNames: string[] = ['tag'];
    const entryTypes: DuckDBType[] = [DuckDBUTinyIntType.instance];
    const memberCount = unionType.memberCount;
    for (let i = 0; i < memberCount; i++) {
      entryNames.push(unionType.memberTags[i]);
      entryTypes.push(unionType.memberTypes[i]);
    }
    const structVectorType = new DuckDBStructType(entryNames, entryTypes);
    return new DuckDBUnionVector(
      unionType,
      DuckDBStructVector.fromRawVector(structVectorType, vector, itemCount)
    );
  }
  public override get type(): DuckDBUnionType {
    return this.unionType;
  }
  public override get itemCount(): number {
    return this.structVector.itemCount;
  }
  public override getItem(itemIndex: number): DuckDBUnionValue | null {
    const tagValue = this.structVector.getItemValue(itemIndex, 0);
    if (tagValue == null) {
      return null;
    }
    const memberIndex = Number(tagValue);
    const tag = this.unionType.memberTags[memberIndex];
    const entryIndex = memberIndex + 1;
    const value = this.structVector.getItemValue(itemIndex, entryIndex);
    return new DuckDBUnionValue(tag, value);
  }
  public override setItem(itemIndex: number, value: DuckDBUnionValue | null) {
    if (value != null) {
      const memberIndex = this.unionType.memberIndexForTag(value.tag);
      this.structVector.setItemValue(itemIndex, 0, memberIndex);
      const entryIndex = memberIndex + 1;
      this.structVector.setItemValue(itemIndex, entryIndex, value.value);
      for (let i = 1; i <= this.unionType.memberCount; i++) {
        if (i !== entryIndex) {
          this.structVector.setItemValue(itemIndex, i, null);
        }
      }
    } else {
      for (let i = 0; i <= this.unionType.memberCount; i++) {
        this.structVector.setItemValue(itemIndex, i, null);
      }
    }
  }
  public override flush() {
    this.structVector.flush();
  }
  public override slice(offset: number, length: number): DuckDBUnionVector {
    return new DuckDBUnionVector(
      this.unionType,
      this.structVector.slice(offset, length)
    );
  }
}
