import { DuckDBListType } from '../DuckDBType';
import { DuckDBVector } from '../DuckDBVector';
import { DuckDBValue } from './DuckDBValue';

export class DuckDBListValue<TValue extends DuckDBValue = DuckDBValue> {
  public readonly type: DuckDBListType;
  public readonly vector: DuckDBVector<TValue>;

  public constructor(type: DuckDBListType, vector: DuckDBVector<TValue>) {
    this.type = type;
    this.vector = vector;
  }
}
