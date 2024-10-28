import { DuckDBArrayType } from '../DuckDBType';
import { DuckDBVector } from '../DuckDBVector';
import { DuckDBValue } from './DuckDBValue';

export class DuckDBArrayValue<TValue extends DuckDBValue = DuckDBValue> {
  public readonly type: DuckDBArrayType;
  public readonly vector: DuckDBVector<TValue>;

  public constructor(type: DuckDBArrayType, vector: DuckDBVector<TValue>) {
    this.type = type;
    this.vector = vector;
  }
}
