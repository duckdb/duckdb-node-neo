import { DuckDBStructType } from '../DuckDBType';
import { DuckDBValue } from './DuckDBValue';

export class DuckDBStructValue {
  public readonly type: DuckDBStructType;
  public readonly values: readonly DuckDBValue[];

  public constructor(type: DuckDBStructType, values: readonly DuckDBValue[]) {
    this.type = type;
    this.values = values;
  }
}
