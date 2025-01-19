import { DuckDBType } from './DuckDBType';
import { DuckDBValue } from './values';

export interface DuckDBValueConverter<T> {
  convertValue(value: DuckDBValue, type: DuckDBType): T;
}
