import { DuckDBType } from './DuckDBType';
import { DuckDBValue } from './values';

export type DuckDBValueConverter<T> = (
  value: DuckDBValue,
  type: DuckDBType,
  converter: DuckDBValueConverter<T>
) => T | null;
