import { quotedString } from '../sql';
import { DuckDBValue } from '../values';

export function displayStringForDuckDBValue(value: DuckDBValue): string {
  if (value == null) {
    return 'NULL';
  }
  if (typeof value === 'string') {
    return quotedString(value);
  }
  return value.toString();
}
