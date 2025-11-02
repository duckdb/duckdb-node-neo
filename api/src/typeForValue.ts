import {
  ANY,
  ARRAY,
  BIT,
  BLOB,
  BOOLEAN,
  DATE,
  DECIMAL,
  DOUBLE,
  DuckDBType,
  HUGEINT,
  INTEGER,
  INTERVAL,
  LIST,
  MAP,
  SQLNULL,
  STRUCT,
  TIME,
  TIME_NS,
  TIMESTAMP,
  TIMESTAMP_MS,
  TIMESTAMP_NS,
  TIMESTAMP_S,
  TIMESTAMPTZ,
  TIMETZ,
  UNION,
  UUID,
  VARCHAR,
} from './DuckDBType';
import {
  DuckDBArrayValue,
  DuckDBBitValue,
  DuckDBBlobValue,
  DuckDBDateValue,
  DuckDBDecimalValue,
  DuckDBIntervalValue,
  DuckDBListValue,
  DuckDBMapValue,
  DuckDBStructValue,
  DuckDBTimeNSValue,
  DuckDBTimestampMillisecondsValue,
  DuckDBTimestampNanosecondsValue,
  DuckDBTimestampSecondsValue,
  DuckDBTimestampTZValue,
  DuckDBTimestampValue,
  DuckDBTimeTZValue,
  DuckDBTimeValue,
  DuckDBUnionValue,
  DuckDBUUIDValue,
  DuckDBValue,
} from './values';

export function typeForValue(value: DuckDBValue): DuckDBType {
  if (value === null) {
    return SQLNULL;
  } else {
    switch (typeof value) {
      case 'boolean':
        return BOOLEAN;
      case 'number':
        if (Math.round(value) === value) {
          return INTEGER;
        } else {
          return DOUBLE;
        }
      case 'bigint':
        return HUGEINT;
      case 'string':
        return VARCHAR;
      case 'object':
        if (value instanceof DuckDBArrayValue) {
          return ARRAY(typeForValue(value.items[0]), value.items.length);
        } else if (value instanceof DuckDBBitValue) {
          return BIT;
        } else if (value instanceof DuckDBBlobValue) {
          return BLOB;
        } else if (value instanceof DuckDBDateValue) {
          return DATE;
        } else if (value instanceof DuckDBDecimalValue) {
          return DECIMAL(value.width, value.scale);
        } else if (value instanceof DuckDBIntervalValue) {
          return INTERVAL;
        } else if (value instanceof DuckDBListValue) {
          return LIST(typeForValue(value.items[0]));
        } else if (value instanceof DuckDBMapValue) {
          return MAP(
            typeForValue(value.entries[0].key),
            typeForValue(value.entries[0].value)
          );
        } else if (value instanceof DuckDBStructValue) {
          const entryTypes: Record<string, DuckDBType> = {};
          for (const key in value.entries) {
            entryTypes[key] = typeForValue(value.entries[key]);
          }
          return STRUCT(entryTypes);
        } else if (value instanceof DuckDBTimestampMillisecondsValue) {
          return TIMESTAMP_MS;
        } else if (value instanceof DuckDBTimestampNanosecondsValue) {
          return TIMESTAMP_NS;
        } else if (value instanceof DuckDBTimestampSecondsValue) {
          return TIMESTAMP_S;
        } else if (value instanceof DuckDBTimestampTZValue) {
          return TIMESTAMPTZ;
        } else if (value instanceof DuckDBTimestampValue) {
          return TIMESTAMP;
        } else if (value instanceof DuckDBTimeTZValue) {
          return TIMETZ;
        } else if (value instanceof DuckDBTimeValue) {
          return TIME;
        } else if (value instanceof DuckDBTimeNSValue) {
          return TIME_NS;
        } else if (value instanceof DuckDBUnionValue) {
          return UNION({ [value.tag]: typeForValue(value.value) });
        } else if (value instanceof DuckDBUUIDValue) {
          return UUID;
        }
        break;
    }
  }
  return ANY;
}
