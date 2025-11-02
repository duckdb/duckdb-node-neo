import { DuckDBArrayValue } from './DuckDBArrayValue';
import { DuckDBBitValue } from './DuckDBBitValue';
import { DuckDBBlobValue } from './DuckDBBlobValue';
import { DuckDBDateValue } from './DuckDBDateValue';
import { DuckDBDecimalValue } from './DuckDBDecimalValue';
import { DuckDBIntervalValue } from './DuckDBIntervalValue';
import { DuckDBListValue } from './DuckDBListValue';
import { DuckDBMapValue } from './DuckDBMapValue';
import { DuckDBStructValue } from './DuckDBStructValue';
import { DuckDBTimeNSValue } from './DuckDBTimeNSValue';
import { DuckDBTimestampMillisecondsValue } from './DuckDBTimestampMillisecondsValue';
import { DuckDBTimestampNanosecondsValue } from './DuckDBTimestampNanosecondsValue';
import { DuckDBTimestampSecondsValue } from './DuckDBTimestampSecondsValue';
import { DuckDBTimestampTZValue } from './DuckDBTimestampTZValue';
import { DuckDBTimestampValue } from './DuckDBTimestampValue';
import { DuckDBTimeTZValue } from './DuckDBTimeTZValue';
import { DuckDBTimeValue } from './DuckDBTimeValue';
import { DuckDBUnionValue } from './DuckDBUnionValue';
import { DuckDBUUIDValue } from './DuckDBUUIDValue';

export type DuckDBValue =
  | null
  | boolean
  | number
  | bigint
  | string
  | DuckDBArrayValue
  | DuckDBBitValue
  | DuckDBBlobValue
  | DuckDBDateValue
  | DuckDBDecimalValue
  | DuckDBIntervalValue
  | DuckDBListValue
  | DuckDBMapValue
  | DuckDBStructValue
  | DuckDBTimestampMillisecondsValue
  | DuckDBTimestampNanosecondsValue
  | DuckDBTimestampSecondsValue
  | DuckDBTimestampTZValue
  | DuckDBTimestampValue
  | DuckDBTimeTZValue
  | DuckDBTimeValue
  | DuckDBTimeNSValue
  | DuckDBUnionValue
  | DuckDBUUIDValue;
