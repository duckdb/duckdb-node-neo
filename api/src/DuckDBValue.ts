import duckdb from '@duckdb/node-bindings';

type Date_ = duckdb.Date_;
type Decimal = duckdb.Decimal;
type Interval = duckdb.Interval;
type Time = duckdb.Time;
type Timestamp = duckdb.Timestamp;

export type { Date_, Decimal, Interval, Time, Timestamp };

export type DuckDBValue =
  | null
  | boolean
  | number
  | bigint
  | string
  | Uint8Array
  | Date_
  | Decimal
  | Interval
  | Time
  | Timestamp
  ;
