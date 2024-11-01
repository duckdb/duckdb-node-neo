const DAYS_IN_400_YEARS = 146097; // (((365 * 4 + 1) * 25) - 1) * 4 + 1
const MILLISECONDS_PER_DAY_NUM = 86400000; // 1000 * 60 * 60 * 24

const MICROSECONDS_PER_SECOND = 1000000n;
const MICROSECONDS_PER_MILLISECOND = 1000n;
const NANOSECONDS_PER_SECOND = 1000000000n
const SECONDS_PER_MINUTE = 60n;
const MINUTES_PER_HOUR = 60n;
const MICROSECONDS_PER_DAY = 86400000000n; // 24 * 60 * 60 * 1000000
const NANOSECONDS_PER_DAY = 86400000000000n; // 24 * 60 * 60 * 1000000000

const NEGATIVE_INFINITY_TIMESTAMP = -9223372036854775807n; // -(2^63-1)
const POSITIVE_INFINITY_TIMESTAMP = 9223372036854775807n; // 2^63-1

export function getDuckDBDateStringFromYearMonthDay(
  year: number,
  month: number,
  dayOfMonth: number,
): string {
  const yearStr = String(Math.abs(year)).padStart(4, '0');
  const monthStr = String(month).padStart(2, '0');
  const dayOfMonthStr = String(dayOfMonth).padStart(2, '0');
  return `${yearStr}-${monthStr}-${dayOfMonthStr}${year < 0 ? ' (BC)' : ''}`;
}

export function getDuckDBDateStringFromDays(days: number): string {
  const absDays = Math.abs(days);
  const sign = days < 0 ? -1 : 1;
  // 400 years is the shortest interval with a fixed number of days. (Leap years and different length months can result
  // in shorter intervals having different number of days.) By separating the number of 400 year intervals from the
  // interval covered by the remaining days, we can guarantee that the date resulting from shifting the epoch by the
  // remaining interval is within the valid range of the JS Date object. This allows us to use JS Date to calculate the
  // year, month, and day of month for the date represented by the remaining interval, thus accounting for leap years
  // and different length months. We can then safely add back the years from the 400 year intervals, because the month
  // and day of month won't change when a date is shifted by a whole number of such intervals.
  const num400YearIntervals = Math.floor(absDays / DAYS_IN_400_YEARS);
  const yearsFrom400YearIntervals = sign * num400YearIntervals * 400;
  const absDaysFromRemainingInterval = absDays % DAYS_IN_400_YEARS;
  const millisecondsFromRemainingInterval =
    sign * absDaysFromRemainingInterval * MILLISECONDS_PER_DAY_NUM;
  const date = new Date(millisecondsFromRemainingInterval);
  let year = yearsFrom400YearIntervals + date.getUTCFullYear();
  if (year < 0) {
    year--; // correct for non-existence of year zero
  }
  const month = date.getUTCMonth() + 1; // getUTCMonth returns zero-indexed month, but we want a one-index month for display
  const dayOfMonth = date.getUTCDate(); // getUTCDate returns one-indexed day-of-month
  return getDuckDBDateStringFromYearMonthDay(year, month, dayOfMonth);
}

export function getDuckDBTimeStringFromParts(
  hoursPart: bigint,
  minutesPart: bigint,
  secondsPart: bigint,
  microsecondsPart: bigint,
): string {
  const hoursStr = String(hoursPart).padStart(2, '0');
  const minutesStr = String(minutesPart).padStart(2, '0');
  const secondsStr = String(secondsPart).padStart(2, '0');
  const microsecondsStr = String(microsecondsPart)
    .padStart(6, '0')
    .replace(/0+$/, '');
  return `${hoursStr}:${minutesStr}:${secondsStr}${
    microsecondsStr.length > 0 ? `.${microsecondsStr}` : ''
  }`;
}

export function getDuckDBTimeStringFromPartsNS(
  hoursPart: bigint,
  minutesPart: bigint,
  secondsPart: bigint,
  nanosecondsPart: bigint,
): string {
  const hoursStr = String(hoursPart).padStart(2, '0');
  const minutesStr = String(minutesPart).padStart(2, '0');
  const secondsStr = String(secondsPart).padStart(2, '0');
  const nanosecondsStr = String(nanosecondsPart)
    .padStart(9, '0')
    .replace(/0+$/, '');
  return `${hoursStr}:${minutesStr}:${secondsStr}${
    nanosecondsStr.length > 0 ? `.${nanosecondsStr}` : ''
  }`;
}

export function getDuckDBTimeStringFromPositiveMicroseconds(
  positiveMicroseconds: bigint,
): string {
  const microsecondsPart = positiveMicroseconds % MICROSECONDS_PER_SECOND;
  const seconds = positiveMicroseconds / MICROSECONDS_PER_SECOND;
  const secondsPart = seconds % SECONDS_PER_MINUTE;
  const minutes = seconds / SECONDS_PER_MINUTE;
  const minutesPart = minutes % MINUTES_PER_HOUR;
  const hoursPart = minutes / MINUTES_PER_HOUR;
  return getDuckDBTimeStringFromParts(
    hoursPart,
    minutesPart,
    secondsPart,
    microsecondsPart,
  );
}

export function getDuckDBTimeStringFromPositiveNanoseconds(
  positiveNanoseconds: bigint,
): string {
  const nanosecondsPart = positiveNanoseconds % NANOSECONDS_PER_SECOND;
  const seconds = positiveNanoseconds / NANOSECONDS_PER_SECOND;
  const secondsPart = seconds % SECONDS_PER_MINUTE;
  const minutes = seconds / SECONDS_PER_MINUTE;
  const minutesPart = minutes % MINUTES_PER_HOUR;
  const hoursPart = minutes / MINUTES_PER_HOUR;
  return getDuckDBTimeStringFromPartsNS(
    hoursPart,
    minutesPart,
    secondsPart,
    nanosecondsPart,
  );
}

export function getDuckDBTimeStringFromMicrosecondsInDay(
  microsecondsInDay: bigint,
): string {
  const positiveMicroseconds =
    microsecondsInDay < 0
      ? microsecondsInDay + MICROSECONDS_PER_DAY
      : microsecondsInDay;
  return getDuckDBTimeStringFromPositiveMicroseconds(positiveMicroseconds);
}

export function getDuckDBTimeStringFromNanosecondsInDay(
  nanosecondsInDay: bigint,
): string {
  const positiveNanoseconds =
    nanosecondsInDay < 0
      ? nanosecondsInDay + NANOSECONDS_PER_DAY
      : nanosecondsInDay;
  return getDuckDBTimeStringFromPositiveNanoseconds(positiveNanoseconds);
}

export function getDuckDBTimeStringFromMicroseconds(
  microseconds: bigint,
): string {
  const negative = microseconds < 0;
  const positiveMicroseconds = negative ? -microseconds : microseconds;
  const positiveString =
    getDuckDBTimeStringFromPositiveMicroseconds(positiveMicroseconds);
  return negative ? `-${positiveString}` : positiveString;
}

export function getDuckDBTimestampStringFromDaysAndMicroseconds(
  days: bigint,
  microsecondsInDay: bigint,
  timezone?: string | null,
): string {
  // This conversion of BigInt to Number is safe, because the largest absolute value that `days` can has is 106751991,
  // which fits without loss of precision in a JS Number. (106751991 = (2^63-1) / MICROSECONDS_PER_DAY)
  const dateStr = getDuckDBDateStringFromDays(Number(days));
  const timeStr = getDuckDBTimeStringFromMicrosecondsInDay(microsecondsInDay);
  const timezoneStr = timezone ? ` ${timezone}` : '';
  return `${dateStr} ${timeStr}${timezoneStr}`;
}

export function getDuckDBTimestampStringFromDaysAndNanoseconds(
  days: bigint,
  nanosecondsInDay: bigint,
  timezone?: string | null,
): string {
  // This conversion of BigInt to Number is safe, because the largest absolute value that `days` can has is 106751
  // which fits without loss of precision in a JS Number. (106751 = (2^63-1) / NANOSECONDS_PER_DAY)
  const dateStr = getDuckDBDateStringFromDays(Number(days));
  const timeStr = getDuckDBTimeStringFromNanosecondsInDay(nanosecondsInDay);
  const timezoneStr = timezone ? ` ${timezone}` : '';
  return `${dateStr} ${timeStr}${timezoneStr}`;
}

export function getDuckDBTimestampStringFromMicroseconds(
  microseconds: bigint,
  timezone?: string | null,
): string {
  // Note that -infinity and infinity are only representable in TIMESTAMP (and TIMESTAMPTZ), not the other timestamp
  // variants. This is by-design and matches DuckDB.
  if (microseconds === NEGATIVE_INFINITY_TIMESTAMP) {
    return '-infinity';
  }
  if (microseconds === POSITIVE_INFINITY_TIMESTAMP) {
    return 'infinity';
  }
  let days = microseconds / MICROSECONDS_PER_DAY;
  let microsecondsPart = microseconds % MICROSECONDS_PER_DAY;
  if (microsecondsPart < 0) {
    days--;
    microsecondsPart += MICROSECONDS_PER_DAY;
  }
  return getDuckDBTimestampStringFromDaysAndMicroseconds(
    days,
    microsecondsPart,
    timezone,
  );
}

export function getDuckDBTimestampStringFromSeconds(
  seconds: bigint,
  timezone?: string | null,
): string {
  return getDuckDBTimestampStringFromMicroseconds(
    seconds * MICROSECONDS_PER_SECOND,
    timezone,
  );
}

export function getDuckDBTimestampStringFromMilliseconds(
  milliseconds: bigint,
  timezone?: string | null,
): string {
  return getDuckDBTimestampStringFromMicroseconds(
    milliseconds * MICROSECONDS_PER_MILLISECOND,
    timezone,
  );
}

export function getDuckDBTimestampStringFromNanoseconds(
  nanoseconds: bigint,
  timezone?: string | null,
): string {
  let days = nanoseconds / NANOSECONDS_PER_DAY;
  let nanosecondsPart = nanoseconds % NANOSECONDS_PER_DAY;
  if (nanosecondsPart < 0) {
    days--;
    nanosecondsPart += NANOSECONDS_PER_DAY;
  }
  return getDuckDBTimestampStringFromDaysAndNanoseconds(
    days,
    nanosecondsPart,
    timezone,
  );
}

// Assumes baseUnit can be pluralized by adding an 's'.
function numberAndUnit(value: number, baseUnit: string): string {
  return `${value} ${baseUnit}${Math.abs(value) !== 1 ? 's' : ''}`;
}

export function getDuckDBIntervalString(
  months: number,
  days: number,
  microseconds: bigint,
): string {
  const parts: string[] = [];
  if (months !== 0) {
    const sign = months < 0 ? -1 : 1;
    const absMonths = Math.abs(months);
    const absYears = Math.floor(absMonths / 12);
    const years = sign * absYears;
    const extraMonths = sign * (absMonths - absYears * 12);
    if (years !== 0) {
      parts.push(numberAndUnit(years, 'year'));
    }
    if (extraMonths !== 0) {
      parts.push(numberAndUnit(extraMonths, 'month'));
    }
  }
  if (days !== 0) {
    parts.push(numberAndUnit(days, 'day'));
  }
  if (microseconds !== 0n) {
    parts.push(getDuckDBTimeStringFromMicroseconds(microseconds));
  }
  if (parts.length > 0) {
    return parts.join(' ');
  }
  return '00:00:00';
}
