/**
 * Decimal string formatting.
 *
 * Supports a subset of the functionality of `BigInt.prototype.toLocaleString` for locale-specific formatting.
 */

/*
 * Locale formatting options for DuckDBDecimal.
 *
 * This is a subset of the options available for `BigInt.prototype.toLocaleString`
 */
export interface DuckDBDecimalFormatOptions {
  useGrouping?: boolean;
  minimumFractionDigits?: number;
  maximumFractionDigits?: number;
}

export interface LocaleOptions {
  locales?: string | string[];
  options?: DuckDBDecimalFormatOptions;
}

/*
 * Get the decimal separator for a given locale.
 * Somewhat expensive, so use getCachedDecimalSeparator if you need to call this multiple times.
 */

function getDecimalSeparator(locales?: string | string[]): string {
  const decimalSeparator =
    new Intl.NumberFormat(locales, { useGrouping: false })
      .formatToParts(0.1)
      .find((part) => part.type === 'decimal')?.value ?? '.';
  return decimalSeparator;
}

/*
 * Get the decimal separator for a given locale, and cache the result.
 */
const cachedDecimalSeparators: { [localeKey: string]: string } = {};

function getCachedDecimalSeparator(locales?: string | string[]): string {
  const cacheKey = JSON.stringify(locales);
  if (cacheKey in cachedDecimalSeparators) {
    return cachedDecimalSeparators[cacheKey];
  }
  const decimalSeparator = getDecimalSeparator(locales);
  cachedDecimalSeparators[cacheKey] = decimalSeparator;
  return decimalSeparator;
}

// Helper function to format whole part of a decimal value.
// Note that we explicitly omit 'minimumFractionDigits' and 'maximumFractionDigits' from the options
// passed to toLocaleString, because they are only relevant for the fractional part of the number, and
// would result in formatting the whole part as a real number, which we don't want.
function formatWholePart(
  localeOptions: LocaleOptions | undefined,
  val: bigint,
): string {
  if (localeOptions) {
    const {
      minimumFractionDigits: _minFD,
      maximumFractionDigits: _maxFD,
      ...restOptions
    } = localeOptions.options ?? {};
    return val.toLocaleString(localeOptions?.locales, restOptions);
  }
  return String(val);
}

// Format the fractional part of a decimal value
// Note that we must handle minimumFractionDigits and maximumFractionDigits ourselves, and that
// we don't apply `useGrouping` because that only applies to the whole part of the number.
function formatFractionalPart(
  localeOptions: LocaleOptions | undefined,
  val: bigint,
  scale: number,
): string {
  const fractionalPartStr = String(val).padStart(scale, '0');
  if (!localeOptions) {
    return fractionalPartStr;
  }
  const minFracDigits = localeOptions?.options?.minimumFractionDigits ?? 0;
  const maxFracDigits = localeOptions?.options?.maximumFractionDigits ?? 20;

  return fractionalPartStr.padEnd(minFracDigits, '0').slice(0, maxFracDigits);
}

/**
 * Convert a scaled decimal value to a string, possibly using locale-specific formatting.
 */
export function stringFromDecimal(
  scaledValue: bigint,
  scale: number,
  localeOptions?: LocaleOptions,
): string {
  // Decimal values are represented as integers that have been scaled up by a power of ten. The `scale` property of
  // the type is the exponent of the scale factor. For a scale greater than zero, we need to separate out the
  // fractional part by reversing this scaling.
  if (scale > 0) {
    const scaleFactor = BigInt(10) ** BigInt(scale);
    const absScaledValue = scaledValue < 0 ? -scaledValue : scaledValue;

    const prefix = scaledValue < 0 ? '-' : '';

    const wholePartNum = absScaledValue / scaleFactor;
    const wholePartStr = formatWholePart(localeOptions, wholePartNum);

    const fractionalPartNum = absScaledValue % scaleFactor;
    const fractionalPartStr = formatFractionalPart(
      localeOptions,
      fractionalPartNum,
      scale,
    );

    const decimalSeparatorStr = localeOptions
      ? getCachedDecimalSeparator(localeOptions.locales)
      : '.';

    return `${prefix}${wholePartStr}${decimalSeparatorStr}${fractionalPartStr}`;
  }
  // For a scale of zero, there is no fractional part, so a direct string conversion works.
  if (localeOptions) {
    return scaledValue.toLocaleString(
      localeOptions?.locales,
      localeOptions?.options as BigIntToLocaleStringOptions | undefined,
    );
  }
  return String(scaledValue);
}
