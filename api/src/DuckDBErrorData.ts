import duckdb from '@duckdb/node-bindings';

/**
 * Structured error information from DuckDB operations.
 * 
 * ErrorData provides detailed error information including the error type and message,
 * allowing for more precise error handling compared to string-based errors.
 */
export class DuckDBErrorData {
  private readonly error_data: duckdb.ErrorData;

  constructor(error_data: duckdb.ErrorData) {
    this.error_data = error_data;
  }

  /**
   * Get the error type of this error.
   * 
   * @returns The DuckDB error type enumeration value
   */
  public get errorType(): duckdb.ErrorType {
    return duckdb.error_data_error_type(this.error_data);
  }

  /**
   * Get the error message text.
   * 
   * Returns null if there is no error (hasError is false).
   * 
   * @returns The error message, or null if no error
   */
  public get message(): string | null {
    if (!this.hasError) {
      return null;
    }
    return duckdb.error_data_message(this.error_data);
  }

  /**
   * Check if this error object contains an error.
   * 
   * @returns true if an error is present, false otherwise
   */
  public get hasError(): boolean {
    return duckdb.error_data_has_error(this.error_data);
  }

  /**
   * Convert the error to a string representation.
   * 
   * This provides compatibility with code expecting string-based errors.
   * Returns the error message if present, or an empty string if no error.
   * 
   * @returns The error message as a string, or empty string if no error
   */
  public toString(): string {
    return this.message || '';
  }
}
