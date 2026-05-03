import duckdb from '@duckdb/node-bindings';

export class DuckDBErrorData {
  private readonly error_data: duckdb.ErrorData;

  constructor(error_data: duckdb.ErrorData) {
    this.error_data = error_data;
  }

  public get errorType(): duckdb.ErrorType {
    return duckdb.error_data_error_type(this.error_data);
  }

  public get message(): string | null {
    if (!this.hasError) {
      return null;
    }
    return duckdb.error_data_message(this.error_data);
  }

  public get hasError(): boolean {
    return duckdb.error_data_has_error(this.error_data);
  }

  public toString(): string {
    return this.message || '';
  }
}
