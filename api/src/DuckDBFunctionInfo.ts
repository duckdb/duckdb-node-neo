import duckdb from '@duckdb/node-bindings';

export class DuckDBFunctionInfo {
  private readonly function_info: duckdb.FunctionInfo;
  constructor(function_info: duckdb.FunctionInfo) {
    this.function_info = function_info;
  }
  public get bindData(): object | undefined {
    return this.getBindData();
  }
  public getBindData(): object | undefined {
    return duckdb.scalar_function_get_bind_data(this.function_info);
  }
  public get extraInfo(): object | undefined {
    return this.getExtraInfo();
  }
  public getExtraInfo(): object | undefined {
    return duckdb.scalar_function_get_extra_info(this.function_info);
  }
  public setError(error: string) {
    duckdb.scalar_function_set_error(this.function_info, error);
  }
}
