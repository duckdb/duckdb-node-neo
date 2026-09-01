import duckdb from '@duckdb/node-bindings';
import { DuckDBClientContext } from './DuckDBClientContext';

export class DuckDBScalarFunctionInitInfo {
  private readonly init_info: duckdb.ScalarFunctionInitInfo;
  constructor(init_info: duckdb.ScalarFunctionInitInfo) {
    this.init_info = init_info;
  }
  public get clientContext(): DuckDBClientContext {
    return this.getClientContext();
  }
  public getClientContext(): DuckDBClientContext {
    return new DuckDBClientContext(
      duckdb.scalar_function_init_get_client_context(this.init_info),
    );
  }
  public get extraInfo(): object | undefined {
    return this.getExtraInfo();
  }
  public getExtraInfo(): object | undefined {
    return duckdb.scalar_function_init_get_extra_info(this.init_info);
  }
  public get bindData(): object | undefined {
    return this.getBindData();
  }
  public getBindData(): object | undefined {
    return duckdb.scalar_function_init_get_bind_data(this.init_info);
  }
  public setState(state: object) {
    duckdb.scalar_function_init_set_state(this.init_info, state);
  }
  public setError(error: string) {
    duckdb.scalar_function_init_set_error(this.init_info, error);
  }
}
