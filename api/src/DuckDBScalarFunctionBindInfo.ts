import duckdb from '@duckdb/node-bindings';
import { DuckDBClientContext } from './DuckDBClientContext';

export class DuckDBScalarFunctionBindInfo {
  private readonly bind_info: duckdb.ScalarFunctionBindInfo;
  constructor(bind_info: duckdb.ScalarFunctionBindInfo) {
    this.bind_info = bind_info;
  }
  public get clientContext(): DuckDBClientContext {
    return this.getClientContext();
  }
  public getClientContext(): DuckDBClientContext {
    return new DuckDBClientContext(
      duckdb.scalar_function_get_client_context(this.bind_info),
    );
  }
  public get extraInfo(): object | undefined {
    return this.getExtraInfo();
  }
  public getExtraInfo(): object | undefined {
    return duckdb.scalar_function_bind_get_extra_info(this.bind_info);
  }
  public setBindData(bindData: object) {
    duckdb.scalar_function_set_bind_data(this.bind_info, bindData);
  }
  public setError(error: string) {
    duckdb.scalar_function_bind_set_error(this.bind_info, error);
  }
}

/** @deprecated Renamed to DuckDBScalarFunctionBindInfo. */
export const DuckDBBindInfo = DuckDBScalarFunctionBindInfo;
/** @deprecated Renamed to DuckDBScalarFunctionBindInfo. */
export type DuckDBBindInfo = DuckDBScalarFunctionBindInfo;
