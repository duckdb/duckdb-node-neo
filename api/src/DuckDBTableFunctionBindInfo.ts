import duckdb from '@duckdb/node-bindings';
import { DuckDBClientContext } from './DuckDBClientContext';
import { DuckDBType } from './DuckDBType';
import { readValue } from './readValue';
import { DuckDBValue } from './values';

export class DuckDBTableFunctionBindInfo {
  private readonly bind_info: duckdb.TableFunctionBindInfo;
  constructor(bind_info: duckdb.TableFunctionBindInfo) {
    this.bind_info = bind_info;
  }
  public get clientContext(): DuckDBClientContext {
    return this.getClientContext();
  }
  public getClientContext(): DuckDBClientContext {
    return new DuckDBClientContext(
      duckdb.table_function_get_client_context(this.bind_info),
    );
  }
  public get extraInfo(): object | undefined {
    return this.getExtraInfo();
  }
  public getExtraInfo(): object | undefined {
    return duckdb.bind_get_extra_info(this.bind_info);
  }
  public get parameterCount(): number {
    return this.getParameterCount();
  }
  public getParameterCount(): number {
    return duckdb.bind_get_parameter_count(this.bind_info);
  }
  public getParameter(index: number): DuckDBValue {
    return readValue(duckdb.bind_get_parameter(this.bind_info, index));
  }
  /** Returns null if no parameter with this name was supplied. */
  public getNamedParameter(name: string): DuckDBValue | null {
    const value = duckdb.bind_get_named_parameter(this.bind_info, name);
    return value === null ? null : readValue(value);
  }
  public addResultColumn(name: string, type: DuckDBType) {
    duckdb.bind_add_result_column(
      this.bind_info,
      name,
      type.toLogicalType().logical_type,
    );
  }
  public setBindData(bindData: object) {
    duckdb.bind_set_bind_data(this.bind_info, bindData);
  }
  public setCardinality(cardinality: number, isExact: boolean) {
    duckdb.bind_set_cardinality(this.bind_info, cardinality, isExact);
  }
  public setError(error: string) {
    duckdb.bind_set_error(this.bind_info, error);
  }
}
