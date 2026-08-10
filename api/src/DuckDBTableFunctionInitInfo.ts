import duckdb from '@duckdb/node-bindings';

export class DuckDBTableFunctionInitInfo {
  private readonly init_info: duckdb.TableFunctionInitInfo;
  constructor(init_info: duckdb.TableFunctionInitInfo) {
    this.init_info = init_info;
  }
  public get extraInfo(): object | undefined {
    return this.getExtraInfo();
  }
  public getExtraInfo(): object | undefined {
    return duckdb.init_get_extra_info(this.init_info);
  }
  public get bindData(): object | undefined {
    return this.getBindData();
  }
  public getBindData(): object | undefined {
    return duckdb.init_get_bind_data(this.init_info);
  }
  /**
   * The number of columns the scan has been asked for, which is all of them
   * unless the function opted into projection pushdown.
   */
  public get columnCount(): number {
    return this.getColumnCount();
  }
  public getColumnCount(): number {
    return duckdb.init_get_column_count(this.init_info);
  }
  /** The index, among the bound result columns, of the given projected column. */
  public getColumnIndex(columnIndex: number): number {
    return duckdb.init_get_column_index(this.init_info, columnIndex);
  }
  /** All projected column indexes, in order. */
  public getColumnIndexes(): number[] {
    const count = this.getColumnCount();
    const indexes: number[] = [];
    for (let i = 0; i < count; i++) {
      indexes.push(this.getColumnIndex(i));
    }
    return indexes;
  }
  public setInitData(initData: object) {
    duckdb.init_set_init_data(this.init_info, initData);
  }
  public setMaxThreads(maxThreads: number) {
    duckdb.init_set_max_threads(this.init_info, maxThreads);
  }
  public setError(error: string) {
    duckdb.init_set_error(this.init_info, error);
  }
}
