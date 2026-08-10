import duckdb from '@duckdb/node-bindings';
import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBTableFunctionBindInfo } from './DuckDBTableFunctionBindInfo';
import { DuckDBTableFunctionInfo } from './DuckDBTableFunctionInfo';
import { DuckDBTableFunctionInitInfo } from './DuckDBTableFunctionInitInfo';
import { DuckDBType } from './DuckDBType';

export type DuckDBTableBindFunction = (
  bindInfo: DuckDBTableFunctionBindInfo,
) => void;

export type DuckDBTableInitFunction = (
  initInfo: DuckDBTableFunctionInitInfo,
) => void;

/**
 * Produces one chunk of the scan per call.
 *
 * Set `outputDataChunk.rowCount` before writing, then write that many rows to
 * each column vector. A row count of zero reports that the scan is finished, so
 * a function that never sets zero never terminates.
 */
export type DuckDBTableMainFunction = (
  functionInfo: DuckDBTableFunctionInfo,
  outputDataChunk: DuckDBDataChunk,
) => void;

export class DuckDBTableFunction {
  readonly table_function: duckdb.TableFunction;

  public constructor() {
    this.table_function = duckdb.create_table_function();
  }

  public static create({
    name,
    bindFunction,
    initFunction,
    localInitFunction,
    mainFunction,
    parameterTypes,
    namedParameterTypes,
    supportsProjectionPushdown,
    extraInfo,
  }: {
    name: string;
    bindFunction: DuckDBTableBindFunction;
    initFunction: DuckDBTableInitFunction;
    localInitFunction?: DuckDBTableInitFunction;
    mainFunction: DuckDBTableMainFunction;
    parameterTypes?: readonly DuckDBType[];
    namedParameterTypes?: Readonly<Record<string, DuckDBType>>;
    supportsProjectionPushdown?: boolean;
    extraInfo?: object;
  }): DuckDBTableFunction {
    const tableFunction = new DuckDBTableFunction();
    tableFunction.setName(name);
    tableFunction.setBindFunction(bindFunction);
    tableFunction.setInitFunction(initFunction);
    if (localInitFunction) {
      tableFunction.setLocalInitFunction(localInitFunction);
    }
    tableFunction.setMainFunction(mainFunction);
    if (parameterTypes) {
      for (const parameterType of parameterTypes) {
        tableFunction.addParameter(parameterType);
      }
    }
    if (namedParameterTypes) {
      for (const [
        parameterName,
        parameterType,
      ] of Object.entries(namedParameterTypes)) {
        tableFunction.addNamedParameter(parameterName, parameterType);
      }
    }
    if (supportsProjectionPushdown) {
      tableFunction.setSupportsProjectionPushdown(true);
    }
    if (extraInfo) {
      tableFunction.setExtraInfo(extraInfo);
    }
    return tableFunction;
  }

  public destroySync() {
    duckdb.destroy_table_function_sync(this.table_function);
  }

  public setName(name: string) {
    duckdb.table_function_set_name(this.table_function, name);
  }

  public setBindFunction(bindFunction: DuckDBTableBindFunction) {
    duckdb.table_function_set_bind(this.table_function, (info) => {
      bindFunction(new DuckDBTableFunctionBindInfo(info));
    });
  }

  public setInitFunction(initFunction: DuckDBTableInitFunction) {
    duckdb.table_function_set_init(this.table_function, (info) => {
      initFunction(new DuckDBTableFunctionInitInfo(info));
    });
  }

  public setLocalInitFunction(localInitFunction: DuckDBTableInitFunction) {
    duckdb.table_function_set_local_init(this.table_function, (info) => {
      localInitFunction(new DuckDBTableFunctionInitInfo(info));
    });
  }

  public setMainFunction(mainFunction: DuckDBTableMainFunction) {
    duckdb.table_function_set_function(
      this.table_function,
      (info, output) => {
        mainFunction(
          new DuckDBTableFunctionInfo(info),
          new DuckDBDataChunk(output),
        );
      },
    );
  }

  public addParameter(parameterType: DuckDBType) {
    duckdb.table_function_add_parameter(
      this.table_function,
      parameterType.toLogicalType().logical_type,
    );
  }

  public addNamedParameter(name: string, parameterType: DuckDBType) {
    duckdb.table_function_add_named_parameter(
      this.table_function,
      name,
      parameterType.toLogicalType().logical_type,
    );
  }

  public setSupportsProjectionPushdown(pushdown: boolean) {
    duckdb.table_function_supports_projection_pushdown(
      this.table_function,
      pushdown,
    );
  }

  public setExtraInfo(extraInfo: object) {
    duckdb.table_function_set_extra_info(this.table_function, extraInfo);
  }
}
