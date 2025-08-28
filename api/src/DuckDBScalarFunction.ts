import duckdb from '@databrainhq/node-bindings';
import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBFunctionInfo } from './DuckDBFunctionInfo';
import { DuckDBType } from './DuckDBType';
import { DuckDBVector } from './DuckDBVector';

export type DuckDBScalarMainFunction = (
  functionInfo: DuckDBFunctionInfo,
  inputDataChunk: DuckDBDataChunk,
  outputVector: DuckDBVector,
) => void;

export class DuckDBScalarFunction {
  readonly scalar_function: duckdb.ScalarFunction;

  public constructor() {
    this.scalar_function = duckdb.create_scalar_function();
  }

  public static create({
    name,
    mainFunction,
    returnType,
    parameterTypes,
    varArgsType,
    specialHandling,
    volatile,
    extraInfo,
  }: {
    name: string;
    mainFunction: DuckDBScalarMainFunction;
    returnType: DuckDBType;
    parameterTypes?: readonly DuckDBType[];
    varArgsType?: DuckDBType;
    specialHandling?: boolean;
    volatile?: boolean;
    extraInfo?: object;
  }): DuckDBScalarFunction {
    const scalarFunction = new DuckDBScalarFunction();
    scalarFunction.setName(name);
    scalarFunction.setMainFunction(mainFunction);
    scalarFunction.setReturnType(returnType);
    if (parameterTypes) {
      for (const parameterType of parameterTypes) {
        scalarFunction.addParameter(parameterType);
      }
    }
    if (varArgsType) {
      scalarFunction.setVarArgs(varArgsType);
    }
    if (specialHandling) {
      scalarFunction.setSpecialHandling();
    }
    if (volatile) {
      scalarFunction.setVolatile();
    }
    if (extraInfo) {
      scalarFunction.setExtraInfo(extraInfo);
    }
    return scalarFunction;
  }

  public destroySync() {
    duckdb.destroy_scalar_function_sync(this.scalar_function);
  }

  public setName(name: string) {
    duckdb.scalar_function_set_name(this.scalar_function, name);
  }

  public setMainFunction(mainFunction: DuckDBScalarMainFunction) {
    duckdb.scalar_function_set_function(
      this.scalar_function,
      (info, input, output) => {
        const functionInfo = new DuckDBFunctionInfo(info);
        const inputDataChunk = new DuckDBDataChunk(input);
        const outputVector = DuckDBVector.create(
          output,
          inputDataChunk.rowCount,
        );
        mainFunction(functionInfo, inputDataChunk, outputVector);
      },
    );
  }

  public setReturnType(returnType: DuckDBType) {
    duckdb.scalar_function_set_return_type(
      this.scalar_function,
      returnType.toLogicalType().logical_type,
    );
  }

  public addParameter(parameterType: DuckDBType) {
    duckdb.scalar_function_add_parameter(
      this.scalar_function,
      parameterType.toLogicalType().logical_type,
    );
  }

  public setVarArgs(varArgsType: DuckDBType) {
    duckdb.scalar_function_set_varargs(
      this.scalar_function,
      varArgsType.toLogicalType().logical_type,
    );
  }

  public setSpecialHandling() {
    duckdb.scalar_function_set_special_handling(this.scalar_function);
  }

  public setVolatile() {
    duckdb.scalar_function_set_volatile(this.scalar_function);
  }

  public setExtraInfo(extraInfo: object) {
    duckdb.scalar_function_set_extra_info(this.scalar_function, extraInfo);
  }
}
