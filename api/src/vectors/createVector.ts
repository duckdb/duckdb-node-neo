import duckdb from '@duckdb/node-bindings';
import { DuckDBLogicalType } from '../DuckDBLogicalType';
import {
  DuckDBArrayType,
  DuckDBDecimalType,
  DuckDBEnumType,
  DuckDBListType,
  DuckDBMapType,
  DuckDBStructType,
  DuckDBType,
  DuckDBUnionType,
} from '../DuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBArrayVector } from './DuckDBArrayVector';
import { DuckDBBigIntVector } from './DuckDBBigIntVector';
import { DuckDBBigNumVector } from './DuckDBBigNumVector';
import { DuckDBBitVector } from './DuckDBBitVector';
import { DuckDBBlobVector } from './DuckDBBlobVector';
import { DuckDBBooleanVector } from './DuckDBBooleanVector';
import { DuckDBDateVector } from './DuckDBDateVector';
import { DuckDBDecimal128Vector } from './DuckDBDecimal128Vector';
import { DuckDBDecimal16Vector } from './DuckDBDecimal16Vector';
import { DuckDBDecimal32Vector } from './DuckDBDecimal32Vector';
import { DuckDBDecimal64Vector } from './DuckDBDecimal64Vector';
import { DuckDBDoubleVector } from './DuckDBDoubleVector';
import { DuckDBEnum16Vector } from './DuckDBEnum16Vector';
import { DuckDBEnum32Vector } from './DuckDBEnum32Vector';
import { DuckDBEnum8Vector } from './DuckDBEnum8Vector';
import { DuckDBFloatVector } from './DuckDBFloatVector';
import { DuckDBGeometryVector } from './DuckDBGeometryVector';
import { DuckDBHugeIntVector } from './DuckDBHugeIntVector';
import { DuckDBIntegerVector } from './DuckDBIntegerVector';
import { DuckDBIntervalVector } from './DuckDBIntervalVector';
import { DuckDBListVector } from './DuckDBListVector';
import { DuckDBMapVector } from './DuckDBMapVector';
import { DuckDBSmallIntVector } from './DuckDBSmallIntVector';
import { DuckDBStructVector } from './DuckDBStructVector';
import { DuckDBTimeNSVector } from './DuckDBTimeNSVector';
import { DuckDBTimeTZVector } from './DuckDBTimeTZVector';
import { DuckDBTimeVector } from './DuckDBTimeVector';
import { DuckDBTimestampMillisecondsVector } from './DuckDBTimestampMillisecondsVector';
import { DuckDBTimestampNanosecondsVector } from './DuckDBTimestampNanosecondsVector';
import { DuckDBTimestampSecondsVector } from './DuckDBTimestampSecondsVector';
import { DuckDBTimestampTZVector } from './DuckDBTimestampTZVector';
import { DuckDBTimestampVector } from './DuckDBTimestampVector';
import { DuckDBTinyIntVector } from './DuckDBTinyIntVector';
import { DuckDBUBigIntVector } from './DuckDBUBigIntVector';
import { DuckDBUHugeIntVector } from './DuckDBUHugeIntVector';
import { DuckDBUIntegerVector } from './DuckDBUIntegerVector';
import { DuckDBUSmallIntVector } from './DuckDBUSmallIntVector';
import { DuckDBUTinyIntVector } from './DuckDBUTinyIntVector';
import { DuckDBUUIDVector } from './DuckDBUUIDVector';
import { DuckDBUnionVector } from './DuckDBUnionVector';
import { DuckDBVarCharVector } from './DuckDBVarCharVector';
import { DuckDBVariantVector } from './DuckDBVariantVector';
import { vectorRegistry } from './vectorRegistry';

function createVector(
  vector: duckdb.Vector,
  itemCount: number,
  knownType?: DuckDBType
): DuckDBVector {
    const vectorType = knownType
      ? knownType
      : DuckDBLogicalType.create(
          duckdb.vector_get_column_type(vector)
        ).asType();
    switch (vectorType.typeId) {
      case DuckDBTypeId.BOOLEAN:
        return DuckDBBooleanVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.TINYINT:
        return DuckDBTinyIntVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.SMALLINT:
        return DuckDBSmallIntVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.INTEGER:
        return DuckDBIntegerVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.BIGINT:
        return DuckDBBigIntVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.UTINYINT:
        return DuckDBUTinyIntVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.USMALLINT:
        return DuckDBUSmallIntVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.UINTEGER:
        return DuckDBUIntegerVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.UBIGINT:
        return DuckDBUBigIntVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.FLOAT:
        return DuckDBFloatVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.DOUBLE:
        return DuckDBDoubleVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.TIMESTAMP:
        return DuckDBTimestampVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.DATE:
        return DuckDBDateVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.TIME:
        return DuckDBTimeVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.INTERVAL:
        return DuckDBIntervalVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.HUGEINT:
        return DuckDBHugeIntVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.UHUGEINT:
        return DuckDBUHugeIntVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.VARCHAR:
        return DuckDBVarCharVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.BLOB:
        return DuckDBBlobVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.DECIMAL:
        if (vectorType instanceof DuckDBDecimalType) {
          const { width } = vectorType;
          if (width <= 0) {
            throw new Error(`DECIMAL width not positive: ${width}`);
          } else if (width <= 4) {
            return DuckDBDecimal16Vector.fromRawVector(
              vectorType,
              vector,
              itemCount
            );
          } else if (width <= 9) {
            return DuckDBDecimal32Vector.fromRawVector(
              vectorType,
              vector,
              itemCount
            );
          } else if (width <= 18) {
            return DuckDBDecimal64Vector.fromRawVector(
              vectorType,
              vector,
              itemCount
            );
          } else if (width <= 38) {
            return DuckDBDecimal128Vector.fromRawVector(
              vectorType,
              vector,
              itemCount
            );
          } else {
            throw new Error(`DECIMAL width too large: ${width}`);
          }
        }
        throw new Error(
          'DuckDBType has DECIMAL type id but is not an instance of DuckDBDecimalType'
        );
      case DuckDBTypeId.TIMESTAMP_S:
        return DuckDBTimestampSecondsVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.TIMESTAMP_MS:
        return DuckDBTimestampMillisecondsVector.fromRawVector(
          vector,
          itemCount
        );
      case DuckDBTypeId.TIMESTAMP_NS:
        return DuckDBTimestampNanosecondsVector.fromRawVector(
          vector,
          itemCount
        );
      case DuckDBTypeId.ENUM:
        if (vectorType instanceof DuckDBEnumType) {
          const { internalTypeId } = vectorType;
          switch (internalTypeId) {
            case DuckDBTypeId.UTINYINT:
              return DuckDBEnum8Vector.fromRawVector(
                vectorType,
                vector,
                itemCount
              );
            case DuckDBTypeId.USMALLINT:
              return DuckDBEnum16Vector.fromRawVector(
                vectorType,
                vector,
                itemCount
              );
            case DuckDBTypeId.UINTEGER:
              return DuckDBEnum32Vector.fromRawVector(
                vectorType,
                vector,
                itemCount
              );
            default:
              throw new Error(
                `unsupported ENUM internal type: ${internalTypeId}`
              );
          }
        }
        throw new Error(
          'DuckDBType has ENUM type id but is not an instance of DuckDBEnumType'
        );
      case DuckDBTypeId.LIST:
        if (vectorType instanceof DuckDBListType) {
          return DuckDBListVector.fromRawVector(vectorType, vector, itemCount);
        }
        throw new Error(
          'DuckDBType has LIST type id but is not an instance of DuckDBListType'
        );
      case DuckDBTypeId.STRUCT:
        if (vectorType instanceof DuckDBStructType) {
          return DuckDBStructVector.fromRawVector(
            vectorType,
            vector,
            itemCount
          );
        }
        throw new Error(
          'DuckDBType has STRUCT type id but is not an instance of DuckDBStructType'
        );
      case DuckDBTypeId.MAP:
        if (vectorType instanceof DuckDBMapType) {
          return DuckDBMapVector.fromRawVector(vectorType, vector, itemCount);
        }
        throw new Error(
          'DuckDBType has MAP type id but is not an instance of DuckDBMapType'
        );
      case DuckDBTypeId.ARRAY:
        if (vectorType instanceof DuckDBArrayType) {
          return DuckDBArrayVector.fromRawVector(vectorType, vector, itemCount);
        }
        throw new Error(
          'DuckDBType has ARRAY type id but is not an instance of DuckDBArrayType'
        );
      case DuckDBTypeId.UUID:
        return DuckDBUUIDVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.UNION:
        if (vectorType instanceof DuckDBUnionType) {
          return DuckDBUnionVector.fromRawVector(vectorType, vector, itemCount);
        }
        throw new Error(
          'DuckDBType has UNION type id but is not an instance of DuckDBUnionType'
        );
      case DuckDBTypeId.BIT:
        return DuckDBBitVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.TIME_TZ:
        return DuckDBTimeTZVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.TIMESTAMP_TZ:
        return DuckDBTimestampTZVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.ANY:
        throw new Error(`Invalid vector type: ANY`);
      case DuckDBTypeId.BIGNUM:
        return DuckDBBigNumVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.SQLNULL:
        throw new Error(`Invalid vector type: SQLNULL`);
      case DuckDBTypeId.STRING_LITERAL:
        throw new Error(`Invalid vector type: STRING_LITERAL`);
      case DuckDBTypeId.INTEGER_LITERAL:
        throw new Error(`Invalid vector type: INTEGER_LITERAL`);
      case DuckDBTypeId.TIME_NS:
        return DuckDBTimeNSVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.GEOMETRY:
        return DuckDBGeometryVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.VARIANT:
        return DuckDBVariantVector.fromRawVector(vector, itemCount);
      default:
        throw new Error(
          `Invalid type id: ${(vectorType as DuckDBType).typeId}`
        );
    }
}

vectorRegistry.create = createVector;
