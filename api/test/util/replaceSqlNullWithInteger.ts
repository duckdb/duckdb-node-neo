import {
  ARRAY,
  DuckDBIntegerType,
  DuckDBType,
  DuckDBTypeId,
  LIST,
  MAP,
  STRUCT,
  UNION,
} from '../../src';

export function replaceSqlNullWithInteger(input: DuckDBType): DuckDBType {
  switch (input.typeId) {
    case DuckDBTypeId.SQLNULL:
      return DuckDBIntegerType.create(input.alias);
    case DuckDBTypeId.LIST:
      return LIST(replaceSqlNullWithInteger(input.valueType), input.alias);
    case DuckDBTypeId.STRUCT: {
      const entries: Record<string, DuckDBType> = {};
      for (let i = 0; i < input.entryCount; i++) {
        entries[input.entryNames[i]] = replaceSqlNullWithInteger(
          input.entryTypes[i],
        );
      }
      return STRUCT(entries, input.alias);
    }
    case DuckDBTypeId.ARRAY:
      return ARRAY(
        replaceSqlNullWithInteger(input.valueType),
        input.length,
        input.alias,
      );
    case DuckDBTypeId.MAP:
      return MAP(
        replaceSqlNullWithInteger(input.keyType),
        replaceSqlNullWithInteger(input.valueType),
        input.alias,
      );
    case DuckDBTypeId.UNION: {
      const members: Record<string, DuckDBType> = {};
      for (let i = 0; i < input.memberCount; i++) {
        members[input.memberTags[i]] = replaceSqlNullWithInteger(
          input.memberTypes[i],
        );
      }
      return UNION(members, input.alias);
    }
    default:
      return input;
  }
}
