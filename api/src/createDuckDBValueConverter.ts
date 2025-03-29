import { DuckDBTypeId } from './DuckDBTypeId';
import { DuckDBValueConverter } from './DuckDBValueConverter';

export function createDuckDBValueConverter<T>(
  convertersByTypeId: Record<DuckDBTypeId, DuckDBValueConverter<T> | undefined>
): DuckDBValueConverter<T> {
  return (value, type, converter) => {
    if (value == null) {
      return null;
    }
    const converterForTypeId = convertersByTypeId[type.typeId];
    if (!converterForTypeId) {
      throw new Error(`No converter for typeId: ${type.typeId}`);
    }
    return converterForTypeId(value, type, converter);
  };
}
