import duckdb from '@duckdb/node-bindings';

/**
 * Gets the bytes either in or referenced by a `duckdb_string_t`
 * that is at `string_byte_offset` of the given `DataView`.
 */
function getStringBytes(dv: DataView, string_byte_offset: number): Uint8Array {
  const length_in_bytes = dv.getUint32(string_byte_offset, true);
  if (length_in_bytes <= 12) {
    return new Uint8Array(dv.buffer, dv.byteOffset + string_byte_offset + 4, length_in_bytes);
  } else {
    return duckdb.get_data_from_pointer(dv.buffer, dv.byteOffset + string_byte_offset + 8, length_in_bytes);
  }
}

const decoder = new TextDecoder();

/**
 * Gets the UTF-8 string either in or referenced by a `duckdb_string_t`
 * that is at `string_byte_offset` of the given `DataView`.
 */
export function getVarchar(dv: DataView, string_byte_offset: number): string {
  return decoder.decode(getStringBytes(dv, string_byte_offset));
}

export function getValue(type: duckdb.Type, dv: DataView, index: number): any {
  switch (type) {
    case duckdb.Type.INTEGER:
      return dv.getInt32(index * 4, true);
    default:
      throw new Error('not implemented');
  }
}
