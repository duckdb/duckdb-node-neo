import { compileConvertingRowObjectBuilderSafe } from './compileRowObjectBuilder';
import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBValueConverter } from './DuckDBValueConverter';

export function convertRowObjectsFromChunks<T>(
  chunks: readonly DuckDBDataChunk[],
  columnNames: readonly string[],
  converter: DuckDBValueConverter<T>
): Record<string, T | null>[] {
  const rowObjects: Record<string, T | null>[] = [];
  // Compile the fixed-shape builder once per result, then reuse it across all chunks.
  const builder = compileConvertingRowObjectBuilderSafe<T>(columnNames);
  for (const chunk of chunks) {
    chunk.convertToRowObjectsWithBuilder(builder, converter, rowObjects);
  }
  return rowObjects;
}
