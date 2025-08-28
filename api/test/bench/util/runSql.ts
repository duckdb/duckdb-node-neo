import { DuckDBConnection } from '../../../src';

export async function runSql(
  connection: DuckDBConnection,
  sql: string,
): Promise<void> {
  const result = await connection.run(sql);
  let valueCount = 0;
  let nullCount = 0;
  let chunk = await result.fetchChunk();
  while (chunk && chunk.rowCount > 0) {
    const col0 = chunk.getColumnVector(0);
    for (let i = 0; i < col0.itemCount; i++) {
      if (col0.getItem(i) === null) {
        nullCount++;
      } else {
        valueCount++;
      }
    }
    chunk = await result.fetchChunk();
  }
}
