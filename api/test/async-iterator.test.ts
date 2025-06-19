import { describe, test, assert } from 'vitest';
import { withConnection } from './api.test';
import assert from 'assert';

describe('DuckDBResult async iterator', () => {

  test('should iterate over basic query results', async () => {
    await withConnection(async (connection) => {
      await connection.run('CREATE TABLE test_table (id INTEGER, name VARCHAR)');
      await connection.run("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')");

      const result = await connection.run('SELECT * FROM test_table ORDER BY id');

      const rows: any[] = [];
      for await (const row of result) {
        rows.push(row);
      }

      assert.equal(rows.length, 2);
      assert.deepEqual(rows[0], { id: 1, name: 'Alice' });
      assert.deepEqual(rows[1], { id: 2, name: 'Bob' });
    });
  });

  test('should handle empty result sets', async () => {
    await withConnection(async (connection) => {
      await connection.run('CREATE TABLE empty_table (id INTEGER)');

      const result = await connection.run('SELECT * FROM empty_table');

      const rows: any[] = [];
      for await (const row of result) {
        rows.push(row);
      }

      assert.equal(rows.length, 0);
    });
  });

  test('should allow early termination with break', async () => {
    await withConnection(async (connection) => {
      await connection.run('CREATE TABLE numbers (value INTEGER)');
      await connection.run('INSERT INTO numbers SELECT range FROM range(10)');

      const result = await connection.run('SELECT * FROM numbers ORDER BY value');

      const rows: any[] = [];
      for await (const row of result) {
        rows.push(row);
        if (rows.length >= 3) {
          break;
        }
      }

      assert.equal(rows.length, 3);
      assert.equal(rows[0].value, 0);
      assert.equal(rows[2].value, 2);
    });
  });

  test('should handle null values', async () => {
    await withConnection(async (connection) => {
      await connection.run('CREATE TABLE nullable_table (id INTEGER, value VARCHAR)');
      await connection.run("INSERT INTO nullable_table VALUES (1, 'test'), (2, null)");

      const result = await connection.run('SELECT * FROM nullable_table ORDER BY id');

      const rows: any[] = [];
      for await (const row of result) {
        rows.push(row);
      }

      assert.equal(rows.length, 2);
      assert.equal(rows[0].value, 'test');
      assert.isNull(rows[1].value);
    });
  });

  test('should fetch chunks progressively', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run('SELECT range as value FROM range(10000)');
  
      let count = 0;
      let chunkCount = 0;
  
      // Track chunks
      const originalFetchChunk = result.fetchChunk.bind(result);
      result.fetchChunk = async function () {
        const chunk = await originalFetchChunk();
        if (chunk && chunk.rowCount > 0) {
          chunkCount++;
        }
        return chunk;
      };
  
      assert.equal(chunkCount, 0);
      
      // Use the SAME iterator instance
      const iterator = result[Symbol.asyncIterator]();
      
      // Get first row
      const firstResult = await iterator.next();
      assert.equal(chunkCount, 1);
      assert.isFalse(firstResult.done);
      count++;
      
      // Continue with the same iterator
      let nextResult = await iterator.next();
      while (!nextResult.done) {
        count++;
        nextResult = await iterator.next();
      }
      
      assert.equal(count, 10000);
      assert.isAbove(chunkCount, 1);
    });
  });
});