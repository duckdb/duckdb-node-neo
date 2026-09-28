import { assert, test } from 'vitest';
import {
  DuckDBInstance,
  DuckDBLogStorage,
  DuckDBTimestampValue,
} from '../src';

test('custom log storage receives log entries after its handle is destroyed', async () => {
  const instance = await DuckDBInstance.create();
  const connection = await instance.connect();
  const extraData = { source: 'api test' };
  const entries: {
    extraData: object | undefined;
    timestamp: DuckDBTimestampValue;
    level: string;
    logType: string;
    message: string;
  }[] = [];
  const storage = DuckDBLogStorage.create({
    name: 'node_api_test',
    extraData,
    writeLogEntry(receivedExtraData, timestamp, level, logType, message) {
      entries.push({
        extraData: receivedExtraData,
        timestamp,
        level,
        logType,
        message,
      });
    },
  });
  instance.registerLogStorage(storage);
  storage.destroySync();

  await connection.run('SET enable_logging = true');
  await connection.run("SET logging_storage = 'node_api_test'");
  await connection.run("SELECT write_log('api log entry')");

  const entry = entries.find(({ message }) => message.includes('api log entry'));
  assert.isDefined(entry);
  assert.strictEqual(entry.extraData, extraData);
  assert.instanceOf(entry.timestamp, DuckDBTimestampValue);
  assert.strictEqual(entry.level, 'INFO');
  assert.isNotEmpty(entry.logType);
});
