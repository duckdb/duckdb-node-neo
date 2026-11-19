import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { data } from './utils/expectedVectors';
import { expectResult } from './utils/expectResult';
import { withConnection } from './utils/withConnection';

async function expectSetting(
  connection: duckdb.Connection,
  name: string,
  expected: { value: string; input_type: string; scope: string },
) {
  const result = await duckdb.query(
    connection,
    `SELECT value, input_type, scope FROM duckdb_settings() WHERE name = '${name}'`,
  );
  await expectResult(result, {
    chunkCount: 1,
    rowCount: 1,
    columns: [
      { name: 'value', logicalType: { typeId: duckdb.Type.VARCHAR } },
      { name: 'input_type', logicalType: { typeId: duckdb.Type.VARCHAR } },
      { name: 'scope', logicalType: { typeId: duckdb.Type.VARCHAR } },
    ],
    chunks: [
      {
        rowCount: 1,
        vectors: [
          data(16, [true], [expected.value]),
          data(16, [true], [expected.input_type]),
          data(16, [true], [expected.scope]),
        ],
      },
    ],
  });
}

suite('config option', () => {
  test('register a varchar option with an explicit type and read it back', async () => {
    await withConnection(async (connection) => {
      const option = duckdb.create_config_option();
      duckdb.config_option_set_name(option, 'test_string_option');
      duckdb.config_option_set_type(option, duckdb.create_logical_type(duckdb.Type.VARCHAR));
      duckdb.config_option_set_description(option, 'A string option for the test');
      duckdb.config_option_set_default_value(option, duckdb.create_varchar('default_string'));
      duckdb.register_config_option(connection, option);

      // Until it is set, the value is the default from the database's config.
      const context = duckdb.connection_get_client_context(connection);
      const before = duckdb.client_context_get_config_option(context, 'test_string_option');
      expect(before).not.toBeNull();
      expect(duckdb.get_varchar(before!.value)).toBe('default_string');
      expect(before!.scope).toBe(duckdb.ConfigOptionScope.GLOBAL);

      // A plain SET uses the option's default scope, SESSION here.
      await duckdb.query(connection, "SET test_string_option = 'new_value'");
      const after = duckdb.client_context_get_config_option(context, 'test_string_option');
      expect(duckdb.get_varchar(after!.value)).toBe('new_value');
      expect(after!.scope).toBe(duckdb.ConfigOptionScope.SESSION);

      // duckdb_settings() reports a session value's scope as LOCAL.
      await expectSetting(connection, 'test_string_option', {
        value: 'new_value',
        input_type: 'VARCHAR',
        scope: 'LOCAL',
      });
      const result = await duckdb.query(
        connection,
        "SELECT description FROM duckdb_settings() WHERE name = 'test_string_option'",
      );
      await expectResult(result, {
        chunkCount: 1,
        rowCount: 1,
        columns: [{ name: 'description', logicalType: { typeId: duckdb.Type.VARCHAR } }],
        chunks: [{ rowCount: 1, vectors: [data(16, [true], ['A string option for the test'])] }],
      });
    });
  });

  test('infer the type from the default value and set a global scope', async () => {
    await withConnection(async (connection) => {
      const option = duckdb.create_config_option();
      duckdb.config_option_set_name(option, 'test_integer_option');
      duckdb.config_option_set_default_value(option, duckdb.create_int32(42));
      duckdb.config_option_set_default_scope(option, duckdb.ConfigOptionScope.GLOBAL);
      duckdb.register_config_option(connection, option);

      const context = duckdb.connection_get_client_context(connection);
      const setting = duckdb.client_context_get_config_option(context, 'test_integer_option');
      expect(setting).not.toBeNull();
      expect(duckdb.get_int32(setting!.value)).toBe(42);
      expect(setting!.scope).toBe(duckdb.ConfigOptionScope.GLOBAL);

      // With a GLOBAL default scope, a plain SET changes the global value.
      await duckdb.query(connection, 'SET test_integer_option = 43');
      const changed = duckdb.client_context_get_config_option(context, 'test_integer_option');
      expect(duckdb.get_int32(changed!.value)).toBe(43);
      expect(changed!.scope).toBe(duckdb.ConfigOptionScope.GLOBAL);
      await expectSetting(connection, 'test_integer_option', {
        value: '43',
        input_type: 'INTEGER',
        scope: 'GLOBAL',
      });
    });
  });

  test('cast the default value to the type set before it', async () => {
    await withConnection(async (connection) => {
      const option = duckdb.create_config_option();
      duckdb.config_option_set_name(option, 'test_cast_option');
      duckdb.config_option_set_type(option, duckdb.create_logical_type(duckdb.Type.INTEGER));
      duckdb.config_option_set_default_value(option, duckdb.create_varchar('7'));
      duckdb.register_config_option(connection, option);

      const context = duckdb.connection_get_client_context(connection);
      const setting = duckdb.client_context_get_config_option(context, 'test_cast_option');
      expect(duckdb.get_int32(setting!.value)).toBe(7);
      await expectSetting(connection, 'test_cast_option', {
        value: '7',
        input_type: 'INTEGER',
        scope: 'GLOBAL',
      });
    });
  });

  test('read a built-in setting and get null for an unknown name', async () => {
    await withConnection(async (connection) => {
      const context = duckdb.connection_get_client_context(connection);
      await duckdb.query(connection, 'SET threads = 2');
      const threads = duckdb.client_context_get_config_option(context, 'threads');
      expect(threads).not.toBeNull();
      expect(duckdb.get_int64(threads!.value)).toBe(2n);
      expect(threads!.scope).not.toBe(duckdb.ConfigOptionScope.INVALID);
      expect(duckdb.client_context_get_config_option(context, 'no_such_setting')).toBeNull();
    });
  });

  test('refuse an option without a name, without a type, or with a name already registered', async () => {
    await withConnection(async (connection) => {
      const failure = /Failed to register config option/;

      const unnamed = duckdb.create_config_option();
      duckdb.config_option_set_default_value(unnamed, duckdb.create_varchar('x'));
      expect(() => duckdb.register_config_option(connection, unnamed)).toThrow(failure);

      const untyped = duckdb.create_config_option();
      duckdb.config_option_set_name(untyped, 'test_untyped_option');
      expect(() => duckdb.register_config_option(connection, untyped)).toThrow(failure);

      const first = duckdb.create_config_option();
      duckdb.config_option_set_name(first, 'test_taken_option');
      duckdb.config_option_set_default_value(first, duckdb.create_int32(1));
      duckdb.register_config_option(connection, first);
      const taken = duckdb.create_config_option();
      duckdb.config_option_set_name(taken, 'test_taken_option');
      duckdb.config_option_set_default_value(taken, duckdb.create_int32(2));
      expect(() => duckdb.register_config_option(connection, taken)).toThrow(failure);

      // A refused option is still usable once it is complete.
      duckdb.config_option_set_default_value(untyped, duckdb.create_varchar('now typed'));
      duckdb.register_config_option(connection, untyped);
      const context = duckdb.connection_get_client_context(connection);
      const setting = duckdb.client_context_get_config_option(context, 'test_untyped_option');
      expect(duckdb.get_varchar(setting!.value)).toBe('now typed');
    });
  });

  test('registering copies the option, so one handle can define two settings', async () => {
    await withConnection(async (connection) => {
      const option = duckdb.create_config_option();
      duckdb.config_option_set_name(option, 'test_first_option');
      duckdb.config_option_set_default_value(option, duckdb.create_varchar('first'));
      duckdb.register_config_option(connection, option);
      duckdb.config_option_set_name(option, 'test_second_option');
      duckdb.config_option_set_default_value(option, duckdb.create_varchar('second'));
      duckdb.register_config_option(connection, option);

      const context = duckdb.connection_get_client_context(connection);
      const first = duckdb.client_context_get_config_option(context, 'test_first_option');
      const second = duckdb.client_context_get_config_option(context, 'test_second_option');
      expect(duckdb.get_varchar(first!.value)).toBe('first');
      expect(duckdb.get_varchar(second!.value)).toBe('second');
    });
  });

  test('reject an invalid default scope', () => {
    const option = duckdb.create_config_option();
    expect(() => duckdb.config_option_set_default_scope(option, duckdb.ConfigOptionScope.INVALID)).toThrow(
      RangeError,
    );
    expect(() => duckdb.config_option_set_default_scope(option, 4 as duckdb.ConfigOptionScope)).toThrow(
      RangeError,
    );
  });
});
