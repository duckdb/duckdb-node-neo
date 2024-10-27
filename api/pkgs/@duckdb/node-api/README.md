# DuckDB Node API

An API for using [DuckDB](https://duckdb.org/) in [Node](https://nodejs.org/).

This is a high-level API meant for applications. It depends on low-level bindings that adhere closely to [DuckDB's C API](https://duckdb.org/docs/api/c/overview), available separately as [@duckdb/duckdb-bindings](https://www.npmjs.com/package/@duckdb/node-bindings).

## Features

### Main differences from [duckdb-node](https://www.npmjs.com/package/duckdb)
- Native support for Promises; no need for separate [duckdb-async](https://www.npmjs.com/package/duckdb-async) wrapper.
- DuckDB-specific API; not based on the [SQLite Node API](https://www.npmjs.com/package/sqlite3).
- Lossless & efficent support for values of all [DuckDB data types](https://duckdb.org/docs/sql/data_types/overview).
- Wraps [released DuckDB binaries](https://github.com/duckdb/duckdb/releases) instead of rebuilding DuckDB.
- Built on [DuckDB's C API](https://duckdb.org/docs/api/c/overview); exposes more functionality.

### Roadmap

Some features are not yet complete:
- Friendlier APIs for consuming advanced data types and values, especially converting them to strings.
- Appending and binding advanced data types. (Additional DuckDB C API support needed.)
- Writing to data chunk vectors. (Directly writing to binary buffers is challenging to support using the Node Addon API.)
- User-defined types & functions. (Support for this was added to the DuckDB C API in v1.1.0.)
- Profiling info (Added in v1.1.0)
- Table description (Added in v1.1.0)
- APIs for Arrow. (This part of the DuckDB C API is [deprecated](https://github.com/duckdb/duckdb/blob/e791508e9bc2eb84bc87eb794074f4893093b743/src/include/duckdb.h#L3760).)

### Supported Platforms

- Linux x64
- Mac OS X (Darwin) arm64 (Apple Silicon)
- Windows (Win32) x64

## Examples

### Get Basic Information

```ts
import duckdb from '@duckdb/node-api';

console.log(duckdb.version());

console.log(duckdb.configurationOptionDescriptions());
```

### Create Instance

```ts
import { DuckDBInstance } from '@duckdb/node-api';
```

Create with an in-memory database:
```ts
const instance = await DuckDBInstance.create(':memory:');
```

Equivalent to the above:
```ts
const instance = await DuckDBInstance.create();
```

Read from and write to a database file, which is created if needed:
```ts
const instance = await DuckDBInstance.create('my_duckdb.db');
```

Set configuration options:
```ts
const instance = await DuckDBInstance.create('my_duckdb.db', { threads: '4' });
```

Dispose:
```ts
await instance.dispose();
```

### Connect

```ts
const connection = await instance.connect();
```

Dispose:
```ts
await connection.dispose();
```

### Run SQL

```ts
const result = await connection.run('from test_all_types()');
```

Dispose:
```ts
result.dispose();
```

### Parameterize SQL

```ts
const prepared = await connection.prepare('select $1, $2');
prepared.bindVarchar(1, 'duck');
prepared.bindInteger(2, 42);
const result = await prepared.run();
```

Dispose:
```ts
result.dispose();
prepared.dispose();
```

### Inspect Result

Get column names and types:
```ts
const columnNames = [];
const columnTypes = [];
const columnCount = result.columnCount;
for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
  const columnName = result.columnName(columnIndex);
  const columnType = result.columnType(columnIndex);
  columnNames.push(columnName);
  columnTypes.push(columnType);
}
```

Fetch data chunks:
```ts
const chunks = [];
while (true) {
  const chunk = await result.fetchChunk();
  if (chunk.rowCount === 0) {
    break;
  }
  chunks.push(chunk);
}
```

Read column data:
```ts
const columns = [];
const columnCount = result.columnCount;
for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
  const columnValues = [];
  const columnVector = chunk.getColumn(columnIndex);
  const itemCount = columnVector.itemCount;
  for (let itemIndex = 0; itemIndex < itemCount; itemIndex++) {
    const value = columnVector.getItem(itemIndex);
    columnValues.push(value);
  }
  columns.push(columnValues);
}
```

Dispose data chunk:
```ts
chunk.dispose();
```

### Inspect Data Types

```ts
import { DuckDBTypeId } from '@duckdb/node-api';

function typeToString(dataType) {
  switch (dataType.typeId) {
    case DuckDBTypeId.ARRAY:
      return `${typeToString(dataType.valueType)}[${dataType.length}]`;
    case DuckDBTypeId.DECIMAL:
      return `DECIMAL(${dataType.width},${dataType.scale})`;
    case DuckDBTypeId.ENUM:
      return `ENUM(${dataType.values.map(
        value => `'${value.replace(`'`, `''`)}'`
      ).join(', ')})`;
    case DuckDBTypeId.LIST:
      return `${typeToString(dataType.valueType)}[]`;
    case DuckDBTypeId.MAP:
      return `MAP(${typeToString(dataType.keyType)}, ${typeToString(dataType.valueType)})`;
    case DuckDBTypeId.STRUCT:
      return `STRUCT(${dataType.entries.map(
        entry => `"${entry.name.replace(`"`, `""`)}" ${typeToString(entry.valueType)}`
      ).join(', ')})`;
    case DuckDBTypeId.UNION:
      return `UNION(${dataType.alternatives.map(
        alt => `"${alt.tag.replace(`"`, `""`)}" ${typeToString(alt.valueType)}`
      ).join(', ')})`;
    default:
      return DuckDBTypeId[dataType.typeId];
  }
}
```

### Inspect Data Values

```ts
import { DuckDBTypeId } from '@duckdb/node-api';

function valueToString(value, dataType) {
  switch (dataType.typeId) {
    case DuckDBTypeId.ARRAY:
      return value
        ? `[${Array.from({ length: dataType.length }).map(
            (_, i) => valueToString(value.getItem(i), dataType.valueType)
          ).join(', ')}]`
        : 'null';
    case DuckDBTypeId.DECIMAL:
      return JSON.stringify(value, replacer);
    case DuckDBTypeId.INTERVAL:
      return JSON.stringify(value, replacer);
    case DuckDBTypeId.LIST:
      return value
        ? `[${Array.from({ length: value.itemCount }).map(
            (_, i) => valueToString(value.getItem(i), dataType.valueType)
          ).join(', ')}]`
        : 'null';
    case DuckDBTypeId.MAP:
      return value
        ? `{ ${value.map(
            (entry) => `${valueToString(entry.key, dataType.keyType)}=${valueToString(entry.value, dataType.valueType)}`
          ).join(', ')} }`
        : 'null';
    case DuckDBTypeId.STRUCT:
      return value
        ? `{ ${value.map(
            (entry, i) => `'${entry.name.replace(`'`, `''`)}': ${valueToString(entry.value, dataType.entries[i].valueType)}`
          ).join(', ')} }`
        : 'null';
    case DuckDBTypeId.TIME_TZ:
      return JSON.stringify(value, replacer);
    case DuckDBTypeId.UNION:
      return value
        ? valueToString(value.value, dataType.alternatives.find((alt) => alt.tag === value.tag).valueType)
        : 'null';
    default:
      return String(value);
  }
}

function replacer(key, value) {
  return typeof value === "bigint" ? { $bigint: value.toString() } : value;
}
```

### Append To Table

```ts
const createTableResult = await connection.run(`create or replace table target_table(i integer, v varchar)`);
createTableResult.dispose();

const appender = await connection.createAppender('main', 'target_table');
try {
  appender.appendInteger(42);
  appender.appendVarchar('duck');
  appender.endRow();
  appender.appendInteger(123);
  appender.appendVarchar('mallard');
  appender.endRow();
  appender.flush();
  appender.appendInteger(17);
  appender.appendVarchar('goose');
  appender.endRow();
  appender.close(); // also flushes
} finally {
  appender.dispose();
}
```

### Extract Statements

```ts
const extractedStatements = await connection.extractStatements(`
  create or replace table numbers as from range(?);
  from numbers where range < ?;
  drop table numbers;
`);
const parameterValues = [10, 7];
try {
  const statementCount = extractedStatements.count;
  for (let statementIndex = 0; statementIndex < statementCount; statementIndex++) {
    const prepared = await extractedStatements.prepare(statementIndex);
    try {
      let parameterCount = prepared.parameterCount;
      for (let parameterIndex = 1; parameterIndex <= parameterCount; parameterIndex++) {
        prepared.bindInteger(parameterIndex, parameterValues.shift());
      }
      const result = await prepared.run();
      // ...
      result.dispose();
    } finally {
      prepared.dispose();
    }
  }
} finally {
  extractedStatements.dispose();
}
```

### Control Evaluation

```ts
import { DuckDBPendingResultState } from '@duckdb/node-api';

async function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

const prepared = await connection.prepare('from range(10_000_000)');
try {
  const pending = prepared.start();
  try {
    while (pending.runTask() !== DuckDBPendingResultState.RESULT_READY) {
      console.log('not ready');
      await sleep(1);
    }
    console.log('ready');
    const result = await pending.getResult();
    // ...
    result.dispose();
  } finally {
    pending.dispose();
  }
} finally {
  prepared.dispose();
}
```
