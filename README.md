# DuckDB Node Bindings & API

[Node](https://nodejs.org/) bindings to the [DuckDB](https://duckdb.org/) C API, plus a friendly API for using DuckDB in Node applications.

This repository also contains a [GitHub Packages NPM Registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-npm-registry) that serves as a temporary location for the NPM packages until they are ready to be published to the main NPM registry.

The following platforms are supported:
- Linux x64
- Mac OS ARM64 (Apple Silicon)
- Windows x64

## To install the packages hosted here:

### 1. Create a [GitHub Personal Access Token (classic)](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens#creating-a-personal-access-token-classic) (PAT) with at least `read:packages` scope.

Remember to copy the token value and save it somewhere safe.

### 2. Create a local directory with a `package.json` containing:

```
{
  "dependencies": {
    "@duckdb/node-api": "*"
  }
}
```

### 3. In the same directory, create an `.npmrc` containing:

```
@duckdb:registry=https://npm.pkg.github.com
```

### 4. In the same directory, log in to `npm` using your PAT:

```
npm login --scope=@duckdb
```

When prompted, enter your GitHub username, and, for your password, your GitHub PAT.

### 5. In the same directory, run `npm install`.

This should create a `node_modules` directory containing three packages:
- `@duckdb/node-api`
- `@duckdb/node-bindings`
- `@duckdb/node-bindings-darwin-arm64` OR `@duckdb/node-bindings-linux-x64` OR `@duckdb/node-bindings-win32-x64`

## To test the installation:

### 1. Create a test file.

Example (`test.mjs`):
```
import duckdb, { DuckDBInstance, DuckDBTypeId } from '@duckdb/node-api';

console.log(duckdb.version());

const instance = await DuckDBInstance.create();
const connection = await instance.connect();
const result = await connection.run('from test_all_types()');
const chunk = await result.fetchChunk();
for (let c = 0; c < chunk.columnCount; c++) {
  console.log(`[col ${c}] ${result.columnName(c)}::${DuckDBTypeId[result.columnTypeId(c)]}`);
  const type = result.columnType(c);
  const column = chunk.getColumn(c);
  for (let r = 0; r < chunk.rowCount; r++) {
    const value = column.getItem(r);
    console.log(`  [row ${r}] ${valueToString(value, type)}`);
  }
}

function replacer(key, value) {
  return typeof value === "bigint" ? { $bigint: value.toString() } : value;
}

function valueToString(value, type) {
  switch (type.typeId) {
    case DuckDBTypeId.ARRAY:
      return value
        ? `ARRAY [${Array.from({ length: type.length }).map((_, i) => valueToString(value.getItem(i), type.valueType)).join(', ')}]`
        : 'null';
    case DuckDBTypeId.DECIMAL:
      return JSON.stringify(value, replacer);
    case DuckDBTypeId.INTERVAL:
      return JSON.stringify(value, replacer);
    case DuckDBTypeId.LIST:
      return value
        ? `LIST [${Array.from({ length: value.itemCount }).map((_, i) => valueToString(value.getItem(i), type.valueType)).join(', ')}]`
        : 'null';
    case DuckDBTypeId.MAP:
      return value
        ? `MAP { ${value.map((entry) => `${valueToString(entry.key, type.keyType)}: ${valueToString(entry.value, type.valueType)}`).join(', ')} }`
        : 'null';
    case DuckDBTypeId.STRUCT:
      return value
        ? `STRUCT { ${value.map((entry, i) => `"${entry.name}": ${valueToString(entry.value, type.entries[i].valueType)}`).join(', ')} }`
        : 'null';
    case DuckDBTypeId.TIME_TZ:
      return JSON.stringify(value, replacer);
    case DuckDBTypeId.UNION:
      return value
        ? valueToString(value.value, type.alternatives.find((alt) => alt.tag === value.tag).valueType)
        : 'null';
    default:
      return String(value);
  }
}
```

### 2. Run the test file using:

```
node test.mjs
```

Expected output for above example:

```
v1.1.2
[col 0] bool::BOOLEAN
  [row 0] false
  [row 1] true
  [row 2] null
[col 1] tinyint::TINYINT
  [row 0] -128
  [row 1] 127
  [row 2] null
[col 2] smallint::SMALLINT
  [row 0] -32768
  [row 1] 32767
  [row 2] null
[col 3] int::INTEGER
  [row 0] -2147483648
  [row 1] 2147483647
  [row 2] null
[col 4] bigint::BIGINT
  [row 0] -9223372036854775808
  [row 1] 9223372036854775807
  [row 2] null
[col 5] hugeint::HUGEINT
  [row 0] -170141183460469231731687303715884105728
  [row 1] 170141183460469231731687303715884105727
  [row 2] null
[col 6] uhugeint::UHUGEINT
  [row 0] 0
  [row 1] 340282366920938463463374607431768211455
  [row 2] null
[col 7] utinyint::UTINYINT
  [row 0] 0
  [row 1] 255
  [row 2] null
[col 8] usmallint::USMALLINT
  [row 0] 0
  [row 1] 65535
  [row 2] null
[col 9] uint::UINTEGER
  [row 0] 0
  [row 1] 4294967295
  [row 2] null
[col 10] ubigint::UBIGINT
  [row 0] 0
  [row 1] 18446744073709551615
  [row 2] null
[col 11] varint::VARINT
  [row 0] -179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368
  [row 1] 179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368
  [row 2] null
[col 12] date::DATE
  [row 0] -2147483646
  [row 1] 2147483646
  [row 2] null
[col 13] time::TIME
  [row 0] 0
  [row 1] 86400000000
  [row 2] null
[col 14] timestamp::TIMESTAMP
  [row 0] -9223372022400000000
  [row 1] 9223372036854775806
  [row 2] null
[col 15] timestamp_s::TIMESTAMP_S
  [row 0] -9223372022400
  [row 1] 9223372036854
  [row 2] null
[col 16] timestamp_ms::TIMESTAMP_MS
  [row 0] -9223372022400000
  [row 1] 9223372036854775
  [row 2] null
[col 17] timestamp_ns::TIMESTAMP_NS
  [row 0] -9223286400000000000
  [row 1] 9223372036854775806
  [row 2] null
[col 18] time_tz::TIME_TZ
  [row 0] {"microseconds":0,"offset":57599}
  [row 1] {"microseconds":86400000000,"offset":-57599}
  [row 2] null
[col 19] timestamp_tz::TIMESTAMP_TZ
  [row 0] -9223372022400000000
  [row 1] 9223372036854775806
  [row 2] null
[col 20] float::FLOAT
  [row 0] -3.4028234663852886e+38
  [row 1] 3.4028234663852886e+38
  [row 2] null
[col 21] double::DOUBLE
  [row 0] -1.7976931348623157e+308
  [row 1] 1.7976931348623157e+308
  [row 2] null
[col 22] dec_4_1::DECIMAL
  [row 0] {"scaledValue":-9999,"type":{"typeId":19,"width":4,"scale":1}}
  [row 1] {"scaledValue":9999,"type":{"typeId":19,"width":4,"scale":1}}
  [row 2] null
[col 23] dec_9_4::DECIMAL
  [row 0] {"scaledValue":-999999999,"type":{"typeId":19,"width":9,"scale":4}}
  [row 1] {"scaledValue":999999999,"type":{"typeId":19,"width":9,"scale":4}}
  [row 2] null
[col 24] dec_18_6::DECIMAL
  [row 0] {"scaledValue":{"$bigint":"-999999999999999999"},"type":{"typeId":19,"width":18,"scale":6}}
  [row 1] {"scaledValue":{"$bigint":"999999999999999999"},"type":{"typeId":19,"width":18,"scale":6}}
  [row 2] null
[col 25] dec38_10::DECIMAL
  [row 0] {"scaledValue":{"$bigint":"-99999999999999999999999999999999999999"},"type":{"typeId":19,"width":38,"scale":10}}
  [row 1] {"scaledValue":{"$bigint":"99999999999999999999999999999999999999"},"type":{"typeId":19,"width":38,"scale":10}}
  [row 2] null
[col 26] uuid::UUID
  [row 0] -170141183460469231731687303715884105728
  [row 1] 170141183460469231731687303715884105727
  [row 2] null
[col 27] interval::INTERVAL
  [row 0] {"months":0,"days":0,"micros":{"$bigint":"0"}}
  [row 1] {"months":999,"days":999,"micros":{"$bigint":"999999999"}}
  [row 2] null
[col 28] varchar::VARCHAR
  [row 0] 🦆🦆🦆🦆🦆🦆
  [row 1] goose
  [row 2] null
[col 29] blob::BLOB
  [row 0] thisisalongblobwithnullbytes
  [row 1] a
  [row 2] null
[col 30] bit::BIT
  [row 0] 1000100101110100010101001110101
  [row 1] 10101
  [row 2] null
[col 31] small_enum::ENUM
  [row 0] DUCK_DUCK_ENUM
  [row 1] GOOSE
  [row 2] null
[col 32] medium_enum::ENUM
  [row 0] enum_0
  [row 1] enum_299
  [row 2] null
[col 33] large_enum::ENUM
  [row 0] enum_0
  [row 1] enum_69999
  [row 2] null
[col 34] int_array::LIST
  [row 0] LIST []
  [row 1] LIST [42, 999, null, null, -42]
  [row 2] null
[col 35] double_array::LIST
  [row 0] LIST []
  [row 1] LIST [42, NaN, Infinity, -Infinity, null, -42]
  [row 2] null
[col 36] date_array::LIST
  [row 0] LIST []
  [row 1] LIST [0, 2147483647, -2147483647, null, 19124]
  [row 2] null
[col 37] timestamp_array::LIST
  [row 0] LIST []
  [row 1] LIST [0, 9223372036854775807, -9223372036854775807, null, 1652372625000000]
  [row 2] null
[col 38] timestamptz_array::LIST
  [row 0] LIST []
  [row 1] LIST [0, 9223372036854775807, -9223372036854775807, null, 1652397825000000]
  [row 2] null
[col 39] varchar_array::LIST
  [row 0] LIST []
  [row 1] LIST [🦆🦆🦆🦆🦆🦆, goose, null, ]
  [row 2] null
[col 40] nested_int_array::LIST
  [row 0] LIST []
  [row 1] LIST [LIST [], LIST [42, 999, null, null, -42], null, LIST [], LIST [42, 999, null, null, -42]]
  [row 2] null
[col 41] struct::STRUCT
  [row 0] STRUCT { "a": null, "b": null }
  [row 1] STRUCT { "a": 42, "b": 🦆🦆🦆🦆🦆🦆 }
  [row 2] null
[col 42] struct_of_arrays::STRUCT
  [row 0] STRUCT { "a": null, "b": null }
  [row 1] STRUCT { "a": LIST [42, 999, null, null, -42], "b": LIST [🦆🦆🦆🦆🦆🦆, goose, null, ] }
  [row 2] null
[col 43] array_of_structs::LIST
  [row 0] LIST []
  [row 1] LIST [STRUCT { "a": null, "b": null }, STRUCT { "a": 42, "b": 🦆🦆🦆🦆🦆🦆 }, null]
  [row 2] null
[col 44] map::MAP
  [row 0] MAP {  }
  [row 1] MAP { key1: 🦆🦆🦆🦆🦆🦆, key2: goose }
  [row 2] null
[col 45] union::UNION
  [row 0] Frank
  [row 1] 5
  [row 2] null
[col 46] fixed_int_array::ARRAY
  [row 0] ARRAY [null, 2, 3]
  [row 1] ARRAY [4, 5, 6]
  [row 2] null
[col 47] fixed_varchar_array::ARRAY
  [row 0] ARRAY [a, null, c]
  [row 1] ARRAY [d, e, f]
  [row 2] null
[col 48] fixed_nested_int_array::ARRAY
  [row 0] ARRAY [ARRAY [null, 2, 3], null, ARRAY [null, 2, 3]]
  [row 1] ARRAY [ARRAY [4, 5, 6], ARRAY [null, 2, 3], ARRAY [4, 5, 6]]
  [row 2] null
[col 49] fixed_nested_varchar_array::ARRAY
  [row 0] ARRAY [ARRAY [a, null, c], null, ARRAY [a, null, c]]
  [row 1] ARRAY [ARRAY [d, e, f], ARRAY [a, null, c], ARRAY [d, e, f]]
  [row 2] null
[col 50] fixed_struct_array::ARRAY
  [row 0] ARRAY [STRUCT { "a": null, "b": null }, STRUCT { "a": 42, "b": 🦆🦆🦆🦆🦆🦆 }, STRUCT { "a": null, "b": null }]
  [row 1] ARRAY [STRUCT { "a": 42, "b": 🦆🦆🦆🦆🦆🦆 }, STRUCT { "a": null, "b": null }, STRUCT { "a": 42, "b": 🦆🦆🦆🦆🦆🦆 }]
  [row 2] null
[col 51] struct_of_fixed_array::STRUCT
  [row 0] STRUCT { "a": ARRAY [null, 2, 3], "b": ARRAY [a, null, c] }
  [row 1] STRUCT { "a": ARRAY [4, 5, 6], "b": ARRAY [d, e, f] }
  [row 2] null
[col 52] fixed_array_of_int_list::ARRAY
  [row 0] ARRAY [LIST [], LIST [42, 999, null, null, -42], LIST []]
  [row 1] ARRAY [LIST [42, 999, null, null, -42], LIST [], LIST [42, 999, null, null, -42]]
  [row 2] null
[col 53] list_of_fixed_int_array::LIST
  [row 0] LIST [ARRAY [null, 2, 3], ARRAY [4, 5, 6], ARRAY [null, 2, 3]]
  [row 1] LIST [ARRAY [4, 5, 6], ARRAY [null, 2, 3], ARRAY [4, 5, 6]]
  [row 2] null
```
