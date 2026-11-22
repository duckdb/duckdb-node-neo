import { assert, beforeAll, describe, test } from 'vitest';
import {
  ARRAY,
  BIGINT,
  BIGNUM,
  BIT,
  BLOB,
  BOOLEAN,
  DATE,
  DECIMAL,
  DOUBLE,
  DuckDBConnection,
  DuckDBDataChunk,
  DuckDBListValue,
  DuckDBListVector,
  DuckDBScalarFunction,
  DuckDBStructValue,
  DuckDBStructVector,
  DuckDBTableFunction,
  DuckDBType,
  DuckDBValue,
  DuckDBVariantValue,
  DuckDBVariantVector,
  ENUM,
  FLOAT,
  GEOMETRY,
  HUGEINT,
  INTEGER,
  INTERVAL,
  JSDuckDBValueConverter,
  LIST,
  MAP,
  SMALLINT,
  SQLNULL,
  STRUCT,
  TIME,
  TIMESTAMP,
  TIMESTAMP_MS,
  TIMESTAMP_NS,
  TIMESTAMPTZ_NS,
  TIMESTAMP_S,
  TIMETZ,
  TINYINT,
  TUPLE,
  UBIGINT,
  UHUGEINT,
  UINTEGER,
  UNION,
  USMALLINT,
  UTINYINT,
  UUID,
  VARCHAR,
  VARIANT,
  arrayValue,
  bitValue,
  blobValue,
  dateValue,
  decimalValue,
  geometryValue,
  intervalValue,
  listValue,
  mapValue,
  structValue,
  timeTZValue,
  timeValue,
  timestampMillisValue,
  timestampNanosValue,
  timestampSecondsValue,
  timestampValue,
  tupleValue,
  unionValue,
  uuidValue,
  variantValue,
  timestampTZNanosValue,
} from '../src';
import {
  assertColumns,
  assertValues,
  setDefaultTimezone,
  withConnection,
} from './util/testHelpers';

describe('VARIANT', () => {
  beforeAll(setDefaultTimezone);

  test('column type and SQL NULL row', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run(`select NULL::VARIANT as v`);
      assertColumns(result, [{ name: 'v', type: VARIANT }]);
      const chunk = await result.fetchChunk();
      assert.isDefined(chunk);
      if (chunk) {
        assertValues<DuckDBVariantValue, DuckDBVariantVector>(
          chunk,
          0,
          DuckDBVariantVector,
          [null],
        );
      }
    });
  });

  test('primitive scalars round-trip through VARIANT', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run(`
        select
          true::VARIANT                              as b_true,
          false::VARIANT                             as b_false,
          (-128)::TINYINT::VARIANT                   as i8,
          32767::SMALLINT::VARIANT                   as i16,
          (-2147483648)::INTEGER::VARIANT            as i32,
          9223372036854775807::BIGINT::VARIANT       as i64,
          170141183460469231731687303715884105727::HUGEINT::VARIANT as i128,
          255::UTINYINT::VARIANT                     as u8,
          65535::USMALLINT::VARIANT                  as u16,
          4294967295::UINTEGER::VARIANT              as u32,
          18446744073709551615::UBIGINT::VARIANT     as u64,
          340282366920938463463374607431768211455::UHUGEINT::VARIANT as u128,
          (1.5)::FLOAT::VARIANT                      as f32,
          (2.5)::DOUBLE::VARIANT                     as f64,
          'hello'::VARCHAR::VARIANT                  as s,
          '\\x00\\xFF'::BLOB::VARIANT                as bl,
          (123.45)::DECIMAL(5,2)::VARIANT            as dec_s,
          (12345678901234.5)::DECIMAL(18,1)::VARIANT as dec_64,
          (1234567890123456789012345.6789)::DECIMAL(38,4)::VARIANT as dec_128,
          DATE '2024-01-02'::VARIANT                 as d,
          TIME '12:34:56.789'::VARIANT               as t_us,
          TIMESTAMP '2024-01-02 03:04:05.678'::VARIANT as ts_us,
          TIMESTAMP_S '2024-01-02 03:04:05'::VARIANT  as ts_s,
          TIMESTAMP_MS '2024-01-02 03:04:05.678'::VARIANT as ts_ms,
          TIMESTAMP_NS '2024-01-02 03:04:05.678901234'::VARIANT as ts_ns,
          INTERVAL '3 months 4 days 5 minutes'::VARIANT as iv,
          '00112233-4455-6677-8899-aabbccddeeff'::UUID::VARIANT as uu
      `);
      assertColumns(result, [
        { name: 'b_true', type: VARIANT },
        { name: 'b_false', type: VARIANT },
        { name: 'i8', type: VARIANT },
        { name: 'i16', type: VARIANT },
        { name: 'i32', type: VARIANT },
        { name: 'i64', type: VARIANT },
        { name: 'i128', type: VARIANT },
        { name: 'u8', type: VARIANT },
        { name: 'u16', type: VARIANT },
        { name: 'u32', type: VARIANT },
        { name: 'u64', type: VARIANT },
        { name: 'u128', type: VARIANT },
        { name: 'f32', type: VARIANT },
        { name: 'f64', type: VARIANT },
        { name: 's', type: VARIANT },
        { name: 'bl', type: VARIANT },
        { name: 'dec_s', type: VARIANT },
        { name: 'dec_64', type: VARIANT },
        { name: 'dec_128', type: VARIANT },
        { name: 'd', type: VARIANT },
        { name: 't_us', type: VARIANT },
        { name: 'ts_us', type: VARIANT },
        { name: 'ts_s', type: VARIANT },
        { name: 'ts_ms', type: VARIANT },
        { name: 'ts_ns', type: VARIANT },
        { name: 'iv', type: VARIANT },
        { name: 'uu', type: VARIANT },
      ]);
      const chunk = await result.fetchChunk();
      assert.isDefined(chunk);
      if (!chunk) return;

      // 2024-01-02 = 1970-01-01 + 19724 days; ts is 1704164645 unix seconds.
      let i = 0;
      const expect = (v: DuckDBValue, t: DuckDBType) =>
        assertValues<DuckDBVariantValue, DuckDBVariantVector>(
          chunk,
          i++,
          DuckDBVariantVector,
          [variantValue(v, t)],
        );

      expect(true, BOOLEAN);
      expect(false, BOOLEAN);
      expect(-128, TINYINT);
      expect(32767, SMALLINT);
      expect(-2147483648, INTEGER);
      expect(9223372036854775807n, BIGINT);
      expect(170141183460469231731687303715884105727n, HUGEINT);
      expect(255, UTINYINT);
      expect(65535, USMALLINT);
      expect(4294967295, UINTEGER);
      expect(18446744073709551615n, UBIGINT);
      expect(340282366920938463463374607431768211455n, UHUGEINT);
      expect(1.5, FLOAT);
      expect(2.5, DOUBLE);
      expect('hello', VARCHAR);
      expect(blobValue(new Uint8Array([0x00, 0xff])), BLOB);
      expect(decimalValue(12345n, 5, 2), DECIMAL(5, 2));
      expect(decimalValue(123456789012345n, 18, 1), DECIMAL(18, 1));
      expect(decimalValue(12345678901234567890123456789n, 38, 4), DECIMAL(38, 4));
      expect(dateValue(19724), DATE);
      expect(
        timeValue(BigInt(((12 * 60 + 34) * 60 + 56) * 1_000_000 + 789_000)),
        TIME,
      );
      expect(timestampValue(1704164645678000n), TIMESTAMP);
      expect(timestampSecondsValue(1704164645n), TIMESTAMP_S);
      expect(timestampMillisValue(1704164645678n), TIMESTAMP_MS);
      expect(timestampNanosValue(1704164645678901234n), TIMESTAMP_NS);
      expect(intervalValue(3, 4, BigInt(5 * 60 * 1_000_000)), INTERVAL);
      expect(uuidValue(0x00112233_4455_6677_8899_aabbccddeeffn), UUID);
    });
  });

  test('TIMESTAMPTZ_NS decodes through VARIANT', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        `select '2024-01-02 03:04:05.678901234+00'::TIMESTAMPTZ_NS::VARIANT as v`,
      );
      assert.deepStrictEqual(reader.getRowObjects()[0].v, variantValue(
        timestampTZNanosValue(1704164645678901234n),
        TIMESTAMPTZ_NS,
      ));
    });
  });

  test('OBJECT decodes to DuckDBStructValue', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run(
        `select '{"a": 1, "b": "hi", "c": null}'::JSON::VARIANT as v`,
      );
      assertColumns(result, [{ name: 'v', type: VARIANT }]);
      const chunk = await result.fetchChunk();
      // JSON integers decode as UINT64 (variant tag); 1 becomes 1n with
      // UBIGINT type. The decoder reconstructs STRUCT field types from
      // each child's tag.
      assertValues<DuckDBVariantValue, DuckDBVariantVector>(
        chunk!,
        0,
        DuckDBVariantVector,
        [
          variantValue(
            structValue({ a: 1n, b: 'hi', c: null }),
            STRUCT({ a: UBIGINT, b: VARCHAR, c: SQLNULL }),
          ),
        ],
      );
    });
  });

  test('ARRAY decodes to DuckDBListValue with heterogeneous items', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run(
        `select '[1, "two", null, true]'::JSON::VARIANT as v`,
      );
      const chunk = await result.fetchChunk();
      // Non-null children carry differing tags so the list stays
      // LIST(VARIANT) with each non-null item wrapped. A null child
      // remains bare to match how a column-level LIST(VARIANT) decodes
      // a NULL element.
      assertValues<DuckDBVariantValue, DuckDBVariantVector>(
        chunk!,
        0,
        DuckDBVariantVector,
        [
          variantValue(
            listValue([
              variantValue(1n, UBIGINT),
              variantValue('two', VARCHAR),
              null,
              variantValue(true, BOOLEAN),
            ]),
            LIST(VARIANT),
          ),
        ],
      );
    });
  });

  test('nested OBJECT containing ARRAY exercises recursive walk', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run(
        `select '{"outer": [1, {"inner": "leaf"}, [null, true]]}'::JSON::VARIANT as v`,
      );
      const chunk = await result.fetchChunk();
      // The innermost [null, true] homogenizes to LIST(BOOLEAN) — null
      // is compatible with any sibling type. The outer array remains
      // heterogeneous (number / struct / list) so it's LIST(VARIANT).
      assertValues<DuckDBVariantValue, DuckDBVariantVector>(
        chunk!,
        0,
        DuckDBVariantVector,
        [
          variantValue(
            structValue({
              outer: listValue([
                variantValue(1n, UBIGINT),
                variantValue(
                  structValue({ inner: 'leaf' }),
                  STRUCT({ inner: VARCHAR }),
                ),
                variantValue(listValue([null, true]), LIST(BOOLEAN)),
              ]),
            }),
            STRUCT({ outer: LIST(VARIANT) }),
          ),
        ],
      );
    });
  });

  test('VARIANT nested inside a LIST', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run(`
        select [1::INTEGER::VARIANT, 'x'::VARCHAR::VARIANT, NULL::VARIANT] as items
      `);
      assertColumns(result, [
        { name: 'items', type: LIST(VARIANT) },
      ]);
      const chunk = await result.fetchChunk();
      assertValues<DuckDBListValue, DuckDBListVector>(
        chunk!,
        0,
        DuckDBListVector,
        [
          listValue([
            variantValue(1, INTEGER),
            variantValue('x', VARCHAR),
            null,
          ]),
        ],
      );
    });
  });

  test('VARIANT nested inside a STRUCT', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run(`
        select {x: 42::INTEGER::VARIANT, y: 'hello'::VARCHAR::VARIANT}
               ::STRUCT(x VARIANT, y VARIANT) as s
      `);
      assertColumns(result, [
        { name: 's', type: STRUCT({ x: VARIANT, y: VARIANT }) },
      ]);
      const chunk = await result.fetchChunk();
      assertValues<DuckDBStructValue, DuckDBStructVector>(
        chunk!,
        0,
        DuckDBStructVector,
        [
          structValue({
            x: variantValue(42, INTEGER),
            y: variantValue('hello', VARCHAR),
          }),
        ],
      );
    });
  });

  test('JSON converter recurses through VARIANT (heterogeneous list)', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(`
        select '{"a": 1, "b": [true, null, "x"], "c": 3.14}'::JSON::VARIANT as v
      `);
      assert.deepStrictEqual(reader.getRowObjectsJson(), [
        {
          v: {
            a: '1', // JSON converter renders BIGINTs as strings
            b: [true, null, 'x'],
            c: 3.14,
          },
        },
      ]);
    });
  });

  test('JS converter unwraps VARIANT to plain JS shape', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(`
        select '{"a": 1, "b": [true, null, "x"]}'::JSON::VARIANT as v
      `);
      assert.deepStrictEqual(reader.getRowObjectsJS(), [
        { v: { a: 1n, b: [true, null, 'x'] } },
      ]);
    });
  });

  test('JSON converter handles VARIANT nested inside a LIST', async () => {
    // Confirms the existing LIST converter dispatches per-element to the
    // VARIANT converter without needing variant-specific changes.
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(`
        select [1::INTEGER::VARIANT, 'x'::VARCHAR::VARIANT, NULL::VARIANT] as items
      `);
      assert.deepStrictEqual(reader.getRowObjectsJson(), [
        { items: [1, 'x', null] },
      ]);
    });
  });

  test('JSON converter handles VARIANT nested inside a STRUCT', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(`
        select {x: 42::INTEGER::VARIANT, y: 'hello'::VARCHAR::VARIANT}
               ::STRUCT(x VARIANT, y VARIANT) as s
      `);
      assert.deepStrictEqual(reader.getRowObjectsJson(), [
        { s: { x: 42, y: 'hello' } },
      ]);
    });
  });

  test('VARIANT-of-MAP unwraps via the variant converter', () => {
    // VARIANT decoding never produces MAP values, but if a caller wraps
    // a DuckDBMapValue in a DuckDBVariantValue (e.g. for testing or via
    // a future write path) the converter should still produce the same
    // {key, value}[] shape that objectArrayFromMapValue produces.
    const v = variantValue(
      mapValue([
        { key: 'a', value: 1 },
        { key: 'b', value: 2n },
      ]),
    );
    assert.deepStrictEqual(JSDuckDBValueConverter(v, VARIANT, JSDuckDBValueConverter), [
      { key: 'a', value: 1 },
      { key: 'b', value: 2n },
    ]);
  });

  test('VARIANT-of-UNION unwraps via the variant converter', () => {
    const v = variantValue(unionValue('s', 'hi'));
    assert.deepStrictEqual(JSDuckDBValueConverter(v, VARIANT, JSDuckDBValueConverter), {
      tag: 's',
      value: 'hi',
    });
  });

  test('binding a VARIANT parameter (primitive)', async () => {
    // Replaces the previous "not yet supported" test now that createValue
    // routes the cast through DuckDB. Both wrapped and unwrapped inputs
    // round-trip: the wrapper carries an exact `.type`; the unwrapped
    // value falls back to `typeForValue` inference.
    await withConnection(async (connection) => {
      const prepared = await connection.prepare(
        'select ?::VARIANT as v, ?::VARIANT as u',
      );
      prepared.bind([variantValue(42, INTEGER), 'hello'], [VARIANT, VARIANT]);
      const reader = await prepared.runAndReadAll();
      assert.deepStrictEqual(reader.getRowObjects(), [
        {
          v: variantValue(42, INTEGER),
          // INTEGER fits in JS number; typeForValue infers INTEGER for
          // small numbers, but 'hello' is a string → VARCHAR.
          u: variantValue('hello', VARCHAR),
        },
      ]);
    });
  });

  // Extended primitive tags (BIT, BIGNUM, TIME_TZ, GEOMETRY): these share
  // length-prefixed encoding (BIT/BIGNUM/GEOMETRY) or use signed/unsigned
  // variants of integer reads (TIME_TZ), and weren't exercised by the main
  // scalar round-trip test.

  test('BITSTRING round-trips through VARIANT', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run(
        `select '0101'::BIT::VARIANT as v`,
      );
      const chunk = await result.fetchChunk();
      assertValues<DuckDBVariantValue, DuckDBVariantVector>(
        chunk!,
        0,
        DuckDBVariantVector,
        [variantValue(bitValue('0101'), BIT)],
      );
    });
  });

  test('BIGNUM round-trips through VARIANT', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run(
        `select '1234567890123456789012345'::BIGNUM::VARIANT as v`,
      );
      const chunk = await result.fetchChunk();
      assertValues<DuckDBVariantValue, DuckDBVariantVector>(
        chunk!,
        0,
        DuckDBVariantVector,
        [variantValue(1234567890123456789012345n, BIGNUM)],
      );
    });
  });

  test('TIME WITH TIME ZONE round-trips through VARIANT', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run(
        `select TIME WITH TIME ZONE '12:34:56.789+05:30'::VARIANT as v`,
      );
      const chunk = await result.fetchChunk();
      assertValues<DuckDBVariantValue, DuckDBVariantVector>(
        chunk!,
        0,
        DuckDBVariantVector,
        [
          variantValue(
            timeTZValue(45296789000n, 5 * 3600 + 30 * 60),
            TIMETZ,
          ),
        ],
      );
    });
  });

  test('GEOMETRY round-trips through VARIANT', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run(
        `select 'POINT(1 2)'::GEOMETRY::VARIANT as v`,
      );
      const chunk = await result.fetchChunk();
      // WKB for POINT(1, 2), little-endian: 1 | 01 00 00 00 | 8-byte LE
      // double 1.0 | 8-byte LE double 2.0.
      const pointWkb = new Uint8Array([
        0x01, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
      ]);
      assertValues<DuckDBVariantValue, DuckDBVariantVector>(
        chunk!,
        0,
        DuckDBVariantVector,
        [variantValue(geometryValue(pointWkb), GEOMETRY)],
      );
    });
  });

  // Regression tests for code-review fixes.

  test('UINTEGER max routes through the JS / JSON converters', async () => {
    // The decoder records the exact UINTEGER type on the wrapper, so the
    // converter dispatches via UINTEGER → numberFromValue and the value
    // stays a JS number. (Without the type hint, `typeForValue` would
    // infer BIGINT for a number > INT32 max and the variant walker
    // would coerce to bigint as a fallback — exercised separately for
    // unwrapped numeric values.)
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        `select 4294967295::UINTEGER::VARIANT as u`,
      );
      assert.deepStrictEqual(reader.getRowObjectsJS(), [
        { u: 4294967295 },
      ]);
      assert.deepStrictEqual(reader.getRowObjectsJson(), [
        { u: 4294967295 },
      ]);
    });
  });

  test('setItem(null) on a VARIANT vector writes a NULL', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run(
        `select 42::INTEGER::VARIANT as v`,
      );
      const chunk = await result.fetchChunk();
      const vec = chunk!.getColumnVector(0) as DuckDBVariantVector;
      vec.setItem(0, null);
      vec.flush();
      assert.strictEqual(vec.getItem(0), null);
    });
  });

  test('setItem on a result VARIANT vector writes a value', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run(
        `select 42::INTEGER::VARIANT as v from range(2)`,
      );
      const chunk = await result.fetchChunk();
      const vec = chunk!.getColumnVector(0) as DuckDBVariantVector;
      const value = variantValue(
        structValue({ a: 'a string longer than the data buffer starts out' }),
        STRUCT({ a: VARCHAR }),
      );
      vec.setItem(0, value);
      vec.flush();
      // A written row reads back as what it was given; the others still
      // decode, though flushing the lists moved their entries.
      assert.strictEqual(vec.getItem(0), value);
      assert.deepStrictEqual(vec.getItem(1), variantValue(42, INTEGER));
      // And DuckDB reads the written row.
      await connection.run(`create table t (v VARIANT)`);
      const appender = await connection.createAppender('t');
      appender.appendDataChunk(chunk!);
      appender.closeSync();
      const reader = await connection.runAndReadAll(
        `select v::JSON::VARCHAR as v from t`,
      );
      assert.deepStrictEqual(reader.getRowsJS(), [
        ['{"a":"a string longer than the data buffer starts out"}'],
        ['42'],
      ]);
    });
  });

  test('STRUCT(VARIANT, INTEGER) can be read end-to-end', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(`
        select {x: 42::INTEGER::VARIANT, y: 7::INTEGER}
               ::STRUCT(x VARIANT, y INTEGER) as s
      `);
      assert.deepStrictEqual(reader.getRowObjectsJS(), [
        { s: { x: 42, y: 7 } },
      ]);
    });
  });

  // Binding / appending VARIANTs. DuckDB has no `duckdb_create_variant`
  // primitive; instead the variant target type triggers an implicit cast
  // from whatever Value we hand it. Four routes get exercised below:
  //   1. Wrapper round-trip via `bindVariant` / `appendVariant`.
  //   2. Generic `bindValue` / `appendValue` with target=VARIANT.
  //   3. Generic `bindValue` / `appendValue` with a *non-VARIANT* target
  //      (DuckDB performs the cast on its side).
  //   4. Type-specific append methods (`appendInteger`, `appendVarchar`)
  //      that bypass `createValue` entirely.

  test('bindVariant round-trips a primitive VARIANT (path 1)', async () => {
    await withConnection(async (connection) => {
      const r1 = await connection.runAndReadAll(
        `select 42::INTEGER::VARIANT as v`,
      );
      const decoded = r1.getRowObjects()[0].v as DuckDBVariantValue;
      const prepared = await connection.prepare(`select ?::VARIANT as v`);
      prepared.bindVariant(1, decoded);
      const r2 = await prepared.runAndReadAll();
      assert.deepStrictEqual(r2.getRowObjects(), [
        { v: variantValue(42, INTEGER) },
      ]);
    });
  });

  test('bindVariant round-trips a STRUCT VARIANT (path 1)', async () => {
    await withConnection(async (connection) => {
      const r1 = await connection.runAndReadAll(
        `select '{"a": 1, "b": "hi"}'::JSON::VARIANT as v`,
      );
      const decoded = r1.getRowObjects()[0].v as DuckDBVariantValue;
      const prepared = await connection.prepare(`select ?::VARIANT as v`);
      prepared.bindVariant(1, decoded);
      const r2 = await prepared.runAndReadAll();
      assert.deepStrictEqual(r2.getRowObjects(), [
        {
          v: variantValue(
            structValue({ a: 1n, b: 'hi' }),
            STRUCT({ a: UBIGINT, b: VARCHAR }),
          ),
        },
      ]);
    });
  });

  test('bindVariant round-trips a homogeneous ARRAY VARIANT (path 1)', async () => {
    // A JSON array whose elements all share a tag becomes
    // LIST(commonType) with bare items, which round-trips cleanly.
    // (Heterogeneous arrays decode as LIST(VARIANT) with wrapped items
    // and currently cannot be re-bound — see the limitation note.)
    await withConnection(async (connection) => {
      const r1 = await connection.runAndReadAll(
        `select '[1, 2, 3]'::JSON::VARIANT as v`,
      );
      const decoded = r1.getRowObjects()[0].v as DuckDBVariantValue;
      const prepared = await connection.prepare(`select ?::VARIANT as v`);
      prepared.bindVariant(1, decoded);
      const r2 = await prepared.runAndReadAll();
      assert.deepStrictEqual(r2.getRowObjects(), [
        {
          v: variantValue(
            listValue([1n, 2n, 3n]),
            LIST(UBIGINT),
          ),
        },
      ]);
    });
  });

  test('bindValue with explicit VARIANT target (path 2)', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare(
        `select ?::VARIANT as v, ?::VARIANT as u`,
      );
      // Mixed: a raw number (typeForValue → INTEGER) and a raw string
      // (typeForValue → VARCHAR).
      prepared.bindValue(1, 42, VARIANT);
      prepared.bindValue(2, 'hello', VARIANT);
      const reader = await prepared.runAndReadAll();
      assert.deepStrictEqual(reader.getRowObjects(), [
        {
          v: variantValue(42, INTEGER),
          u: variantValue('hello', VARCHAR),
        },
      ]);
    });
  });

  test('bindValue with explicit VARIANT target accepts null (path 2)', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare(`select ?::VARIANT as v`);
      prepared.bindValue(1, null, VARIANT);
      const reader = await prepared.runAndReadAll();
      assert.deepStrictEqual(reader.getRowObjects(), [{ v: null }]);
    });
  });

  test('bindValue with non-VARIANT target casts on DuckDB side (path 3)', async () => {
    // The target column is VARIANT (via the `?::VARIANT` cast) but the
    // bound Value is INTEGER / VARCHAR / STRUCT. DuckDB performs the
    // cast.
    await withConnection(async (connection) => {
      const prepared = await connection.prepare(
        `select ?::VARIANT as i, ?::VARIANT as s, ?::VARIANT as o`,
      );
      prepared.bindValue(1, 42, INTEGER);
      prepared.bindValue(2, 'hello', VARCHAR);
      prepared.bindValue(
        3,
        structValue({ a: 1, b: 'x' }),
        STRUCT({ a: INTEGER, b: VARCHAR }),
      );
      const reader = await prepared.runAndReadAll();
      assert.deepStrictEqual(reader.getRowObjects(), [
        {
          i: variantValue(42, INTEGER),
          s: variantValue('hello', VARCHAR),
          o: variantValue(
            structValue({ a: 1, b: 'x' }),
            STRUCT({ a: INTEGER, b: VARCHAR }),
          ),
        },
      ]);
    });
  });

  test('appendVariant + appendValue + type-specific append (paths 1, 2, 3, 4)', async () => {
    await withConnection(async (connection) => {
      await connection.run(`CREATE TABLE t (v VARIANT)`);
      const appender = await connection.createAppender('t');
      // Path 1: appendVariant with a wrapped value carrying its exact type.
      appender.appendVariant(variantValue(1, INTEGER));
      appender.endRow();
      // Path 2: appendValue with target=VARIANT, raw input.
      appender.appendValue('hello', VARIANT);
      appender.endRow();
      // Path 3: appendValue with target=INTEGER against a VARIANT column.
      appender.appendValue(3, INTEGER);
      appender.endRow();
      // Path 4: type-specific append bypasses createValue.
      appender.appendInteger(4);
      appender.endRow();
      // Null.
      appender.appendNull();
      appender.endRow();
      appender.closeSync();

      const reader = await connection.runAndReadAll(`select v from t`);
      assert.deepStrictEqual(reader.getRowObjects(), [
        { v: variantValue(1, INTEGER) },
        { v: variantValue('hello', VARCHAR) },
        { v: variantValue(3, INTEGER) },
        { v: variantValue(4, INTEGER) },
        { v: null },
      ]);
    });
  });

  test('VARIANT logical types, alone and nested', () => {
    for (const type of [VARIANT, LIST(VARIANT), STRUCT({ v: VARIANT })]) {
      assert.deepStrictEqual(type.toLogicalType().asType(), type);
    }
  });

  test('data chunk with VARIANT columns', () => {
    const chunk = DuckDBDataChunk.create([VARIANT, LIST(VARIANT)], 3);
    assert.strictEqual(chunk.columnCount, 2);
    assert.strictEqual(chunk.rowCount, 3);
  });

  test('scalar function with a VARIANT parameter', async () => {
    await withConnection(async (connection) => {
      connection.registerScalarFunction(
        DuckDBScalarFunction.create({
          name: 'describe_variant',
          parameterTypes: [VARIANT],
          returnType: VARCHAR,
          mainFunction: (_info, input, output) => {
            const vector = input.getColumnVector(0);
            for (let rowIndex = 0; rowIndex < input.rowCount; rowIndex++) {
              output.setItem(rowIndex, String(vector.getItem(rowIndex)));
            }
            output.flush();
          },
        }),
      );
      const reader = await connection.runAndReadAll(`
        select describe_variant(1::INTEGER::VARIANT) as a,
               describe_variant('two'::VARIANT) as b,
               describe_variant([1, 2]::VARIANT) as c
      `);
      assert.deepStrictEqual(reader.getRowObjects(), [
        { a: '1', b: "'two'", c: '[1, 2]' },
      ]);
    });
  });

  test('scalar function returning VARIANT', async () => {
    await withConnection(async (connection) => {
      connection.registerScalarFunction(
        DuckDBScalarFunction.create({
          name: 'to_variant',
          parameterTypes: [INTEGER],
          returnType: VARIANT,
          mainFunction: (_info, input, output) => {
            for (let rowIndex = 0; rowIndex < input.rowCount; rowIndex++) {
              output.setItem(rowIndex, null);
            }
            output.flush();
          },
        }),
      );
      const reader = await connection.runAndReadAll(
        `select to_variant(i::INTEGER) as v from range(3) t(i)`,
      );
      assert.deepStrictEqual(reader.columnType(0), VARIANT);
      assert.deepStrictEqual(reader.getRowObjects(), [
        { v: null },
        { v: null },
        { v: null },
      ]);
    });
  });

  test('table function with a VARIANT result column', async () => {
    await withConnection(async (connection) => {
      connection.registerTableFunction(
        DuckDBTableFunction.create({
          name: 'variant_table',
          bindFunction: (info) => {
            info.addResultColumn('v', VARIANT);
            info.setBindData({});
          },
          initFunction: (info) => {
            info.setInitData({ done: false });
          },
          mainFunction: (info, output) => {
            const initData = info.initData as { done: boolean };
            if (initData.done) {
              output.rowCount = 0;
              return;
            }
            initData.done = true;
            output.rowCount = 2;
            output.setRows([[null], [null]]);
          },
        }),
      );
      const reader = await connection.runAndReadAll(
        `select * from variant_table()`,
      );
      assert.deepStrictEqual(reader.columnType(0), VARIANT);
      assert.deepStrictEqual(reader.getRowObjects(), [
        { v: null },
        { v: null },
      ]);
    });
  });

  test('appendDataChunk with VARIANT NULLs, alone and nested', async () => {
    await withConnection(async (connection) => {
      await connection.run(
        `create table t (i INTEGER, v VARIANT, s STRUCT(v VARIANT))`,
      );
      const appender = await connection.createAppender('t');
      const chunk = DuckDBDataChunk.create([
        INTEGER,
        VARIANT,
        STRUCT({ v: VARIANT }),
      ]);
      chunk.setRows([
        [1, null, structValue({ v: null })],
        [2, null, null],
      ]);
      appender.appendDataChunk(chunk);
      appender.closeSync();
      const reader = await connection.runAndReadAll(`select * from t`);
      assert.deepStrictEqual(reader.getRowObjects(), [
        { i: 1, v: null, s: structValue({ v: null }) },
        { i: 2, v: null, s: null },
      ]);
    });
  });

  test('bindList with a LIST(VARIANT) type', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare(`select ? as l`);
      prepared.bindList(
        1,
        [variantValue(1, INTEGER), variantValue('two', VARCHAR)],
        LIST(VARIANT),
      );
      const reader = await prepared.runAndReadAll();
      assert.deepStrictEqual(reader.columnType(0), LIST(VARIANT));
      assert.deepStrictEqual(reader.getRowObjects(), [
        {
          l: listValue([
            variantValue(1, INTEGER),
            variantValue('two', VARCHAR),
          ]),
        },
      ]);
    });
  });

  test('VARIANT column types of a closed result', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run(
        `select 1::VARIANT as v, [1::VARIANT] as l`,
      );
      result.closeSync();
      assert.deepStrictEqual(result.columnType(0), VARIANT);
      assert.deepStrictEqual(result.columnType(1), LIST(VARIANT));
    });
  });

  test('heterogeneous LIST(VARIANT) round-trips', async () => {
    // A decoded `LIST(VARIANT)` (which wraps each non-null element to
    // preserve mixed types) is bound as a list value with a VARIANT element
    // type.
    await withConnection(async (connection) => {
      const r1 = await connection.runAndReadAll(
        `select '[1, "two"]'::JSON::VARIANT as v`,
      );
      const decoded = r1.getRowObjects()[0].v as DuckDBVariantValue;
      // Confirm the decoder produced the heterogeneous shape.
      assert.deepStrictEqual(decoded.type, LIST(VARIANT));
      const prepared = await connection.prepare(`select ?::VARIANT as v`);
      prepared.bindVariant(1, decoded);
      const r2 = await prepared.runAndReadAll();
      assert.deepStrictEqual(r2.getRowObjects()[0].v, decoded);
    });
  });

  // Round-trip coverage for the SQLNULL-compatible homogenize logic.

  test('[1, 2, null] homogenizes to LIST(UBIGINT) with a bare null', async () => {
    await withConnection(async (connection) => {
      const r1 = await connection.runAndReadAll(
        `select '[1, 2, null]'::JSON::VARIANT as v`,
      );
      const decoded = r1.getRowObjects()[0].v as DuckDBVariantValue;
      assert.deepStrictEqual(decoded, variantValue(
        listValue([1n, 2n, null]),
        LIST(UBIGINT),
      ));
      // And round-trips back through bindVariant.
      const p = await connection.prepare(`select ?::VARIANT as v`);
      p.bindVariant(1, decoded);
      const r2 = await p.runAndReadAll();
      assert.deepStrictEqual(r2.getRowObjects()[0].v, decoded);
    });
  });

  test('[null, null] collapses to LIST(SQLNULL) and round-trips', async () => {
    await withConnection(async (connection) => {
      const r1 = await connection.runAndReadAll(
        `select '[null, null]'::JSON::VARIANT as v`,
      );
      const decoded = r1.getRowObjects()[0].v as DuckDBVariantValue;
      assert.deepStrictEqual(decoded, variantValue(
        listValue([null, null]),
        LIST(SQLNULL),
      ));
      const p = await connection.prepare(`select ?::VARIANT as v`);
      p.bindVariant(1, decoded);
      const r2 = await p.runAndReadAll();
      assert.deepStrictEqual(r2.getRowObjects()[0].v, decoded);
    });
  });

  test('empty [] decodes as LIST(SQLNULL) and round-trips', async () => {
    await withConnection(async (connection) => {
      const r1 = await connection.runAndReadAll(
        `select '[]'::JSON::VARIANT as v`,
      );
      const decoded = r1.getRowObjects()[0].v as DuckDBVariantValue;
      assert.deepStrictEqual(decoded, variantValue(listValue([]), LIST(SQLNULL)));
      const p = await connection.prepare(`select ?::VARIANT as v`);
      p.bindVariant(1, decoded);
      const r2 = await p.runAndReadAll();
      assert.deepStrictEqual(r2.getRowObjects()[0].v, decoded);
    });
  });

  test('empty {} decodes as STRUCT() and round-trips', async () => {
    await withConnection(async (connection) => {
      const r1 = await connection.runAndReadAll(
        `select '{}'::JSON::VARIANT as v`,
      );
      const decoded = r1.getRowObjects()[0].v as DuckDBVariantValue;
      assert.deepStrictEqual(decoded, variantValue(structValue({}), STRUCT({})));
      const p = await connection.prepare(`select ?::VARIANT as v`);
      p.bindVariant(1, decoded);
      const r2 = await p.runAndReadAll();
      assert.deepStrictEqual(r2.getRowObjects()[0].v, decoded);
    });
  });

  test('STRUCT VARIANT containing a SQLNULL-typed field round-trips', async () => {
    // Covers the case where a struct field is JSON-null; the decoder
    // captures the field type as SQLNULL and the bind path must accept
    // it (top-level null shortcut in createValue handles the inner null).
    await withConnection(async (connection) => {
      const r1 = await connection.runAndReadAll(
        `select '{"a": 1, "b": null}'::JSON::VARIANT as v`,
      );
      const decoded = r1.getRowObjects()[0].v as DuckDBVariantValue;
      const p = await connection.prepare(`select ?::VARIANT as v`);
      p.bindVariant(1, decoded);
      const r2 = await p.runAndReadAll();
      assert.deepStrictEqual(r2.getRowObjects()[0].v, decoded);
    });
  });

  test('bindVariant accepts null (short-circuits to bindNull)', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare(`select ?::VARIANT as v`);
      prepared.bindVariant(1, null);
      const reader = await prepared.runAndReadAll();
      assert.deepStrictEqual(reader.getRowObjects(), [{ v: null }]);
    });
  });

  test('mismatched DuckDBVariantValue wrapper produces a VARIANT-context error', async () => {
    // `variantValue('hello', INTEGER)` claims the inner value is an
    // integer but it's actually a string. The error message should
    // surface the VARIANT context to aid debugging.
    await withConnection(async (connection) => {
      const prepared = await connection.prepare(`select ?::VARIANT as v`);
      assert.throws(
        () => prepared.bindVariant(1, variantValue('hello', INTEGER)),
        /Failed to create VARIANT value.*input is not a number/,
      );
    });
  });

  // Writing VARIANT values through vectors. Each test checks the written
  // rows with the decoder and with DuckDB itself, by comparing them against
  // the same values cast to VARIANT in SQL.

  /** Appends `values` to a new table `name(i INTEGER, v VARIANT)` via a chunk. */
  async function appendVariantChunk(
    connection: DuckDBConnection,
    name: string,
    values: readonly (DuckDBVariantValue | null)[],
  ) {
    await connection.run(`create table ${name} (i INTEGER, v VARIANT)`);
    const appender = await connection.createAppender(name);
    const chunk = DuckDBDataChunk.create([INTEGER, VARIANT], values.length);
    chunk.setRows(values.map((v, i) => [i, v]));
    appender.appendDataChunk(chunk);
    appender.closeSync();
  }

  /**
   * Asserts that DuckDB sees `actual.v` and `expected.v` as the same VARIANT
   * in every row, by type, by text, and as JSON.
   */
  async function assertSameVariants(
    connection: DuckDBConnection,
    actual: string,
    expected: string,
  ) {
    const reader = await connection.runAndReadAll(`
      select e.i,
             variant_typeof(a.v) as actual_type,
             variant_typeof(e.v) as expected_type,
             a.v::VARCHAR as actual_text,
             e.v::VARCHAR as expected_text,
             a.v::JSON::VARCHAR as actual_json,
             e.v::JSON::VARCHAR as expected_json
      from ${expected} e left join ${actual} a using (i)
      order by e.i
    `);
    const rows = reader.getRowObjectsJS();
    assert.isNotEmpty(rows);
    for (const row of rows) {
      assert.deepStrictEqual(
        [row.actual_type, row.actual_text, row.actual_json],
        [row.expected_type, row.expected_text, row.expected_json],
        `row ${row.i}`,
      );
    }
  }

  test('vector writes round-trip what a SQL cast to VARIANT produces', async () => {
    const sources = [
      `true`,
      `false`,
      `(-128)::TINYINT`,
      `32767::SMALLINT`,
      `(-2147483648)::INTEGER`,
      `9223372036854775807::BIGINT`,
      `170141183460469231731687303715884105727::HUGEINT`,
      `255::UTINYINT`,
      `65535::USMALLINT`,
      `4294967295::UINTEGER`,
      `18446744073709551615::UBIGINT`,
      `340282366920938463463374607431768211455::UHUGEINT`,
      `(1.5)::FLOAT`,
      `(2.5)::DOUBLE`,
      `'hello'`,
      `repeat('long string ', 100)`,
      `'\\x00\\xFF'::BLOB`,
      `(123.4)::DECIMAL(4,1)`,
      `(123.45)::DECIMAL(5,2)`,
      `(12345678901234.5)::DECIMAL(18,1)`,
      `(1234567890123456789012345.6789)::DECIMAL(38,4)`,
      `DATE '2024-01-02'`,
      `TIME '12:34:56.789'`,
      `'12:34:56.123456789'::TIME_NS`,
      `TIME WITH TIME ZONE '12:34:56.789+05:30'`,
      `TIMESTAMP '2024-01-02 03:04:05.678'`,
      `TIMESTAMP_S '2024-01-02 03:04:05'`,
      `TIMESTAMP_MS '2024-01-02 03:04:05.678'`,
      `TIMESTAMP_NS '2024-01-02 03:04:05.678901234'`,
      `'2024-01-02 03:04:05.678+00'::TIMESTAMPTZ`,
      `'2024-01-02 03:04:05.678901234+00'::TIMESTAMPTZ_NS`,
      `INTERVAL '3 months 4 days 5 minutes'`,
      `'00112233-4455-6677-8899-aabbccddeeff'::UUID`,
      `'0101'::BIT`,
      `'-1234567890123456789012345'::BIGNUM`,
      `'POINT(1 2)'::GEOMETRY`,
      `{'a': 1, 'b': [1, 2, 3], 'c': {'d': 'x'}}`,
      `{'xs': [{'y': 1}, {'y': 2}], 'y': 3}`,
      `[[1, 2], [], [3]]`,
      `'[1, "two", null, {"k": [true]}]'::JSON`,
      `'{"a": 1, "b": null}'::JSON`,
      `'[1, 2, null]'::JSON`,
      `'[]'::JSON`,
      `'{}'::JSON`,
    ];
    await withConnection(async (connection) => {
      await connection.run(`
        create table src as
        select * from (values ${sources
          .map((s, i) => `(${i}, (${s})::VARIANT)`)
          .join(', ')}) t(i, v)
      `);
      const read = await connection.runAndReadAll(
        `select v from src order by i`,
      );
      const decoded = read
        .getRowObjects()
        .map((row) => row.v as DuckDBVariantValue);
      await appendVariantChunk(connection, 'dst', decoded);
      const reread = await connection.runAndReadAll(
        `select v from dst order by i`,
      );
      assert.deepStrictEqual(
        reread.getRowObjects().map((row) => row.v),
        decoded,
      );
      await assertSameVariants(connection, 'dst', 'src');
    });
  });

  test('vector writes encode nested types the way DuckDB casts them', async () => {
    const mood = ENUM(['sad', 'ok', 'happy']);
    const cases: [string, DuckDBVariantValue][] = [
      [
        `MAP {'k1': 1, 'k2': NULL}`,
        variantValue(
          mapValue([
            { key: 'k1', value: 1 },
            { key: 'k2', value: null },
          ]),
          MAP(VARCHAR, INTEGER),
        ),
      ],
      [
        `union_value(n := 42)::UNION(s VARCHAR, n INTEGER)`,
        variantValue(
          unionValue('n', 42),
          UNION({ s: VARCHAR, n: INTEGER }),
        ),
      ],
      [
        `'happy'::ENUM('sad', 'ok', 'happy')`,
        variantValue('happy', mood),
      ],
      [
        `[1, 2, 3]::INTEGER[3]`,
        variantValue(arrayValue([1, 2, 3]), ARRAY(INTEGER, 3)),
      ],
      [
        `row('a', 1)`,
        variantValue(tupleValue(['a', 1]), TUPLE([VARCHAR, INTEGER])),
      ],
      [
        `{'a': NULL::INTEGER, 'b': [NULL::INTEGER, 1]}`,
        variantValue(
          structValue({ a: null, b: listValue([null, 1]) }),
          STRUCT({ a: INTEGER, b: LIST(INTEGER) }),
        ),
      ],
      [
        `[1::VARIANT, 'two'::VARIANT, [3]::VARIANT]`,
        variantValue(
          listValue([
            variantValue(1, INTEGER),
            variantValue('two'),
            variantValue(listValue([3]), LIST(INTEGER)),
          ]),
          LIST(VARIANT),
        ),
      ],
      // Without a type, `typeForValue` picks one, as for `appendVariant`.
      [`42::INTEGER`, variantValue(42)],
      [`5000000000::BIGINT`, variantValue(5000000000)],
      [`'s'`, variantValue('s')],
      [`{'k': 1.5}`, variantValue(structValue({ k: 1.5 }))],
    ];
    await withConnection(async (connection) => {
      await connection.run(`
        create table expected as
        select * from (values ${cases
          .map(([sql], i) => `(${i}, (${sql})::VARIANT)`)
          .join(', ')}) t(i, v)
      `);
      await appendVariantChunk(
        connection,
        'actual',
        cases.map(([, value]) => value),
      );
      await assertSameVariants(connection, 'actual', 'expected');
    });
  });

  test('a NULL is a NULL row at the root and VARIANT_NULL inside', async () => {
    await withConnection(async (connection) => {
      await appendVariantChunk(connection, 't', [
        null,
        variantValue(null),
        variantValue(variantValue(null)),
        variantValue(unionValue('n', null), UNION({ n: INTEGER })),
        variantValue(listValue([null]), LIST(INTEGER)),
        variantValue(structValue({ a: null }), STRUCT({ a: VARCHAR })),
      ]);
      const reader = await connection.runAndReadAll(
        `select v is null as is_null, v::JSON::VARCHAR as json
         from t order by i`,
      );
      assert.deepStrictEqual(reader.getRowsJS(), [
        [true, null],
        [true, null],
        [true, null],
        [true, null],
        [false, '[null]'],
        [false, '{"a":null}'],
      ]);
    });
  });

  test('vector writes of VARIANT nested in other types', async () => {
    await withConnection(async (connection) => {
      await connection.run(`
        create table t (
          s STRUCT(v VARIANT),
          l VARIANT[],
          m MAP(VARCHAR, VARIANT),
          u UNION(v VARIANT, n INTEGER),
          a VARIANT[2]
        )
      `);
      const object = variantValue(
        structValue({ x: listValue([1, 2]) }),
        STRUCT({ x: LIST(INTEGER) }),
      );
      const appender = await connection.createAppender('t');
      const chunk = DuckDBDataChunk.create([
        STRUCT({ v: VARIANT }),
        LIST(VARIANT),
        MAP(VARCHAR, VARIANT),
        UNION({ v: VARIANT, n: INTEGER }),
        ARRAY(VARIANT, 2),
      ]);
      chunk.setRows([
        [
          structValue({ v: object }),
          listValue([variantValue(1, INTEGER), null, object]),
          mapValue([{ key: 'k', value: variantValue('v', VARCHAR) }]),
          unionValue('v', object),
          arrayValue([object, variantValue(true, BOOLEAN)]),
        ],
      ]);
      appender.appendDataChunk(chunk);
      appender.closeSync();
      const reader = await connection.runAndReadAll(`
        select s.v::JSON::VARCHAR as s,
               l::JSON::VARCHAR as l,
               m['k']::VARCHAR as m,
               variant_extract(u.v, 'x')::JSON::VARCHAR as u,
               a[2]::BOOLEAN as a
        from t
      `);
      assert.deepStrictEqual(reader.getRowObjectsJS(), [
        {
          s: '{"x":[1,2]}',
          l: '[1,null,{"x":[1,2]}]',
          m: 'v',
          u: '[1,2]',
          a: true,
        },
      ]);
    });
  });

  test('a scalar function and a table function return VARIANT values', async () => {
    await withConnection(async (connection) => {
      connection.registerScalarFunction(
        DuckDBScalarFunction.create({
          name: 'wrap_variant',
          parameterTypes: [INTEGER],
          returnType: VARIANT,
          mainFunction: (_info, input, output) => {
            const vector = input.getColumnVector(0);
            for (let rowIndex = 0; rowIndex < input.rowCount; rowIndex++) {
              const i = vector.getItem(rowIndex) as number;
              output.setItem(
                rowIndex,
                variantValue(
                  structValue({ i, tags: listValue(['t'.repeat(i)]) }),
                  STRUCT({ i: INTEGER, tags: LIST(VARCHAR) }),
                ),
              );
            }
            output.flush();
          },
        }),
      );
      connection.registerTableFunction(
        DuckDBTableFunction.create({
          name: 'variant_rows',
          bindFunction: (info) => {
            info.addResultColumn('v', VARIANT);
            info.setBindData({});
          },
          initFunction: (info) => {
            info.setInitData({ done: false });
          },
          mainFunction: (info, output) => {
            const initData = info.initData as { done: boolean };
            if (initData.done) {
              output.rowCount = 0;
              return;
            }
            initData.done = true;
            output.rowCount = 2;
            output.setRows([[variantValue('a', VARCHAR)], [variantValue(2)]]);
          },
        }),
      );
      const scalar = await connection.runAndReadAll(`
        select variant_extract(v, 'i')::INTEGER as i,
               variant_extract(v, 'tags')::JSON::VARCHAR as tags
        from (select wrap_variant(i::INTEGER) as v from range(1, 4) t(i))
      `);
      assert.deepStrictEqual(scalar.getRowsJS(), [
        [1, '["t"]'],
        [2, '["tt"]'],
        [3, '["ttt"]'],
      ]);
      const table = await connection.runAndReadAll(
        `select variant_typeof(v) as t, v::VARCHAR as s from variant_rows()`,
      );
      assert.deepStrictEqual(table.getRowsJS(), [
        ['VARCHAR', 'a'],
        ['INT32', '2'],
      ]);
    });
  });

  test('vector writes fill a full chunk of differently sized rows', async () => {
    const values = Array.from({ length: 2048 }, (_, i) =>
      i % 7 === 0
        ? null
        : variantValue(
            structValue({
              n: i,
              xs: listValue(Array.from({ length: i % 5 }, (_, j) => j)),
              s: 's'.repeat(i % 300),
            }),
            STRUCT({ n: INTEGER, xs: LIST(INTEGER), s: VARCHAR }),
          ),
    );
    await withConnection(async (connection) => {
      await appendVariantChunk(connection, 't', values);
      const reader = await connection.runAndReadAll(
        `select v::JSON::VARCHAR as v from t order by i`,
      );
      assert.deepStrictEqual(
        reader.getRowsJS().map(([json]) => json),
        values.map((_, i) =>
          i % 7 === 0
            ? null
            : // DuckDB writes an object's keys in sorted order.
              JSON.stringify({
                n: i,
                s: 's'.repeat(i % 300),
                xs: Array.from({ length: i % 5 }, (_, j) => j),
              }),
        ),
      );
    });
  });

  test('vector writes refuse a value that does not fit its type', () => {
    const chunk = DuckDBDataChunk.create([VARIANT], 1);
    const cases: [DuckDBVariantValue, RegExp][] = [
      [
        variantValue('hello', INTEGER),
        /Failed to write VARIANT value \(inner type INTEGER\): input is not a number/,
      ],
      [variantValue(300, TINYINT), /number out of int8 range/],
      [variantValue(-1, UINTEGER), /number out of uint32 range/],
      [variantValue(1.5, BIGINT), /number is not an integer/],
      [
        variantValue(structValue({ a: 1 }), STRUCT({ a: INTEGER, b: INTEGER })),
        /STRUCT entry 'b' is missing from input/,
      ],
      [
        variantValue(arrayValue([1]), ARRAY(INTEGER, 2)),
        /Expected 2 items/,
      ],
      [
        variantValue('grumpy', ENUM(['sad', 'ok'])),
        /is not a member of/,
      ],
      [
        variantValue(unionValue('x', 1), UNION({ n: INTEGER })),
        /has no member 'x'/,
      ],
      [
        variantValue(listValue([1, 'two'])),
        /inner type INTEGER\[\]\): input is not a number/,
      ],
    ];
    for (const [value, error] of cases) {
      assert.throws(() => chunk.setRows([[value]]), error);
    }
  });
});
