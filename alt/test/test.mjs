import duckdb from 'duckdb';

try {

console.log(duckdb.library_version());
// console.log(duckdb.config_count());
// console.log(duckdb.get_config_flag(0));
// try {
//   console.log(duckdb.get_config_flag(duckdb.config_count()));
// } catch (e) {
//   console.error(e);
// }
// try {
//   console.log(duckdb.get_config_flag(-1));
// } catch (e) {
//   console.error(e);
// }
// for (let i = 0; i < duckdb.config_count(); i++) {
//   console.log(duckdb.get_config_flag(i));
// }
const db = await duckdb.open(':memory:');
const conn = await duckdb.connect(db);
const result = await duckdb.query(conn, 'from test_all_types()');
const column_count = duckdb.column_count(result);
for (let i = 0; i < column_count; i++) {
  console.log(`${i}: ${duckdb.column_name(result, i)} ${duckdb.column_type(result, i)}`);
}

} catch (e) {
  console.error(e);
}