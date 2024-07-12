import duckdb from 'duckdb';

console.log(duckdb.library_version());
console.log(duckdb.config_count());
console.log(duckdb.get_config_flag(0));
try {
  console.log(duckdb.get_config_flag(duckdb.config_count()));
} catch (e) {
  console.error(e);
}
try {
  console.log(duckdb.get_config_flag(-1));
} catch (e) {
  console.error(e);
}
// for (let i = 0; i < duckdb.config_count(); i++) {
//   console.log(duckdb.get_config_flag(i));
// }
