import fs from 'fs';

function getDuckDBHeaderFilePathFromArgs(argv) {
  const duckdbHeaderFilePath = process.argv[2];
  if (!duckdbHeaderFilePath) {
    throw new Error(`First argument should be path to duckdb.h file.`);
  }
  return duckdbHeaderFilePath;
}

function getDuckDBFunctionSignatures(duckdbHeaderFilePath) {
  const sigs = [];
  const duckdbHeaderContents = fs.readFileSync(duckdbHeaderFilePath, { encoding: 'utf-8' });
  const sigRegex = /^DUCKDB_API (?<sig>([^;]|[\r\n])*);$|^#ifndef (?<startif>DUCKDB_API_NO_DEPRECATED|DUCKDB_NO_EXTENSION_FUNCTIONS)$|^#endif$/gm;
  var skip = false;
  var match;
  while (match = sigRegex.exec(duckdbHeaderContents)) {
    if (match.groups.sig) {
      if (skip) {
        // console.log(`SKIPPING ${match.groups.sig}`);
        continue;
      }
      // console.log(match.groups.sig);
      sigs.push(match.groups.sig.replace(/\r\n/gm, ' ').replace(/\n/gm, ' ').replace(/  +/gm, ' '));
    } else if (match.groups.startif) {
      // console.log(match.groups.startif);
      skip = true;
    } else {
      // console.log(match[0]);
      skip = false;
    }
  }
  return sigs;
}

try {
  const sigs = getDuckDBFunctionSignatures(getDuckDBHeaderFilePathFromArgs(process.argv));
  console.log(sigs.map((sig) => `// ${sig}`).join('\n'));
} catch (e) {
  console.error(e);
}

// Usage:
// node getDuckDBFunctionSignatures.mjs ../../src/duckdb.h > duckdb.d.ts
// node getDuckDBFunctionSignatures.mjs ../../src/duckdb.h > duckdb_node_bindings.cpp
