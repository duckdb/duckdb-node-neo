import fs from 'fs';
import path from 'path';

function getFunctionSignaturesFromHeader(headerFilePath) {
  const sigs = [];
  const headerContents = fs.readFileSync(headerFilePath, { encoding: 'utf-8' });
  // Declarations that are conditionally compiled get stamped with the guard they
  // sit under, so the header, the type defs and the bindings agree about which
  // ones are optional.
  //
  // The deprecated guard is spelled two ways. Through 1.5.x it is
  // `#ifndef DUCKDB_API_NO_DEPRECATED`; from 2.0 the same declarations sit under
  // `#if (DUCKDB_API_VERSION_BELOW(x, y, z) || DUCKDB_API_ALLOW_DEPRECATED)`,
  // optionally ANDed with a DUCKDB_API_VERSION_AT_LEAST clause. Both are recognized
  // and both stamp the legacy name, so the `// #ifndef DUCKDB_API_NO_DEPRECATED`
  // markers in duckdb.d.ts and duckdb_node_bindings.cpp keep matching across the
  // 2.0 upgrade without being rewritten.
  //
  // The negative lookahead skips the preamble that *sets* the switch
  // (`#if !defined(DUCKDB_API_ALLOW_DEPRECATED)`), which guards a #define rather
  // than declarations. 2.0 also wraps every declaration in a bare
  // DUCKDB_API_VERSION_AT_LEAST guard; those are not stamped, and since the header
  // keeps one guard per declaration rather than nesting them, the trailing #endif
  // alternative still clears the stamp at the right point.
  const sigRegex = /^DUCKDB_C_API (?<sig>([^;]|[\r\n])*);$|^#ifndef (?<startif>DUCKDB_API_NO_DEPRECATED|DUCKDB_NO_EXTENSION_FUNCTIONS)$|^#if (?!!defined)(?<startdeprecated>.*\bDUCKDB_API_ALLOW_DEPRECATED\b.*)$|^#endif$/gm;
  var ifndef = undefined;
  var match;
  while (match = sigRegex.exec(headerContents)) {
    if (match.groups.sig) {
      sigs.push({ sig: match.groups.sig.replace(/\r\n/gm, ' ').replace(/\n/gm, ' ').replace(/  +/gm, ' '), ...(ifndef ? { ifndef } : {}) });
    } else if (match.groups.startif) {
      ifndef = match.groups.startif;
    } else if (match.groups.startdeprecated) {
      ifndef = 'DUCKDB_API_NO_DEPRECATED';
    } else {
      ifndef = undefined;
    }
  }
  return sigs;
}

function getFunctionSignaturesFromComments(filePath) {
  const sigs = [];
  const fileContents = fs.readFileSync(filePath, { encoding: 'utf-8' });
  const sigRegex = /^\s*\/\/ DUCKDB_C_API (?<sig>([^;])*);$|^\s*\/\/ #ifndef (?<startif>DUCKDB_API_NO_DEPRECATED|DUCKDB_NO_EXTENSION_FUNCTIONS)$|^\s*\/\/ #endif$/gm;
  var ifndef = undefined;
  var match;
  while (match = sigRegex.exec(fileContents)) {
    if (match.groups.sig) {
      sigs.push({ sig: match.groups.sig, ...(ifndef ? { ifndef } : {}) });
    } else if (match.groups.startif) {
      ifndef = match.groups.startif;
    } else {
      ifndef = undefined;
    }
  }
  return sigs;
}

function checkFunctionSignatures() {
  try {
    if (process.argv[2] === 'removeFiles') {
      fs.rmSync('headerSigs.json');
      fs.rmSync('typeDefsSigs.json');
      fs.rmSync('bindingsSigs.json');
      return;
    }

    const headerFilePath = path.join('libduckdb', 'duckdb.h');
    const typeDefsFilePath = path.join('pkgs', '@duckdb', 'node-bindings', 'duckdb.d.ts');
    const bindingsFilePath = path.join('src', 'duckdb_node_bindings.cpp');

    const headerSigs = getFunctionSignaturesFromHeader(headerFilePath);
    const typeDefsSigs = getFunctionSignaturesFromComments(typeDefsFilePath);
    const bindingsSigs = getFunctionSignaturesFromComments(bindingsFilePath);

    console.log(`header sigs: ${headerSigs.length}`);
    console.log(`type defs sigs: ${typeDefsSigs.length}`);
    console.log(`bindings sigs: ${bindingsSigs.length}`);

    const headerSigsJSON = JSON.stringify(headerSigs, null, 2);
    const typeDefsSigsJSON = JSON.stringify(typeDefsSigs, null, 2);
    const bindingsSigsJSON = JSON.stringify(bindingsSigs, null, 2);

    if (headerSigsJSON === typeDefsSigsJSON) {
      console.log('OK: Type defs sigs match header sigs');
    } else {
      console.warn('WARNING: Type defs sigs DO NOT match header sigs!');
    }

    if (headerSigsJSON === bindingsSigsJSON) {
      console.log('OK: Bindings sigs match header sigs');
    } else {
      console.warn('WARNING: Bindings sigs DO NOT match header sigs!');
    }

    if (process.argv[2] === 'writeFiles') {
      fs.writeFileSync('headerSigs.json', headerSigsJSON);
      fs.writeFileSync('typeDefsSigs.json', typeDefsSigsJSON);
      fs.writeFileSync('bindingsSigs.json', bindingsSigsJSON);
    }
  } catch (e) {
    console.error(e);
  }
}

checkFunctionSignatures();
