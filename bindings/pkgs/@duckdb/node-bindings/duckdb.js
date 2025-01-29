const getRuntimePlatformArch = () => `${process.platform}-${process.arch}`;

let native;

switch(getRuntimePlatformArch()) {
    case 'linux-x64':
        native = require('@duckdb/node-bindings-linux-x64/duckdb.node');
        break;
    case 'linux-arm64':
        native = require('@duckdb/node-bindings-linux-arm64/duckdb.node');
        break;
    case 'darwin-arm64':
        native = require('@duckdb/node-bindings-darwin-arm64/duckdb.node');
        break;
    case 'darwin-x64':
        native = require('@duckdb/node-bindings-darwin-x64/duckdb.node');
        break;
    case 'win32-x64':
        native = require('@duckdb/node-bindings-win32-x64/duckdb.node');
        break;
    default:
        throw new Error(`Unsupported platform: ${getRuntimePlatformArch()}`);
}

module.exports = native;

