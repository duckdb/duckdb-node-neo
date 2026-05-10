const getRuntimePlatformArch = () => `${process.platform}-${process.arch}`;

// detect-libc uses Node's own diagnostic report (glibcVersionRuntime) as
// its primary check, with subprocess fallbacks for unusual environments.
// Wrapped in try/catch so a detection failure falls back to glibc rather
// than breaking binding load entirely.
const isLinuxMusl = () => {
    try {
        const { familySync, MUSL } = require('detect-libc');
        return familySync() === MUSL;
    } catch {
        return false;
    }
};

/**
 * @throw Error if there isn't any available native binding for the current platform/arch.
 */
const getNativeNodeBinding = (runtimePlatformArch) => {
    switch(runtimePlatformArch) {
        case `linux-x64`:
            return isLinuxMusl()
                ? require('@duckdb/node-bindings-linux-x64-musl/duckdb.node')
                : require('@duckdb/node-bindings-linux-x64/duckdb.node');
        case 'linux-arm64':
            return isLinuxMusl()
                ? require('@duckdb/node-bindings-linux-arm64-musl/duckdb.node')
                : require('@duckdb/node-bindings-linux-arm64/duckdb.node');
        case 'darwin-arm64':
            return require('@duckdb/node-bindings-darwin-arm64/duckdb.node');
        case 'darwin-x64':
            return require('@duckdb/node-bindings-darwin-x64/duckdb.node');
        case 'win32-arm64':
            return require('@duckdb/node-bindings-win32-arm64/duckdb.node');
        case 'win32-x64':
            return require('@duckdb/node-bindings-win32-x64/duckdb.node');
        default:
            const [platform, arch] = runtimePlatformArch.split('-');
            try {
                return require(`@duckdb/node-bindings-${platform}-${arch}/duckdb.node`);
            } catch (err) {
                throw new Error(`Error loading duckdb native binding: unsupported arch '${arch}' for platform '${platform}'`);
            }            
    }
}

module.exports = getNativeNodeBinding(getRuntimePlatformArch());
