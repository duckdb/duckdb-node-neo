const getRuntimePlatformArch = () => `${process.platform}-${process.arch}`;

/**
 * @throw Error if there isn't any available native binding for the current platform/arch.
 */
const getNativeNodeBinding = (runtimePlatformArch) => {
  switch (runtimePlatformArch) {
    case `linux-x64`:
      return require('@databrainhq/node-bindings-linux-x64/duckdb.node');
    case 'linux-arm64':
      return require('@databrainhq/node-bindings-linux-arm64/duckdb.node');
    case 'darwin-arm64':
      return require('@databrainhq/node-bindings-darwin-arm64/duckdb.node');
    case 'darwin-x64':
      return require('@databrainhq/node-bindings-darwin-x64/duckdb.node');
    case 'win32-x64':
      return require('@databrainhq/node-bindings-win32-x64/duckdb.node');
    default:
      const [platform, arch] = runtimePlatformArch.split('-');
      throw new Error(
        `Error loading duckdb native binding: unsupported arch '${arch}' for platform '${platform}'`,
      );
  }
};

module.exports = getNativeNodeBinding(getRuntimePlatformArch());
