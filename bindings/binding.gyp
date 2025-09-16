{
  'targets': [
    {
      'target_name': 'fetch_libduckdb',
      'type': 'none',
      'conditions': [
        ['OS=="linux" and target_arch=="x64"', {
          'variables': {
            'script_path': '<(module_root_dir)/scripts/fetch_libduckdb_linux_amd64.py',
          },
        }],
        ['OS=="linux" and target_arch=="arm64"', {
          'variables': {
            'script_path': '<(module_root_dir)/scripts/fetch_libduckdb_linux_arm64.py',
          },
        }],
        ['OS=="mac"', {
          'variables': {
            'script_path': '<(module_root_dir)/scripts/fetch_libduckdb_osx_universal.py',
          },
        }],
        ['OS=="win" and target_arch=="x64"', {
          'variables': {
            'script_path': '<(module_root_dir)/scripts/fetch_libduckdb_windows_amd64.py',
          },
        }],
      ],
      'actions': [
        {
          'action_name': 'run_fetch_libduckdb_script',
          'message': 'Fetching and extracting libduckdb',
          'inputs': [],
          'action': ['python3', '<(script_path)'],
          'outputs': ['<(module_root_dir)/libduckdb'],
        },
      ],
    },
    {
      'target_name': 'duckdb',
      'dependencies': [
        'fetch_libduckdb',
        '<!(node -p "require(\'node-addon-api\').targets"):node_addon_api_except_all',
      ],
      'sources': ['src/duckdb_node_bindings.cpp'],
      'include_dirs': ['<(module_root_dir)/libduckdb'],
      'conditions': [
        ['OS=="linux" and target_arch=="x64"', {
          'link_settings': {
            'libraries': [
              '-lduckdb',
              '-L<(module_root_dir)/libduckdb',
              '-Wl,-rpath,\'$$ORIGIN\'',
            ],
          },
          'copies': [
            {
              'files': ['<(module_root_dir)/libduckdb/libduckdb.so.1.4'],
              'destination': '<(module_root_dir)/pkgs/@duckdb/node-bindings-linux-x64',
            },
          ],
        }],
        ['OS=="linux" and target_arch=="arm64"', {
          'link_settings': {
            'libraries': [
              '-lduckdb',
              '-L<(module_root_dir)/libduckdb',
              '-Wl,-rpath,\'$$ORIGIN\'',
            ],
          },
          'copies': [
            {
              'files': ['<(module_root_dir)/libduckdb/libduckdb.so.1.4'],
              'destination': '<(module_root_dir)/pkgs/@duckdb/node-bindings-linux-arm64',
            },
          ],
        }],
        ['OS=="mac" and target_arch=="arm64"', {
          'cflags+': ['-fvisibility=hidden'],
          'xcode_settings': {
            'GCC_SYMBOLS_PRIVATE_EXTERN': 'YES', # -fvisibility=hidden
          },
          'link_settings': {
            'libraries': [
              '-lduckdb.1.4',
              '-L<(module_root_dir)/libduckdb',
              '-Wl,-rpath,@loader_path',
            ],
          },
          'copies': [
            {
              'files': ['<(module_root_dir)/libduckdb/libduckdb.1.4.dylib'],
              'destination': '<(module_root_dir)/pkgs/@duckdb/node-bindings-darwin-arm64',
            },
          ],
        }],
        ['OS=="mac" and target_arch=="x64"', {
          'cflags+': ['-fvisibility=hidden'],
          'xcode_settings': {
            'GCC_SYMBOLS_PRIVATE_EXTERN': 'YES', # -fvisibility=hidden
          },
          'link_settings': {
            'libraries': [
              '-lduckdb.1.4',
              '-L<(module_root_dir)/libduckdb',
              '-Wl,-rpath,@loader_path',
            ],
          },
          'copies': [
            {
              'files': ['<(module_root_dir)/libduckdb/libduckdb.1.4.dylib'],
              'destination': '<(module_root_dir)/pkgs/@duckdb/node-bindings-darwin-x64',
            },
          ],
        }],
        ['OS=="win" and target_arch=="x64"', {
          'link_settings': {
            'libraries': [
              '<(module_root_dir)/libduckdb/duckdb.lib',
            ],
          },
          'copies': [
            {
              'files': ['<(module_root_dir)/libduckdb/duckdb.dll'],
              'destination': '<(module_root_dir)/pkgs/@duckdb/node-bindings-win32-x64',
            },
          ],
        }],
      ],
    },
    {
      'target_name': 'copy_duckdb_node',
      'type': 'none',
      'dependencies': ['duckdb'],
      'conditions': [
        ['OS=="linux" and target_arch=="x64"', {
          'copies': [
            {
              'files': ['<(module_root_dir)/build/Release/duckdb.node'],
              'destination': '<(module_root_dir)/pkgs/@duckdb/node-bindings-linux-x64',
            },
          ],
        }],
        ['OS=="linux" and target_arch=="arm64"', {
          'copies': [
            {
              'files': ['<(module_root_dir)/build/Release/duckdb.node'],
              'destination': '<(module_root_dir)/pkgs/@duckdb/node-bindings-linux-arm64',
            },
          ],
        }],
        ['OS=="mac" and target_arch=="arm64"', {
          'copies': [
            {
              'files': ['<(module_root_dir)/build/Release/duckdb.node'],
              'destination': '<(module_root_dir)/pkgs/@duckdb/node-bindings-darwin-arm64',
            },
          ],
        }],
        ['OS=="mac" and target_arch=="x64"', {
          'copies': [
            {
              'files': ['<(module_root_dir)/build/Release/duckdb.node'],
              'destination': '<(module_root_dir)/pkgs/@duckdb/node-bindings-darwin-x64',
            },
          ],
        }],
        ['OS=="win" and target_arch=="x64"', {
          'copies': [
            {
              'files': ['<(module_root_dir)/build/Release/duckdb.node'],
              'destination': '<(module_root_dir)/pkgs/@duckdb/node-bindings-win32-x64',
            },
          ],
        }],
      ],
    },
  ],
}
