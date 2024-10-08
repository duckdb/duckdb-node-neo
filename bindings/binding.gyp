{
  'targets': [
    {
      'target_name': 'fetch_libduckdb',
      'type': 'none',
      'conditions': [
        ['OS=="linux"', {
          'variables': {
            'script_path': '<(module_root_dir)/scripts/fetch_libduckdb_linux.py',
          },
        }],
        ['OS=="mac"', {
          'variables': {
            'script_path': '<(module_root_dir)/scripts/fetch_libduckdb_mac.py',
          },
        }],
        ['OS=="win"', {
          'variables': {
            'script_path': '<(module_root_dir)/scripts/fetch_libduckdb_win.py',
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
        '<!(node -p "require(\'node-addon-api\').targets"):node_addon_api_except',
      ],
      'sources': ['src/duckdb_node_bindings.cpp'],
      'include_dirs': ['<(module_root_dir)/libduckdb'],
      'conditions': [
        ['OS=="linux"', {
          'link_settings': {
            'libraries': [
              '-lduckdb',
              '-L<(module_root_dir)/libduckdb',
              '-Wl,-rpath,\'$$ORIGIN\'',
            ],
          },
          'copies': [
            {
              'files': ['<(module_root_dir)/libduckdb/libduckdb.so'],
              'destination': '<(module_root_dir)/pkgs/@duckdb/node-bindings-linux-x64',
            },
          ],
        }],
        ['OS=="mac"', {
          'cflags+': ['-fvisibility=hidden'],
          'xcode_settings': {
            'GCC_SYMBOLS_PRIVATE_EXTERN': 'YES', # -fvisibility=hidden
          },
          'link_settings': {
            'libraries': [
              '-lduckdb',
              '-L<(module_root_dir)/libduckdb',
              '-Wl,-rpath,@loader_path',
            ],
          },
          'copies': [
            {
              'files': ['<(module_root_dir)/libduckdb/libduckdb.dylib'],
              'destination': '<(module_root_dir)/pkgs/@duckdb/node-bindings-darwin-arm64',
            },
          ],
        }],
        ['OS=="win"', {
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
        ['OS=="linux"', {
          'copies': [
            {
              'files': ['<(module_root_dir)/build/Release/duckdb.node'],
              'destination': '<(module_root_dir)/pkgs/@duckdb/node-bindings-linux-x64',
            },
          ],
        }],
        ['OS=="mac"', {
          'copies': [
            {
              'files': ['<(module_root_dir)/build/Release/duckdb.node'],
              'destination': '<(module_root_dir)/pkgs/@duckdb/node-bindings-darwin-arm64',
            },
          ],
        }],
        ['OS=="win"', {
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
