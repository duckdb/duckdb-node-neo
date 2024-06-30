{
  'targets': [
    {
      'target_name': 'fetch_libduckdb',
      'type': 'none',
      'conditions': [
        ['OS=="linux"', {
          'variables': {
            'zip_url': '"https://github.com/duckdb/duckdb/releases/download/v1.0.0/libduckdb-linux-amd64.zip"',
          },
        }],
        ['OS=="mac"', {
          'variables': {
            'zip_url': '"https://github.com/duckdb/duckdb/releases/download/v1.0.0/libduckdb-osx-universal.zip"',
          },
        }],
        ['OS=="win"', {
          'variables': {
            'zip_url': '"https://github.com/duckdb/duckdb/releases/download/v1.0.0/libduckdb-windows-amd64.zip"',
          },
        }],
      ],
      'actions': [
        {
          'action_name': 'run_fetch_libduckdb_script',
          'message': 'Running fetch libduckdb script',
          'inputs': [],
          'action': ['python3', '<(module_root_dir)/scripts/fetch_libduckdb.py', '<(OS)', '<(module_root_dir)/libduckdb', '<(zip_url)'],
          'outputs': ['<(module_root_dir)/libduckdb/libduckdb.zip'],
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
              'destination': 'package/lib',
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
              'destination': 'package/lib',
            },
          ],
        }],
        ['OS=="win"', {
          'link_settings': {
            'libraries': [
              '<(module_root_dir)/libduckdb/libduckdb.lib',
            ],
          },
          'copies': [
            {
              'files': ['<(module_root_dir)/libduckdb/libduckdb.dll'],
              'destination': 'package/lib',
            },
          ],
        }],
      ]
    },
    {
      'target_name': 'copy_duckdb',
      'type': 'none',
      'dependencies': ['duckdb'],
      'copies': [
        {
          'files': ['<(module_root_dir)/build/Release/duckdb.node'],
          'destination': 'package/lib',
        },
      ],
    }
  ]
}
