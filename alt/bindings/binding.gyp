{
  'targets': [
    {
      'target_name': 'fetch_libduckdb',
      'type': 'none',
      'actions': [
        {
          'action_name': 'run_fetch_libduckdb_script',
          'message': 'Running fetch libduckdb script',
          'inputs': [],
          'action': ['python3', '<(module_root_dir)/scripts/fetch_libduckdb.py', '<(OS)', '<(module_root_dir)/libduckdb'],
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
        }]
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
