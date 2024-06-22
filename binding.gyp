{
    "targets": [
    {
            'target_name': 'duckdb_shared_object',
            'type': 'none',
            'actions': [
                {
                    'action_name': 'download',
                    'message': 'Downloading DuckDB shared library...',
                    'inputs': [],
                    'outputs': ['lib/binding/libduckdb', 'src/duckdb.h', 'src/duckdb_node_generated.cpp'],
                    'action': ['python3', 'generate-wrapper.py'],
                },
            ],
      },
        {
            "target_name": "<(module_name)",
            "dependencies": ["duckdb_shared_object"],
            "sources": [
                "src/duckdb_node.cpp"
            ], 
            "include_dirs": [
                "<!(node -p \"require('node-addon-api').include_dir\")", 
                "src/duckdb/src/include"
            ], 
            "defines": [
                "NAPI_VERSION=6"
            ], 
            "cflags_cc": [
                "-frtti", 
                "-fexceptions", 
                "-Wno-redundant-move"
            ], 
            "cflags_cc!": [
                "-fno-rrti", 
                "-fno-exceptions"
            ], 
            "cflags": [
                "-frtti", 
                "-fexceptions", 
                "-Wno-redundant-move", 
                "-frtti"
            ], 
            "cflags!": [
                "-fno-rrti", 
                "-fno-exceptions"
            ], 
            "xcode_settings": {
                "GCC_ENABLE_CPP_EXCEPTIONS": "YES", 
                "GCC_ENABLE_CPP_RTTI": "YES", 
                "CLANG_CXX_LIBRARY": "libc++", 
                "MACOSX_DEPLOYMENT_TARGET": "10.15", 
                "CLANG_CXX_LANGUAGE_STANDARD": "c++20",
                "OTHER_CFLAGS": [
                    "-fexceptions", 
                    "-frtti", 
                    "-Wno-redundant-move"
                ]
            }, 
            "msvs_settings": {
                "VCCLCompilerTool": {
                    "ExceptionHandling": 1, 
                    "AdditionalOptions": [
                        "/bigobj", 
                        "/GR"
                    ]
                }
            }, 
            "conditions": [
                [
                    "OS==\"win\"", 
                    {
                        "defines": [
                            "DUCKDB_BUILD_LIBRARY"
                        ]
                    }
                ]
            ], 
            "libraries": []
        }, 
        {
            "target_name": "action_after_build", 
            "type": "none", 
            "dependencies": [
                "<(module_name)"
            ], 
            "copies": [
                {
                    "files": [
                        "<(PRODUCT_DIR)/<(module_name).node"
                    ], 
                    "destination": "<(module_path)"
                }
            ]
        }
    ]
}