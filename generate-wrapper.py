import shutil
import sys
import pycparser
import tempfile
import urllib.request
import os
import zipfile
import platform

# those functions return promises asynchronously since they may block and/or do IO
async_functions = [
    'duckdb_open',
    'duckdb_open_ext',
    'duckdb_close',
    'duckdb_connect',
    'duckdb_disconnect',
    'duckdb_query',
    'duckdb_prepare',
    'duckdb_execute_prepared',
    'duckdb_fetch_chunk',
    'duckdb_execute_tasks',
    'duckdb_appender_create',
    'duckdb_appender_flush',
    'duckdb_appender_close',
    'duckdb_appender_destroy',
    'duckdb_execute_prepared',
    'duckdb_extract_statements',
    'duckdb_prepare_extracted_statement',
    'duckdb_execute_pending',
]

pointer_wrappers = [
    'duckdb_appender',
    'duckdb_config',
    'duckdb_connection',
    'duckdb_data_chunk',
    'duckdb_database',
    'duckdb_extracted_statements',
    'duckdb_logical_type',
    'duckdb_pending_result',
    'duckdb_prepared_statement',
    'duckdb_value',
    'duckdb_vector',
]

deprecated_functions = [
]

def typename(decl):
    const = ''
    if hasattr(decl, 'quals') and 'const' in decl.quals:
        const = 'const '
    if isinstance(decl, pycparser.c_ast.TypeDecl):
        return const + typename(decl.type)
    elif isinstance(decl, pycparser.c_ast.PtrDecl):
        return const + typename(decl.type) + '*'
    elif isinstance(decl, pycparser.c_ast.IdentifierType):
        return decl.names[0].replace('_Bool', 'bool')
    else:
       raise ValueError(decl)

class DuckDBHeaderVisitor(pycparser.c_ast.NodeVisitor):
    cpp_result = ''
    types_result = ''
    c_type_to_ts_type = {
        "bool": "boolean",
        "double": "number",
        "char*": "string",
        "char**": "out_string_wrapper",
        "const char*": "string",
        "const char**": "out_string_wrapper",
        "float": "number",
        "idx_t": "number",
        "idx_t*": "idx_pointer",
        "int8_t": "number",
        "int16_t": "number",
        "int32_t": "number",
        "int64_t": "number", # should use bigint because max safe int in JS is 2^53-1
        "uint8_t": "number",
        "uint16_t": "number",
        "uint32_t": "number",
        "uint64_t": "number", # should use bigint because max safe int in JS is 2^53-1
        "uint64_t*": "uint64_pointer",
        "size_t": "number",
        "void": "void",
        "void*": "pointer",
        "const void*": "pointer",
    }

    def visit_TypeDecl(self, node):
        name = node.declname
        if not name.startswith('duckdb_'):
            return

        if isinstance(node.type, pycparser.c_ast.Struct):
            self.cpp_result += f'\tduckdb_node::PointerHolder<{name}>::Init(env, exports, constructors, "{name}");\n'
            self.types_result += f'export class {name} {{}}\n'
            self.c_type_to_ts_type[name] = name
            self.c_type_to_ts_type[f'{name}*'] = name

        elif isinstance(node.type, pycparser.c_ast.Enum):
            self.cpp_result += f'\tauto {name}_enum = Napi::Object::New(env);\n'
            self.types_result += f'export enum {name} {{\n'
            self.c_type_to_ts_type[name] = name
            enum_idx = 0
            for enum in node.type.values.enumerators:
                if enum.value is not None:
                    enum_idx = int(enum.value.value)
                self.cpp_result += f'\t{name}_enum.Set("{enum.name}", {enum_idx});\n'
                self.types_result += f'  {enum.name} = {enum_idx},\n'
                enum_idx += 1
            self.cpp_result += f'''\texports.DefineProperty(
\t    Napi::PropertyDescriptor::Value("{name}", {name}_enum,
\t                                    static_cast<napi_property_attributes>(napi_enumerable | napi_configurable)));
'''
            self.types_result += f'}}\n'
        
        elif typename(node.type) == 'void':
            # Do these void* types need any corresponding Napi code?
            self.types_result += f'export class {name} {{}}\n'
            self.c_type_to_ts_type[name] = name
            self.c_type_to_ts_type[f'{name}*'] = name
            
        else:
            print(f'type not handled: {name}')
            print(node)


    def visit_FuncDecl(self, node):
       name = None
       ret = typename(node.type)

       args = []
       if isinstance(node.type, pycparser.c_ast.TypeDecl):
           name = node.type.declname
       elif isinstance(node.type, pycparser.c_ast.PtrDecl):
           name = node.type.type.declname
       else:
           raise ValueError(node.type)


       if node.args:
           for p in node.args.params:
               args.append((p.name, typename(p.type)))

       if name == '__routine':
           print(f'function skipped: {name}')
           return # ??

       if 'replacement' in name:
           print(f'function skipped: {name}')
           return # ??

       if 'delete_callback' in name:
           print(f'function skipped: {name}')
           self.types_result += f'export type {name} = (data: pointer) => void;\n'
           return # ??

       if 'duckdb_init_' in name:
            print(f'function skipped: {name}')
            return

       if 'table_function' in name:
           print(f'function skipped: {name}')
           return # TODO

       if 'arrow' in name:
           print(f'function skipped: {name}')
           return # TODO

       if name in deprecated_functions:
           print(f'deprecated function skipped: {name}')
           return

       #print(f"{name}")

       n_args = len(args)

       fwrap_args = list(map(lambda arg: arg[1], args)) + ['"' + name + '"']
       is_async = name in async_functions
       asyncstr = ''
       if is_async:
           asyncstr = 'Async'
       voidstr = ''
       if ret == 'void':
           voidstr = 'Void'
       else:
           fwrap_args.insert(0, ret)
       fwrap_arg_str = ', '.join(fwrap_args)

       ts_args_strs = list(map(lambda arg: f'{arg[0]}: {self.c_type_to_ts_type[arg[1]] if arg[1] in self.c_type_to_ts_type else arg[1]}', args))
       ts_args_str = ', '.join(ts_args_strs)
       ts_ret_type = self.c_type_to_ts_type[ret] if ret in self.c_type_to_ts_type else ret
       if is_async:
          ts_ret_type = f'Promise<{ts_ret_type}>'

       self.cpp_result += f'''\texports.Set(
\t    Napi::String::New(env, "{name}"),
\t    Napi::Function::New<duckdb_node::FunctionWrappers::{asyncstr}FunctionWrapper{n_args}{voidstr}<{fwrap_arg_str}>>(env));
'''
       self.types_result += f'export function {name}({ts_args_str}): {ts_ret_type};\n'



def create_func_defs(filename):
    ast = pycparser.parse_file(filename, use_cpp=False)

    v = DuckDBHeaderVisitor()
    v.visit(ast)
    return v.cpp_result, v.types_result

def get_release_zip_url():
    system = platform.system()
    if system == "Linux":
        machine = platform.machine()
        if machine == "aarch64":
            return "https://github.com/duckdb/duckdb/releases/download/v1.0.0/libduckdb-linux-aarch64.zip"
        elif machine == "amd64" or machine == "x86_64":
            return "https://github.com/duckdb/duckdb/releases/download/v1.0.0/libduckdb-linux-amd64.zip"
        else:
            raise Exception("Unsupported machine: " + machine)
    elif system == "Darwin":
        return "https://github.com/duckdb/duckdb/releases/download/v1.0.0/libduckdb-osx-universal.zip"
    elif system == "Windows":
        return "https://github.com/duckdb/duckdb/releases/download/v1.0.0/libduckdb-windows-amd64.zip"
    else:
        raise Exception("Unsupported system: " + system)

def get_lib_file_name():
    system = platform.system()
    if system == "Linux":
        return "libduckdb.so"
    elif system == "Darwin":
        return "libduckdb.dylib"
    elif system == "Windows":
        return "duckdb.dll"
    else:
        raise Exception("Unsupported system: " + system)

if __name__ == "__main__":
    dir_path = os.path.dirname(os.path.realpath(__file__))

    with tempfile.TemporaryDirectory() as zip_tmp:
        zip_path = os.path.join(zip_tmp, "libduckdb.zip")
        
        release_zip_url = get_release_zip_url()
        print("Downloading " + release_zip_url)
        urllib.request.urlretrieve(release_zip_url, zip_path)
        
        print("Extracting zip")
        zip = zipfile.ZipFile(zip_path)

        with tempfile.TemporaryDirectory() as ext_tmp:
            lib_file_name = get_lib_file_name()
            zip.extract(lib_file_name, ext_tmp)
            shutil.copy(os.path.join(ext_tmp, lib_file_name), os.path.join(dir_path, "lib", "binding", "libduckdb"))

            zip.extract("duckdb.h", "src")
            zip.extract("duckdb.h", ext_tmp)

            print("Preprocessing duckdb.h")
            os.system("sed -i -e 's/#include <stdlib.h>/#include <stddef.h>/' %s" % os.path.join(ext_tmp, "duckdb.h")) # until 0.10.0 has been released
            os.system("gcc -DDUCKDB_NO_EXTENSION_FUNCTIONS -DDUCKDB_API_NO_DEPRECATED -E -D__builtin_va_list=int -D'__attribute__(x)=' %s > %s"
                % (os.path.join(ext_tmp, "duckdb.h"), os.path.join(ext_tmp, "duckdb-preprocessed.h")))

            print("Generating C code and TypeScript definitions")
            cpp_result, types_result = create_func_defs(os.path.join(ext_tmp, "duckdb-preprocessed.h"))

    out = open(os.path.join(dir_path, "src", "duckdb_node_generated.cpp"), 'wb')
    out.write('''// This file is generated by generate-wrapper.py, please do not edit

#include "duckdb.h"
#include "function_wrappers.hpp"

static void RegisterGenerated(Napi::Env env, Napi::Object exports,
                              std::unordered_map<const char *, Napi::FunctionReference> &constructors) {
'''.encode())
    out.write(cpp_result.encode())
    out.write('}\n'.encode())

    types_out = open('lib/duckdb.d.ts', 'wb')
    
    types_out.write('// placeholder interfaces for pointer types\n'.encode())
    types_out.write('export interface idx_pointer extends pointer {}\n'.encode())

    types_out.write('// bindings-defined types\n'.encode())
    types_out.write('export class pointer {}\n'.encode())
    types_out.write('export class uint64_pointer {}\n'.encode())
    types_out.write('export class out_string_wrapper {}\n'.encode())

    types_out.write('// generated types and functions\n'.encode())
    types_out.write(types_result.encode())

    types_out.write('// bindings-defined constants\n'.encode())
    types_out.write('export const sizeof_bool: number;\n'.encode())

    types_out.write('// bindings-defined functions\n'.encode())
    types_out.write('export function copy_buffer(buffer: pointer, length: number): Uint8Array | null;\n'.encode())
    types_out.write('export function copy_buffer_double(buffer: number, length: number): Uint8Array | null;\n'.encode())
    types_out.write('export function out_get_string(string_wrapper: out_string_wrapper): string;\n'.encode())
