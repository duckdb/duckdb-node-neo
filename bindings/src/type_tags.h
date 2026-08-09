#pragma once

#include "napi_setup.h"

// Type tags for the externals that cross the boundary, in one place so that the
// full set is visible and no value is used twice.
//
// Note that bind info, init info and function info get a tag per function family.
// The C API reuses one opaque handle type for each of them across scalar, table
// and later function families, but the structs behind those handles are unrelated
// and are reinterpret_cast without any check, so mixing them corrupts memory
// rather than failing. Separate tags turn that into a thrown error. The externals themselves live
// with what they wrap: generic ones in externals.h, and family-specific ones --
// whose object owns family state -- in that family's header.
//
// The values are generated using: uuidgen | sed -r -e 's/-//g' -e 's/(.{16})(.*)/0x\1, 0x\2/'
//
// inline rather than static: one object across translation units rather than a
// copy per unit. Tags are compared by value, so either works, but there is no
// reason to have more than one.

inline constexpr napi_type_tag AppenderTypeTag = {
  0x32E0AB3B83F74A89, 0xB785905D92D54996
};

inline constexpr napi_type_tag ClientContextTypeTag = {
  0x1E1738782ED94232, 0x867B024D1858DF3A
};

inline constexpr napi_type_tag ConfigTypeTag = {
  0x5963FBB9648B4D2A, 0xB41ADE86056218D1
};

inline constexpr napi_type_tag ConnectionTypeTag = {
  0x922B9BF54AB04DFC, 0x8A258578D371DB71
};

inline constexpr napi_type_tag DataChunkTypeTag = {
  0x2C7537AB063A4296, 0xB1E70F08B0BBD1A3
};

inline constexpr napi_type_tag DatabaseTypeTag = {
  0x835A8533653C40D1, 0x83B3BE2B233BA8F3
};

inline constexpr napi_type_tag ExtractedStatementsTypeTag = {
  0x59288E1C60C44EEB, 0xBFA35376EE0F04DD
};

inline constexpr napi_type_tag InstanceCacheTypeTag = {
  0x2F3346E30FB5457C, 0xB9201EE5112EEF9F
};

inline constexpr napi_type_tag LogicalTypeTypeTag = {
  0x78AF202191ED4A23, 0x8093715369592A2B
};

inline constexpr napi_type_tag PendingResultTypeTag = {
  0x257E88ECE8294FEC, 0xB64963BBBD1DBB41
};

inline constexpr napi_type_tag PreparedStatementTypeTag = {
  0xA8B03DAD16D34416, 0x9735A7E1F2A1240C
};

inline constexpr napi_type_tag ResultTypeTag = {
  0x08F7FE3AE12345E5, 0x8733310DC29372D9
};

inline constexpr napi_type_tag ScalarFunctionBindInfoTypeTag = {
  0x9922F8468F9A43C0, 0xB2573B112B9600D8
};

inline constexpr napi_type_tag ScalarFunctionInfoTypeTag = {
  0xB0E6739D698048EA, 0x9E79734E3E137AC3
};

inline constexpr napi_type_tag ScalarFunctionTypeTag = {
  0x95D48B7051D14994, 0x9F883D7DF5DEA86D
};

inline constexpr napi_type_tag TableFunctionBindInfoTypeTag = {
  0xFF9280FBDC3341E3, 0xAE7F563D67540007
};

inline constexpr napi_type_tag TableFunctionInfoTypeTag = {
  0xA8CFE12055EE470C, 0xB37877D9FDD36A98
};

inline constexpr napi_type_tag TableFunctionInitInfoTypeTag = {
  0x45B24025B7B443D9, 0xA2978778FE81AD51
};

inline constexpr napi_type_tag TableFunctionTypeTag = {
  0xBECF7A8CEBA84520, 0xBFC47C84A51544EF
};

inline constexpr napi_type_tag ValueTypeTag = {
  0xC60F36613BF14E93, 0xBAA92848936FAA25
};

inline constexpr napi_type_tag VectorTypeTag = {
  0x9FE56DE8E3124D07, 0x9ABF31145EDE1C9E
};
