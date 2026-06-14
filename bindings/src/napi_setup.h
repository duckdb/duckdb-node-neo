#pragma once

// Single include site for napi.h so the node-addon-api configuration #defines
// below are guaranteed to precede it. Include this instead of napi.h directly.
#define NODE_ADDON_API_DISABLE_DEPRECATED
#define NODE_ADDON_API_REQUIRE_BASIC_FINALIZERS
#define NODE_API_NO_EXTERNAL_BUFFERS_ALLOWED
#include "napi.h"
