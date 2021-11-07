package node

import "github.com/caravan/streaming/internal/stream/node"

// Subprocess constructs a processor that consists of the specified Processors,
// each to be invoked one after the other
var Subprocess = node.Subprocess
