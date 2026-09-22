// Package registry implements the content-addressed artifact registry:
// SQLite-backed upload/chunk metadata, a sharded blob store and the
// open -> committing -> committed upload state machine.
package registry

import _ "modernc.org/sqlite"
