package migrations

import "embed"

// FS embeds all versioned SQL migration files.
//
//go:embed *.sql
var FS embed.FS
