// Copyright (c) 2026 Digiliza
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"go.mau.fi/util/dbutil"

	"go.mau.fi/whatsmeow/store/sqlstore/upgrades"
)

// storeDB wraps dbutil.Database with optional schema-qualified query
// rewriting. When schema is set, every whatsmeow_* table reference is
// qualified directly in the SQL ("schema".whatsmeow_device), which lets any
// number of Containers share a single *sql.DB pool without relying on a
// per-connection search_path.
type storeDB struct {
	*dbutil.Database
	// schema is the PostgreSQL schema every whatsmeow_* table reference is
	// scoped to. Empty means no rewriting (default upstream behavior).
	schema string
	// quotedSchema is the pre-quoted schema identifier used in rewrites.
	quotedSchema string
	// sharedPool marks databases whose underlying *sql.DB is owned by the
	// caller and shared between containers, so Container.Close must not
	// close it.
	sharedPool bool

	// rewriteCache memoizes rewritten queries: rewrite runs on every query
	// and almost all of them are package-level constants, so paying the
	// regexp cost once per distinct query keeps it off the hot path (dbutil
	// avoids regexps there for the same reason). rewriteCacheLen caps the
	// cache so dynamically-built queries (variable placeholder lists) cannot
	// grow it without bound.
	rewriteCache    sync.Map
	rewriteCacheLen atomic.Int64
}

// rewriteCacheLimit is far above the ~70 distinct query constants in this
// package; the cap only guards against unbounded dynamically-built queries.
const rewriteCacheLimit = 1024

func newStoreDB(db *dbutil.Database) *storeDB {
	return &storeDB{Database: db}
}

func newSchemaStoreDB(db *dbutil.Database, schema string) *storeDB {
	return &storeDB{
		Database:     db,
		schema:       schema,
		quotedSchema: `"` + strings.ReplaceAll(schema, `"`, `""`) + `"`,
		sharedPool:   true,
	}
}

// tableRefRegex finds whatsmeow_* names in positions where SQL expects a
// table reference (after FROM/INTO/UPDATE/TABLE/REFERENCES/EXISTS/ON).
//
// Two kinds of whatsmeow_* tokens must stay unqualified and are deliberately
// not matched:
//   - index names ("CREATE INDEX whatsmeow_x_idx ON whatsmeow_x"): Postgres
//     requires unqualified index names, and they are already scoped to the
//     table's schema. The token follows INDEX, which is not in the keyword
//     set; the table after ON is qualified.
//   - column references through the bare table name inside ON CONFLICT
//     clauses ("WHERE whatsmeow_lid_map.pn<>excluded.pn"): the conflict
//     target row is exposed under the unqualified table name.
var tableRefRegex = regexp.MustCompile(`(?i)\b(FROM|INTO|UPDATE|TABLE|REFERENCES|EXISTS|ON)(\s+)(whatsmeow_[A-Za-z0-9_]+)`)

func (d *storeDB) rewrite(query string) string {
	if d.schema == "" {
		return query
	}
	if cached, ok := d.rewriteCache.Load(query); ok {
		return cached.(string)
	}
	rewritten := tableRefRegex.ReplaceAllStringFunc(query, func(match string) string {
		groups := tableRefRegex.FindStringSubmatch(match)
		return groups[1] + groups[2] + d.quotedSchema + "." + groups[3]
	})
	if d.rewriteCacheLen.Add(1) <= rewriteCacheLimit {
		d.rewriteCache.Store(query, rewritten)
	}
	return rewritten
}

func (d *storeDB) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return d.Database.Exec(ctx, d.rewrite(query), args...)
}

func (d *storeDB) Query(ctx context.Context, query string, args ...any) (dbutil.Rows, error) {
	return d.Database.Query(ctx, d.rewrite(query), args...)
}

func (d *storeDB) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return d.Database.QueryRow(ctx, d.rewrite(query), args...)
}

var upgradeHeaderRegex = regexp.MustCompile(`^-- (?:v(\d+) -> )?v(\d+)(?: \(compatible with v(\d+)\+\))?:`)

// Markers interpreted by dbutil's upgrade runner but NOT by the schema-scoped
// runner below. If an upstream sync introduces one of these, applying the file
// verbatim would be silently wrong (e.g. an upgrade flagged "transaction: off"
// running inside a transaction, or SQLite-only lines executed on Postgres), so
// loading fails loudly instead.
var (
	unsupportedMarkerRegex   = regexp.MustCompile(`(?m)^\s*-- (transaction|only):`)
	splitUpgradeFileRegex    = regexp.MustCompile(`\.(postgres|sqlite)\.sql$`)
	errUnsupportedUpgradeFmt = "upgrade %s uses dbutil feature %q not supported by the schema-scoped runner; extend upgradeSchema before syncing this upstream change"
)

type schemaUpgrade struct {
	to     int
	compat int
	sql    string
}

type upgradeFS interface {
	fs.ReadDirFS
	fs.ReadFileFS
}

// loadSchemaUpgrades parses the embedded upgrade files into a table indexed
// by source version, mirroring dbutil's upgrade table semantics: a file with
// a "-- vN -> vM" header upgrades from N to M, and a file with just "-- vM"
// upgrades from M-1 to M. Gaps behave as no-op upgrades.
func loadSchemaUpgrades() (map[int]schemaUpgrade, int, error) {
	return loadSchemaUpgradesFS(upgrades.FS)
}

func loadSchemaUpgradesFS(fsys upgradeFS) (table map[int]schemaUpgrade, latest int, err error) {
	entries, err := fsys.ReadDir(".")
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list upgrade files: %w", err)
	}
	table = make(map[int]schemaUpgrade, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		if splitUpgradeFileRegex.MatchString(entry.Name()) {
			return nil, 0, fmt.Errorf(errUnsupportedUpgradeFmt, entry.Name(), "split dialect files")
		}
		data, err := fsys.ReadFile(entry.Name())
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read upgrade %s: %w", entry.Name(), err)
		}
		if marker := unsupportedMarkerRegex.Find(data); marker != nil {
			return nil, 0, fmt.Errorf(errUnsupportedUpgradeFmt, entry.Name(), strings.TrimSpace(string(marker)))
		}
		header, _, _ := strings.Cut(string(data), "\n")
		match := upgradeHeaderRegex.FindStringSubmatch(header)
		if match == nil {
			return nil, 0, fmt.Errorf("missing version header in %s", entry.Name())
		}
		to, _ := strconv.Atoi(match[2])
		from := to - 1
		if match[1] != "" {
			from, _ = strconv.Atoi(match[1])
		}
		compat := to
		if match[3] != "" {
			compat, _ = strconv.Atoi(match[3])
		}
		if _, dup := table[from]; dup {
			return nil, 0, fmt.Errorf("duplicate upgrade from v%d (%s)", from, entry.Name())
		}
		table[from] = schemaUpgrade{to: to, compat: compat, sql: string(data)}
		if to > latest {
			latest = to
		}
	}
	return table, latest, nil
}

func init() {
	// Catch unsupported upstream upgrade formats at process start (and in any
	// test run) rather than on the first session open in production.
	if _, _, err := loadSchemaUpgrades(); err != nil {
		panic(err)
	}
}

// upgradeSchema is the Postgres-only, schema-scoped counterpart of
// dbutil.Database.Upgrade. dbutil executes the embedded SQL verbatim
// (unqualified) and checks information_schema without scoping to a schema,
// so it can't be reused when several schemas share one database. Every
// statement here goes through the query rewriter instead.
func (c *Container) upgradeSchema(ctx context.Context) error {
	db := c.db
	if db.Dialect != dbutil.Postgres {
		return fmt.Errorf("schema-scoped store requires postgres, got %s", db.Dialect)
	}
	if _, err := db.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+db.quotedSchema); err != nil {
		return fmt.Errorf("failed to ensure schema %s exists: %w", db.schema, err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS whatsmeow_version (version INTEGER, compat INTEGER)"); err != nil {
		return fmt.Errorf("failed to create version table: %w", err)
	}
	var version int
	var compatNull sql.NullInt32
	err := db.QueryRow(ctx, "SELECT version, compat FROM whatsmeow_version LIMIT 1").Scan(&version, &compatNull)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to read schema version: %w", err)
	}
	compat := version
	if compatNull.Valid && compatNull.Int32 != 0 {
		compat = int(compatNull.Int32)
	}
	table, latest, err := loadSchemaUpgrades()
	if err != nil {
		return err
	}
	if compat > latest {
		return fmt.Errorf("unsupported database schema version: currently on v%d (compatible down to v%d), latest known: v%d", version, compat, latest)
	}
	for version < latest {
		step, found := table[version]
		if !found {
			version++
			continue
		}
		from := version
		err = db.DoTxn(ctx, nil, func(ctx context.Context) error {
			if _, err := db.Exec(ctx, step.sql); err != nil {
				return fmt.Errorf("failed to run upgrade v%d->v%d: %w", from, step.to, err)
			}
			if _, err := db.Exec(ctx, "DELETE FROM whatsmeow_version"); err != nil {
				return err
			}
			_, err := db.Exec(ctx, "INSERT INTO whatsmeow_version (version, compat) VALUES ($1, $2)", step.to, step.compat)
			return err
		})
		if err != nil {
			return err
		}
		version = step.to
	}
	return nil
}
