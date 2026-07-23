// Copyright (c) 2026 Digiliza
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlstore

import (
	"strings"
	"testing"
	"testing/fstest"

	"go.mau.fi/util/dbutil"
)

func newTestSchemaDB(t *testing.T, schema string) *storeDB {
	t.Helper()
	wrapped, err := dbutil.NewWithDB(nil, "postgres")
	if err != nil {
		t.Fatal(err)
	}
	return newSchemaStoreDB(wrapped, schema)
}

func TestRewriteQualifiesTableReferences(t *testing.T) {
	db := newTestSchemaDB(t, "session_abc")
	cases := []struct{ in, want string }{
		{
			`SELECT identity FROM whatsmeow_identity_keys WHERE our_jid=$1`,
			`SELECT identity FROM "session_abc".whatsmeow_identity_keys WHERE our_jid=$1`,
		},
		{
			`INSERT INTO whatsmeow_sessions (our_jid, their_id, session) VALUES ($1, $2, $3)`,
			`INSERT INTO "session_abc".whatsmeow_sessions (our_jid, their_id, session) VALUES ($1, $2, $3)`,
		},
		{
			`UPDATE whatsmeow_pre_keys SET uploaded=true WHERE jid=$1`,
			`UPDATE "session_abc".whatsmeow_pre_keys SET uploaded=true WHERE jid=$1`,
		},
		{
			"CREATE TABLE whatsmeow_nct_salt (\n\tour_jid TEXT PRIMARY KEY,\n\tFOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE\n)",
			"CREATE TABLE \"session_abc\".whatsmeow_nct_salt (\n\tour_jid TEXT PRIMARY KEY,\n\tFOREIGN KEY (our_jid) REFERENCES \"session_abc\".whatsmeow_device(jid) ON DELETE CASCADE\n)",
		},
		{
			// Index names must stay unqualified; the ON target is qualified.
			`CREATE INDEX whatsmeow_retry_buffer_timestamp_idx ON whatsmeow_retry_buffer (our_jid, timestamp);`,
			`CREATE INDEX whatsmeow_retry_buffer_timestamp_idx ON "session_abc".whatsmeow_retry_buffer (our_jid, timestamp);`,
		},
		{
			// Multi-line query with lowercase keywords from Go constants.
			"SELECT key, sender_jid\nFROM whatsmeow_message_secrets\nWHERE our_jid=$1",
			"SELECT key, sender_jid\nFROM \"session_abc\".whatsmeow_message_secrets\nWHERE our_jid=$1",
		},
	}
	for _, tc := range cases {
		if got := db.rewrite(tc.in); got != tc.want {
			t.Errorf("rewrite(%q):\n got: %s\nwant: %s", tc.in, got, tc.want)
		}
	}
}

func TestRewriteKeepsConflictTargetUnqualified(t *testing.T) {
	db := newTestSchemaDB(t, "session_abc")
	got := db.rewrite(putLIDMappingQuery)
	if !strings.Contains(got, `INSERT INTO "session_abc".whatsmeow_lid_map`) {
		t.Errorf("insert target not qualified: %s", got)
	}
	// The ON CONFLICT excluded-row comparison references the bare table name
	// and must not be qualified (Postgres rejects schema-qualified names
	// there as a correlation reference).
	if !strings.Contains(got, `WHERE whatsmeow_lid_map.pn<>excluded.pn`) {
		t.Errorf("conflict target was wrongly qualified: %s", got)
	}
}

func TestRewriteAppliesEmbeddedUpgrades(t *testing.T) {
	db := newTestSchemaDB(t, "s1")
	table, latest, err := loadSchemaUpgrades()
	if err != nil {
		t.Fatal(err)
	}
	if latest < 14 {
		t.Fatalf("expected at least 14 upgrade versions, got %d", latest)
	}
	base, found := table[0]
	if !found {
		t.Fatal("missing base schema upgrade (from v0)")
	}
	rewritten := db.rewrite(base.sql)
	if strings.Contains(rewritten, " whatsmeow_device(") && !strings.Contains(rewritten, `"s1".whatsmeow_device(`) {
		t.Errorf("base schema left unqualified references:\n%s", rewritten)
	}
	if !strings.Contains(rewritten, `CREATE TABLE "s1".whatsmeow_device`) {
		t.Errorf("device table not qualified:\n%s", rewritten)
	}
}

func TestRewriteMemoizesQueries(t *testing.T) {
	db := newTestSchemaDB(t, "session_abc")
	query := `SELECT identity FROM whatsmeow_identity_keys WHERE our_jid=$1`
	first := db.rewrite(query)
	cached, ok := db.rewriteCache.Load(query)
	if !ok {
		t.Fatal("rewrite result was not cached")
	}
	if cached.(string) != first || db.rewrite(query) != first {
		t.Fatalf("cached rewrite differs from fresh rewrite: %q", first)
	}
}

func TestLoadSchemaUpgradesRejectsUnsupportedDbutilFeatures(t *testing.T) {
	valid := "-- v1: Base\nCREATE TABLE whatsmeow_device (jid TEXT PRIMARY KEY);\n"
	cases := []struct{ name, file, content string }{
		{"transaction marker", "01-base.sql", "-- v1: Base\n-- transaction: off\nCREATE INDEX foo ON whatsmeow_device (jid);\n"},
		{"dialect filter", "01-base.sql", "-- v1: Base\n-- only: sqlite\nPRAGMA foo;\n"},
		{"split postgres file", "01-base.postgres.sql", valid},
		{"split sqlite file", "01-base.sqlite.sql", valid},
	}
	for _, tc := range cases {
		fsys := fstest.MapFS{tc.file: &fstest.MapFile{Data: []byte(tc.content)}}
		if _, _, err := loadSchemaUpgradesFS(fsys); err == nil {
			t.Errorf("%s: expected loadSchemaUpgradesFS to fail", tc.name)
		} else if !strings.Contains(err.Error(), "not supported by the schema-scoped runner") {
			t.Errorf("%s: unexpected error: %v", tc.name, err)
		}
	}
	// A plain file must keep loading.
	fsys := fstest.MapFS{"01-base.sql": &fstest.MapFile{Data: []byte(valid)}}
	table, latest, err := loadSchemaUpgradesFS(fsys)
	if err != nil || latest != 1 || len(table) != 1 {
		t.Fatalf("plain upgrade file rejected: %v (latest=%d, entries=%d)", err, latest, len(table))
	}
}

func TestRewriteNoopWithoutSchema(t *testing.T) {
	wrapped, err := dbutil.NewWithDB(nil, "postgres")
	if err != nil {
		t.Fatal(err)
	}
	db := newStoreDB(wrapped)
	query := `SELECT identity FROM whatsmeow_identity_keys WHERE our_jid=$1`
	if got := db.rewrite(query); got != query {
		t.Errorf("expected no rewriting without schema, got: %s", got)
	}
}
