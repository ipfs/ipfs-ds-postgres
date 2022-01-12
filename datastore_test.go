package pgds

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	dstest "github.com/ipfs/go-datastore/test"
	"github.com/jackc/pgx/v4"
)

var initOnce sync.Once

func envString(t *testing.T, key string, defaultValue string) string {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}
	return v
}

// Automatically re-create the test datastore.
func initPG(t *testing.T) {
	initOnce.Do(func() {
		connConf, err := pgx.ParseConfig(fmt.Sprintf(
			"postgres://%s:%s@%s/%s?sslmode=disable",
			envString(t, "PG_USER", "postgres"),
			envString(t, "PG_PASS", "postgres"),
			envString(t, "PG_HOST", "127.0.0.1"),
			envString(t, "PG_DB", envString(t, "PG_USER", "postgres")),
		))
		if err != nil {
			t.Fatal(err)
		}
		conn, err := pgx.ConnectConfig(context.Background(), connConf)
		if err != nil {
			t.Fatal(err)
		}
		_, err = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS test_datastore")
		if err != nil {
			t.Fatal(err)
		}
		_, err = conn.Exec(context.Background(), "CREATE DATABASE test_datastore")
		if err != nil {
			t.Fatal(err)
		}
		err = conn.Close(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	})
}

// returns datastore, and a function to call on exit.
//
//  d, close := newDS(t)
//  defer close()
func newDS(t *testing.T) (*Datastore, func()) {
	initPG(t)
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?sslmode=disable",
		envString(t, "PG_USER", "postgres"),
		envString(t, "PG_PASS", "postgres"),
		envString(t, "PG_HOST", "127.0.0.1"),
		"test_datastore",
	)
	connConf, err := pgx.ParseConfig(connString)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.ConnectConfig(context.Background(), connConf)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS blocks (key TEXT NOT NULL UNIQUE, data BYTEA)")
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewDatastore(context.Background(), connString)
	return d, func() {
		_, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS blocks")
		_ = conn.Close(context.Background())
	}
}

func TestSuite(t *testing.T) {
	d, done := newDS(t)
	defer done()
	dstest.SubtestAll(t, d)
}
