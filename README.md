# ipfs-ds-postgres

[![Build Status](https://travis-ci.org/ipfs/ipfs-ds-postgres.svg?branch=master)](https://travis-ci.org/ipfs/ipfs-ds-postgres)
[![Coverage](https://codecov.io/gh/ipfs/ipfs-ds-postgres/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/ipfs-ds-postgres)
[![Standard README](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg)](https://github.com/RichardLitt/standard-readme)
[![GoDoc](http://img.shields.io/badge/godoc-reference-5272B4.svg)](https://godoc.org/github.com/alanshaw/ipfs-ds-postgres)
[![golang version](https://img.shields.io/badge/golang-%3E%3D1.14.0-orange.svg)](https://golang.org/)
[![Go Report Card](https://goreportcard.com/badge/github.com/ipfs/ipfs-ds-postgres)](https://goreportcard.com/report/github.com/ipfs/ipfs-ds-postgres)

> An implementation of [the datastore interface](https://github.com/ipfs/go-datastore) for PostgreSQL that uses the [pgx](https://github.com/jackc/pgx) PostgreSQL driver.

**Note: Currently implements `Datastore` and `Batching` interfaces.**

## Install

```sh
go get github.com/ipfs/ipfs-ds-postgres
```

## Usage

Ensure a database is created and a table exists that has the following structure (replacing `table_name` with the name of the table the datastore will use - by default this is `blocks`):

```sql
CREATE TABLE IF NOT EXISTS table_name (key TEXT NOT NULL UNIQUE, data BYTEA)
```

It's recommended to create a `text_pattern_ops` index on the table:

```sql
CREATE INDEX IF NOT EXISTS table_name_key_text_pattern_ops_idx ON table_name (key text_pattern_ops)
```

Import and use in your application:

```go
package main

import (
	"context"
	pgds "github.com/alanshaw/ipfs-ds-postgres"
)

const (
	connString = "postgresql://user:pass@host:12345/database?sslmode=require"
	tableName  = "blocks" // (default)
)

func main() {
	ds, err := pgds.NewDatastore(context.Background(), connString, pgds.Table(tableName))
	if err != nil {
		panic(err)
	}
}
```

## API

[GoDoc Reference](https://godoc.org/github.com/alanshaw/ipfs-ds-postgres)

## Contribute

Feel free to dive in! [Open an issue](https://github.com/alanshaw/ipfs-ds-postgres/issues/new) or submit PRs.

## License

[MIT](LICENSE) © Alan Shaw
