package pgds

import (
	"context"
	"fmt"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// Datastore is a PostgreSQL backed datastore.
type Datastore struct {
	table string
	pool  *pgxpool.Pool
}

// NewDatastore creates a new PostgreSQL datastore
func NewDatastore(ctx context.Context, connString string, options ...Option) (*Datastore, error) {
	cfg := Options{}
	cfg.Apply(append([]Option{OptionDefaults}, options...)...)

	pool, err := pgxpool.Connect(ctx, connString)
	if err != nil {
		return nil, err
	}

	return &Datastore{table: cfg.Table, pool: pool}, nil
}

// PgxPool exposes the underlying pool of connections to Postgres.
func (d *Datastore) PgxPool() *pgxpool.Pool {
	return d.pool
}

// Close closes the underying PostgreSQL database.
func (d *Datastore) Close() error {
	if d.pool != nil {
		d.pool.Close()
	}
	return nil
}

// Delete removes a row from the PostgreSQL database by the given key.
func (d *Datastore) Delete(ctx context.Context, key ds.Key) error {
	sql := fmt.Sprintf("DELETE FROM %s WHERE key = $1", d.table)
	_, err := d.pool.Exec(ctx, sql, key.String())
	if err != nil {
		return err
	}
	return nil
}

// Get retrieves a value from the PostgreSQL database by the given key.
func (d *Datastore) Get(ctx context.Context, key ds.Key) (value []byte, err error) {
	sql := fmt.Sprintf("SELECT data FROM %s WHERE key = $1", d.table)
	row := d.pool.QueryRow(ctx, sql, key.String())
	var out []byte
	switch err := row.Scan(&out); err {
	case pgx.ErrNoRows:
		return nil, ds.ErrNotFound
	case nil:
		return out, nil
	default:
		return nil, err
	}
}

// Has determines if a value for the given key exists in the PostgreSQL database.
func (d *Datastore) Has(ctx context.Context, key ds.Key) (bool, error) {
	sql := fmt.Sprintf("SELECT exists(SELECT 1 FROM %s WHERE key = $1)", d.table)
	row := d.pool.QueryRow(ctx, sql, key.String())
	var exists bool
	switch err := row.Scan(&exists); err {
	case pgx.ErrNoRows:
		return exists, ds.ErrNotFound
	case nil:
		return exists, nil
	default:
		return exists, err
	}
}

// Put "upserts" a row into the SQL database.
func (d *Datastore) Put(ctx context.Context, key ds.Key, value []byte) error {
	sql := fmt.Sprintf("INSERT INTO %s (key, data) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET data = $2", d.table)
	_, err := d.pool.Exec(ctx, sql, key.String(), value)
	if err != nil {
		return err
	}
	return nil
}

// Query returns multiple rows from the SQL database based on the passed query parameters.
func (d *Datastore) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	var sql string
	if q.KeysOnly && q.ReturnsSizes {
		sql = fmt.Sprintf("SELECT key, octet_length(data) FROM %s", d.table)
	} else if q.KeysOnly {
		sql = fmt.Sprintf("SELECT key FROM %s", d.table)
	} else {
		sql = fmt.Sprintf("SELECT key, data FROM %s", d.table)
	}

	if q.Prefix != "" {
		// normalize
		prefix := ds.NewKey(q.Prefix).String()
		if prefix != "/" {
			sql += fmt.Sprintf(` WHERE key LIKE '%s%%' ORDER BY key`, prefix+"/")
		}
	}

	// only apply limit and offset if we do not have to naive filter/order the results
	if len(q.Filters) == 0 && len(q.Orders) == 0 {
		if q.Limit != 0 {
			sql += fmt.Sprintf(" LIMIT %d", q.Limit)
		}
		if q.Offset != 0 {
			sql += fmt.Sprintf(" OFFSET %d", q.Offset)
		}
	}

	rows, err := d.pool.Query(ctx, sql)
	if err != nil {
		return nil, err
	}

	it := dsq.Iterator{
		Next: func() (dsq.Result, bool) {
			if !rows.Next() {
				if rows.Err() != nil {
					return dsq.Result{Error: rows.Err()}, false
				}
				return dsq.Result{}, false
			}

			var key string
			var size int
			var data []byte

			if q.KeysOnly && q.ReturnsSizes {
				err := rows.Scan(&key, &size)
				if err != nil {
					return dsq.Result{Error: err}, false
				}
				return dsq.Result{Entry: dsq.Entry{Key: key, Size: size}}, true
			} else if q.KeysOnly {
				err := rows.Scan(&key)
				if err != nil {
					return dsq.Result{Error: err}, false
				}
				return dsq.Result{Entry: dsq.Entry{Key: key}}, true
			}

			err := rows.Scan(&key, &data)
			if err != nil {
				return dsq.Result{Error: err}, false
			}
			entry := dsq.Entry{Key: key, Value: data}
			if q.ReturnsSizes {
				entry.Size = len(data)
			}
			return dsq.Result{Entry: entry}, true
		},
		Close: func() error {
			rows.Close()
			return nil
		},
	}

	res := dsq.ResultsFromIterator(q, it)

	for _, f := range q.Filters {
		res = dsq.NaiveFilter(res, f)
	}

	res = dsq.NaiveOrder(res, q.Orders...)

	// if we have filters or orders, offset and limit won't have been applied in the query
	if len(q.Filters) > 0 || len(q.Orders) > 0 {
		if q.Offset != 0 {
			res = dsq.NaiveOffset(res, q.Offset)
		}
		if q.Limit != 0 {
			res = dsq.NaiveLimit(res, q.Limit)
		}
	}

	return res, nil
}

// Sync is noop for PostgreSQL databases.
func (d *Datastore) Sync(ctx context.Context, key ds.Key) error {
	return nil
}

// GetSize determines the size in bytes of the value for a given key.
func (d *Datastore) GetSize(ctx context.Context, key ds.Key) (int, error) {
	sql := fmt.Sprintf("SELECT octet_length(data) FROM %s WHERE key = $1", d.table)
	row := d.pool.QueryRow(ctx, sql, key.String())
	var size int
	switch err := row.Scan(&size); err {
	case pgx.ErrNoRows:
		return -1, ds.ErrNotFound
	case nil:
		return size, nil
	default:
		return -1, err
	}
}

var _ ds.Datastore = (*Datastore)(nil)
