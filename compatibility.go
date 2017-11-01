package meddler

import (
	"context"
	"database/sql"
)

type ContextDB interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// Load loads a record using a query for the primary key field.
// Returns sql.ErrNoRows if not found.
func (d *Database) Load(db DB, table string, dst interface{}, pk interface{}) error {
	return d.LoadContext(context.Background(), db, table, dst, pk)
}

// Load using the Default Database type
func Load(db DB, table string, dst interface{}, pk interface{}) error {
	return LoadContext(context.Background(), db, table, dst, pk)
}

// Insert performs an INSERT query for the given record.
// If the record has a primary key flagged, it must be zero, and it
// will be set to the newly-allocated primary key value from the database
// as returned by LastInsertId.
func (d *Database) Insert(db DB, table string, src interface{}) error {
	return d.InsertContext(context.Background(), db, table, src)
}

// Insert using the Default Database type
func Insert(db DB, table string, src interface{}) error {
	return InsertContext(context.Background(), db, table, src)
}

// Update performs and UPDATE query for the given record.
// The record must have an integer primary key field that is non-zero,
// and it will be used to select the database row that gets updated.
func (d *Database) Update(db DB, table string, src interface{}) error {
	return d.UpdateContext(context.Background(), db, table, src)
}

// Update using the Default Database type
func Update(db DB, table string, src interface{}) error {
	return UpdateContext(context.Background(), db, table, src)
}

// Save performs an INSERT or an UPDATE, depending on whether or not
// a primary keys exists and is non-zero.
func (d *Database) Save(db DB, table string, src interface{}) error {
	return d.SaveContext(context.Background(), db, table, src)
}

// Save using the Default Database type
func Save(db DB, table string, src interface{}) error {
	return SaveContext(context.Background(), db, table, src)
}

// QueryRow performs the given query with the given arguments, scanning a
// single row of results into dst. Returns sql.ErrNoRows if there was no
// result row.
func (d *Database) QueryRow(db DB, dst interface{}, query string, args ...interface{}) error {
	return d.QueryRowContext(context.Background(), db, dst, query, args...)
}

// QueryRow using the Default Database type
func QueryRow(db DB, dst interface{}, query string, args ...interface{}) error {
	return QueryRowContext(context.Background(), db, dst, query, args...)
}

// QueryAll performs the given query with the given arguments, scanning
// all results rows into dst.
func (d *Database) QueryAll(db DB, dst interface{}, query string, args ...interface{}) error {
	return d.QueryAllContext(context.Background(), db, dst, query, args...)
}

// QueryAll using the Default Database type
func QueryAll(db DB, dst interface{}, query string, args ...interface{}) error {
	return QueryAllContext(context.Background(), db, dst, query, args...)
}
