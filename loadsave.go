package meddler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/satori/go.uuid"
)

type dbErr struct {
	msg string
	err error
}

func (err *dbErr) Error() string {
	return fmt.Sprintf("%s: %v", err.msg, err.err)
}

// DriverErr returns the original error as returned by the database driver
// if the error comes from the driver, with the second value set to true.
// Otherwise, it returns err itself with false as second value.
func DriverErr(err error) (error, bool) {
	if dbe, ok := err.(*dbErr); ok {
		return dbe.err, true
	}
	return err, false
}

// DB is a generic database interface, matching both *sql.Db and *sql.Tx
type DB interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// LoadContext loads a record using a query for the primary key field.
// Returns sql.ErrNoRows if not found.
func (d *Database) LoadContext(ctx context.Context, db DB, table string, dst interface{}, pk interface{}) error {
	columns, err := d.ColumnsQuoted(dst, true)
	if err != nil {
		return err
	}

	// make sure we have a primary key field
	p, err := d.PrimaryKey(dst)
	if err != nil {
		return err
	}
	if p.key == "" {
		return errors.New("meddler.Load: no primary key field found")
	}

	// run the query
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s = %s", columns, d.quoted(table), d.quoted(p.key), d.Placeholder)

	rows, err := db.QueryContext(ctx, q, pk)
	if err != nil {
		return &dbErr{msg: "meddler.Load: DB error in Query", err: err}
	}

	// scan the row
	return d.ScanRow(rows, dst)
}

// LoadContext using the Default Database type
func LoadContext(ctx context.Context, db DB, table string, dst interface{}, pk interface{}) error {
	return Default.LoadContext(ctx, db, table, dst, pk)
}

// InsertContext performs an INSERT query for the given record.
// If the record has a primary key flagged, it must be zero, and it
// will be set to the newly-allocated primary key value from the database
// as returned by LastInsertId.
func (d *Database) InsertContext(ctx context.Context, db DB, table string, src interface{}) error {
	pk, err := d.PrimaryKey(src)
	if err != nil {
		return err
	}

	if !pk.empty() {
		return errors.New("meddler.Insert: primary key must be empty")
	}

	// gather the query parts
	includePk := false
	if pk.valueType == PkString {
		includePk = true
		d.SetPrimaryKey(src, uuid.NewV4().String())
	}

	namesPart, err := d.ColumnsQuoted(src, includePk)
	if err != nil {
		return err
	}
	valuesPart, err := d.PlaceholdersString(src, includePk)
	if err != nil {
		return err
	}
	values, err := d.Values(src, includePk)
	if err != nil {
		return err
	}

	// run the query
	q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", d.quoted(table), namesPart, valuesPart)
	if pk.valueType == pkInt && d.UseReturningToGetID && pk.key != "" {
		q += " RETURNING " + d.quoted(pk.key)
		var newPk int64
		err := db.QueryRowContext(ctx, q, values...).Scan(&newPk)
		if err != nil {
			return &dbErr{msg: "meddler.Insert: DB error in QueryRow", err: err}
		}
		if err = d.SetPrimaryKey(src, newPk); err != nil {
			return fmt.Errorf("meddler.Insert: Error saving updated pk: %v", err)
		}
	} else if pk.valueType == pkInt && pk.key != "" {
		result, err := db.ExecContext(ctx, q, values...)
		if err != nil {
			return &dbErr{msg: "meddler.Insert: DB error in Exec", err: err}
		}

		// save the new primary key
		newPk, err := result.LastInsertId()
		if err != nil {
			return &dbErr{msg: "meddler.Insert: DB error getting new primary key value", err: err}
		}
		if err = d.SetPrimaryKey(src, newPk); err != nil {
			return fmt.Errorf("meddler.Insert: Error saving updated pk: %v", err)
		}
	} else {
		// no primary key, so no need to lookup new value
		_, err := db.ExecContext(ctx, q, values...)
		if err != nil {
			return &dbErr{msg: "meddler.Insert: DB error in Exec", err: err}
		}
	}

	return nil
}

// InsertContext using the Default Database type
func InsertContext(ctx context.Context, db DB, table string, src interface{}) error {
	return Default.InsertContext(ctx, db, table, src)
}

// UpdateContext performs and UPDATE query for the given record.
// The record must have an integer primary key field that is non-zero,
// and it will be used to select the database row that gets updated.
func (d *Database) UpdateContext(ctx context.Context, db DB, table string, src interface{}) error {
	// gather the query parts
	names, err := d.Columns(src, false)
	if err != nil {
		return err
	}
	placeholders, err := d.Placeholders(src, false)
	if err != nil {
		return err
	}
	values, err := d.Values(src, false)
	if err != nil {
		return err
	}

	// form the column=placeholder pairs
	var pairs []string
	for i := 0; i < len(names) && i < len(placeholders); i++ {
		pair := fmt.Sprintf("%s=%s", d.quoted(names[i]), placeholders[i])
		pairs = append(pairs, pair)
	}

	pk, err := d.PrimaryKey(src)
	if err != nil {
		return err
	}

	ph := d.placeholder(len(placeholders) + 1)

	// run the query

	q := fmt.Sprintf("UPDATE %s SET %s WHERE %s=%s", d.quoted(table),
		strings.Join(pairs, ","),
		d.quoted(pk.key), ph)

	switch pk.valueType {
	case pkInt:
		values = append(values, pk.valueInt)
	case PkString:
		values = append(values, pk.valueString)
	}

	if _, err := db.ExecContext(ctx, q, values...); err != nil {
		return &dbErr{msg: "meddler.Update: DB error in Exec", err: err}
	}

	return nil
}

// UpdateContext using the Default Database type
func UpdateContext(ctx context.Context, db DB, table string, src interface{}) error {
	return Default.UpdateContext(ctx, db, table, src)
}

// SaveContext performs an INSERT or an UPDATE, depending on whether or not
// a primary keys exists and is non-zero.
func (d *Database) SaveContext(ctx context.Context, db DB, table string, src interface{}) error {
	p, err := d.PrimaryKey(src)
	if err != nil {
		return err
	}

	if !p.empty() {
		return d.UpdateContext(ctx, db, table, src)
	}
	return d.InsertContext(ctx, db, table, src)
}

// SaveContext using the Default Database type
func SaveContext(ctx context.Context, db DB, table string, src interface{}) error {
	return Default.SaveContext(ctx, db, table, src)
}

// QueryRowContext performs the given query with the given arguments, scanning a
// single row of results into dst. Returns sql.ErrNoRows if there was no
// result row.
func (d *Database) QueryRowContext(ctx context.Context, db DB, dst interface{}, query string, args ...interface{}) error {
	// perform the query
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}

	// gather the result
	return d.ScanRow(rows, dst)
}

// QueryRowContext using the Default Database type
func QueryRowContext(ctx context.Context, db DB, dst interface{}, query string, args ...interface{}) error {
	return Default.QueryRowContext(ctx, db, dst, query, args...)
}

// QueryAllContext performs the given query with the given arguments, scanning
// all results rows into dst.
func (d *Database) QueryAllContext(ctx context.Context, db DB, dst interface{}, query string, args ...interface{}) error {
	// perform the query
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}

	// gather the results
	return d.ScanAll(rows, dst)
}

// QueryAllContext using the Default Database type
func QueryAllContext(ctx context.Context, db DB, dst interface{}, query string, args ...interface{}) error {
	return Default.QueryAllContext(ctx, db, dst, query, args...)
}
