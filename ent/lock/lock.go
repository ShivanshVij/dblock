// Code generated by ent, DO NOT EDIT.

package lock

import (
	"entgo.io/ent/dialect/sql"
	"github.com/google/uuid"
)

const (
	// Label holds the string label denoting the lock type in the database.
	Label = "lock"
	// FieldID holds the string denoting the id field in the database.
	FieldID = "id"
	// FieldVersion holds the string denoting the version field in the database.
	FieldVersion = "version"
	// FieldOwner holds the string denoting the owner field in the database.
	FieldOwner = "owner"
	// Table holds the table name of the lock in the database.
	Table = "dblock"
)

// Columns holds all SQL columns for lock fields.
var Columns = []string{
	FieldID,
	FieldVersion,
	FieldOwner,
}

// ValidColumn reports if the column name is valid (part of the table columns).
func ValidColumn(column string) bool {
	for i := range Columns {
		if column == Columns[i] {
			return true
		}
	}
	return false
}

var (
	// DefaultVersion holds the default value on creation for the "version" field.
	DefaultVersion func() uuid.UUID
)

// OrderOption defines the ordering options for the Lock queries.
type OrderOption func(*sql.Selector)

// ByID orders the results by the id field.
func ByID(opts ...sql.OrderTermOption) OrderOption {
	return sql.OrderByField(FieldID, opts...).ToFunc()
}

// ByVersion orders the results by the version field.
func ByVersion(opts ...sql.OrderTermOption) OrderOption {
	return sql.OrderByField(FieldVersion, opts...).ToFunc()
}

// ByOwner orders the results by the owner field.
func ByOwner(opts ...sql.OrderTermOption) OrderOption {
	return sql.OrderByField(FieldOwner, opts...).ToFunc()
}
