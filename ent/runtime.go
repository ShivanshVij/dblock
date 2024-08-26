// Code generated by ent, DO NOT EDIT.

package ent

import (
	"github.com/google/uuid"
	"github.com/shivanshvij/dblock/ent/lock"
	"github.com/shivanshvij/dblock/ent/schema"
)

// The init function reads all schema descriptors with runtime code
// (default values, validators, hooks and policies) and stitches it
// to their package variables.
func init() {
	lockFields := schema.Lock{}.Fields()
	_ = lockFields
	// lockDescVersionID is the schema descriptor for versionID field.
	lockDescVersionID := lockFields[1].Descriptor()
	// lock.DefaultVersionID holds the default value on creation for the versionID field.
	lock.DefaultVersionID = lockDescVersionID.Default.(func() uuid.UUID)
}
