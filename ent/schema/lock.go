// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/mixin"
	"github.com/google/uuid"
)

// LockMixin holds the mixin schema definition for the Lock entity.
type LockMixin struct {
	mixin.Schema
}

// Fields of the LockMixin.
func (LockMixin) Fields() []ent.Field {
	return []ent.Field{
		// The id for the Lock, immutable, globally unique
		field.String("id").Unique().Immutable().StorageKey("id"),

		// The version for the Lock, optional, mutable, globally unique
		field.UUID("version", uuid.UUID{}).Optional().Default(uuid.New).Unique(),

		// The owner for the Lock, mutable, optional
		field.String("owner"),
	}
}

// Edges of the LockMixin.
func (LockMixin) Edges() []ent.Edge {
	return nil
}

type Lock struct {
	ent.Schema
}

// Mixin of the Lock.
func (Lock) Mixin() []ent.Mixin {
	return []ent.Mixin{
		LockMixin{},
	}
}

// Annotations of the Lock.
func (Lock) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "dblock"},
	}
}
