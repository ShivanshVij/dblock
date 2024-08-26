// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"github.com/google/uuid"
)

// Lock holds the schema definition for the Lock entity.
type Lock struct {
	ent.Schema
}

// Fields of the Lock.
func (Lock) Fields() []ent.Field {
	return []ent.Field{
		// The id for the Lock, immutable, globally unique
		field.String("id").Unique().Immutable().StorageKey("id"),

		// The version for the Lock, optional, mutable, globally unique
		field.UUID("version", uuid.UUID{}).Optional().Default(uuid.New).Unique(),

		// The owner for the Lock, mutable, optional
		field.String("owner"),
	}
}

// Annotations of the Lock.
func (Lock) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "dblock"},
	}
}

// Edges of the Lock.
func (Lock) Edges() []ent.Edge {
	return nil
}
