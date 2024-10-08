// Code generated by ent, DO NOT EDIT.

package ent

import (
	"context"

	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/dialect/sql/sqlgraph"
	"entgo.io/ent/schema/field"
	"github.com/shivanshvij/dblock/ent/lock"
	"github.com/shivanshvij/dblock/ent/predicate"
)

// LockDelete is the builder for deleting a Lock entity.
type LockDelete struct {
	config
	hooks    []Hook
	mutation *LockMutation
}

// Where appends a list predicates to the LockDelete builder.
func (ld *LockDelete) Where(ps ...predicate.Lock) *LockDelete {
	ld.mutation.Where(ps...)
	return ld
}

// Exec executes the deletion query and returns how many vertices were deleted.
func (ld *LockDelete) Exec(ctx context.Context) (int, error) {
	return withHooks(ctx, ld.sqlExec, ld.mutation, ld.hooks)
}

// ExecX is like Exec, but panics if an error occurs.
func (ld *LockDelete) ExecX(ctx context.Context) int {
	n, err := ld.Exec(ctx)
	if err != nil {
		panic(err)
	}
	return n
}

func (ld *LockDelete) sqlExec(ctx context.Context) (int, error) {
	_spec := sqlgraph.NewDeleteSpec(lock.Table, sqlgraph.NewFieldSpec(lock.FieldID, field.TypeString))
	if ps := ld.mutation.predicates; len(ps) > 0 {
		_spec.Predicate = func(selector *sql.Selector) {
			for i := range ps {
				ps[i](selector)
			}
		}
	}
	affected, err := sqlgraph.DeleteNodes(ctx, ld.driver, _spec)
	if err != nil && sqlgraph.IsConstraintError(err) {
		err = &ConstraintError{msg: err.Error(), wrap: err}
	}
	ld.mutation.done = true
	return affected, err
}

// LockDeleteOne is the builder for deleting a single Lock entity.
type LockDeleteOne struct {
	ld *LockDelete
}

// Where appends a list predicates to the LockDelete builder.
func (ldo *LockDeleteOne) Where(ps ...predicate.Lock) *LockDeleteOne {
	ldo.ld.mutation.Where(ps...)
	return ldo
}

// Exec executes the deletion query.
func (ldo *LockDeleteOne) Exec(ctx context.Context) error {
	n, err := ldo.ld.Exec(ctx)
	switch {
	case err != nil:
		return err
	case n == 0:
		return &NotFoundError{lock.Label}
	default:
		return nil
	}
}

// ExecX is like Exec, but panics if an error occurs.
func (ldo *LockDeleteOne) ExecX(ctx context.Context) {
	if err := ldo.Exec(ctx); err != nil {
		panic(err)
	}
}
