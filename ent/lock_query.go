// Code generated by ent, DO NOT EDIT.

package ent

import (
	"context"
	"fmt"
	"math"

	"entgo.io/ent"
	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/dialect/sql/sqlgraph"
	"entgo.io/ent/schema/field"
	"github.com/shivanshvij/dblock/ent/lock"
	"github.com/shivanshvij/dblock/ent/predicate"
)

// LockQuery is the builder for querying Lock entities.
type LockQuery struct {
	config
	ctx        *QueryContext
	order      []lock.OrderOption
	inters     []Interceptor
	predicates []predicate.Lock
	modifiers  []func(*sql.Selector)
	// intermediate query (i.e. traversal path).
	sql  *sql.Selector
	path func(context.Context) (*sql.Selector, error)
}

// Where adds a new predicate for the LockQuery builder.
func (lq *LockQuery) Where(ps ...predicate.Lock) *LockQuery {
	lq.predicates = append(lq.predicates, ps...)
	return lq
}

// Limit the number of records to be returned by this query.
func (lq *LockQuery) Limit(limit int) *LockQuery {
	lq.ctx.Limit = &limit
	return lq
}

// Offset to start from.
func (lq *LockQuery) Offset(offset int) *LockQuery {
	lq.ctx.Offset = &offset
	return lq
}

// Unique configures the query builder to filter duplicate records on query.
// By default, unique is set to true, and can be disabled using this method.
func (lq *LockQuery) Unique(unique bool) *LockQuery {
	lq.ctx.Unique = &unique
	return lq
}

// Order specifies how the records should be ordered.
func (lq *LockQuery) Order(o ...lock.OrderOption) *LockQuery {
	lq.order = append(lq.order, o...)
	return lq
}

// First returns the first Lock entity from the query.
// Returns a *NotFoundError when no Lock was found.
func (lq *LockQuery) First(ctx context.Context) (*Lock, error) {
	nodes, err := lq.Limit(1).All(setContextOp(ctx, lq.ctx, ent.OpQueryFirst))
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, &NotFoundError{lock.Label}
	}
	return nodes[0], nil
}

// FirstX is like First, but panics if an error occurs.
func (lq *LockQuery) FirstX(ctx context.Context) *Lock {
	node, err := lq.First(ctx)
	if err != nil && !IsNotFound(err) {
		panic(err)
	}
	return node
}

// FirstID returns the first Lock ID from the query.
// Returns a *NotFoundError when no Lock ID was found.
func (lq *LockQuery) FirstID(ctx context.Context) (id string, err error) {
	var ids []string
	if ids, err = lq.Limit(1).IDs(setContextOp(ctx, lq.ctx, ent.OpQueryFirstID)); err != nil {
		return
	}
	if len(ids) == 0 {
		err = &NotFoundError{lock.Label}
		return
	}
	return ids[0], nil
}

// FirstIDX is like FirstID, but panics if an error occurs.
func (lq *LockQuery) FirstIDX(ctx context.Context) string {
	id, err := lq.FirstID(ctx)
	if err != nil && !IsNotFound(err) {
		panic(err)
	}
	return id
}

// Only returns a single Lock entity found by the query, ensuring it only returns one.
// Returns a *NotSingularError when more than one Lock entity is found.
// Returns a *NotFoundError when no Lock entities are found.
func (lq *LockQuery) Only(ctx context.Context) (*Lock, error) {
	nodes, err := lq.Limit(2).All(setContextOp(ctx, lq.ctx, ent.OpQueryOnly))
	if err != nil {
		return nil, err
	}
	switch len(nodes) {
	case 1:
		return nodes[0], nil
	case 0:
		return nil, &NotFoundError{lock.Label}
	default:
		return nil, &NotSingularError{lock.Label}
	}
}

// OnlyX is like Only, but panics if an error occurs.
func (lq *LockQuery) OnlyX(ctx context.Context) *Lock {
	node, err := lq.Only(ctx)
	if err != nil {
		panic(err)
	}
	return node
}

// OnlyID is like Only, but returns the only Lock ID in the query.
// Returns a *NotSingularError when more than one Lock ID is found.
// Returns a *NotFoundError when no entities are found.
func (lq *LockQuery) OnlyID(ctx context.Context) (id string, err error) {
	var ids []string
	if ids, err = lq.Limit(2).IDs(setContextOp(ctx, lq.ctx, ent.OpQueryOnlyID)); err != nil {
		return
	}
	switch len(ids) {
	case 1:
		id = ids[0]
	case 0:
		err = &NotFoundError{lock.Label}
	default:
		err = &NotSingularError{lock.Label}
	}
	return
}

// OnlyIDX is like OnlyID, but panics if an error occurs.
func (lq *LockQuery) OnlyIDX(ctx context.Context) string {
	id, err := lq.OnlyID(ctx)
	if err != nil {
		panic(err)
	}
	return id
}

// All executes the query and returns a list of Locks.
func (lq *LockQuery) All(ctx context.Context) ([]*Lock, error) {
	ctx = setContextOp(ctx, lq.ctx, ent.OpQueryAll)
	if err := lq.prepareQuery(ctx); err != nil {
		return nil, err
	}
	qr := querierAll[[]*Lock, *LockQuery]()
	return withInterceptors[[]*Lock](ctx, lq, qr, lq.inters)
}

// AllX is like All, but panics if an error occurs.
func (lq *LockQuery) AllX(ctx context.Context) []*Lock {
	nodes, err := lq.All(ctx)
	if err != nil {
		panic(err)
	}
	return nodes
}

// IDs executes the query and returns a list of Lock IDs.
func (lq *LockQuery) IDs(ctx context.Context) (ids []string, err error) {
	if lq.ctx.Unique == nil && lq.path != nil {
		lq.Unique(true)
	}
	ctx = setContextOp(ctx, lq.ctx, ent.OpQueryIDs)
	if err = lq.Select(lock.FieldID).Scan(ctx, &ids); err != nil {
		return nil, err
	}
	return ids, nil
}

// IDsX is like IDs, but panics if an error occurs.
func (lq *LockQuery) IDsX(ctx context.Context) []string {
	ids, err := lq.IDs(ctx)
	if err != nil {
		panic(err)
	}
	return ids
}

// Count returns the count of the given query.
func (lq *LockQuery) Count(ctx context.Context) (int, error) {
	ctx = setContextOp(ctx, lq.ctx, ent.OpQueryCount)
	if err := lq.prepareQuery(ctx); err != nil {
		return 0, err
	}
	return withInterceptors[int](ctx, lq, querierCount[*LockQuery](), lq.inters)
}

// CountX is like Count, but panics if an error occurs.
func (lq *LockQuery) CountX(ctx context.Context) int {
	count, err := lq.Count(ctx)
	if err != nil {
		panic(err)
	}
	return count
}

// Exist returns true if the query has elements in the graph.
func (lq *LockQuery) Exist(ctx context.Context) (bool, error) {
	ctx = setContextOp(ctx, lq.ctx, ent.OpQueryExist)
	switch _, err := lq.FirstID(ctx); {
	case IsNotFound(err):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("ent: check existence: %w", err)
	default:
		return true, nil
	}
}

// ExistX is like Exist, but panics if an error occurs.
func (lq *LockQuery) ExistX(ctx context.Context) bool {
	exist, err := lq.Exist(ctx)
	if err != nil {
		panic(err)
	}
	return exist
}

// Clone returns a duplicate of the LockQuery builder, including all associated steps. It can be
// used to prepare common query builders and use them differently after the clone is made.
func (lq *LockQuery) Clone() *LockQuery {
	if lq == nil {
		return nil
	}
	return &LockQuery{
		config:     lq.config,
		ctx:        lq.ctx.Clone(),
		order:      append([]lock.OrderOption{}, lq.order...),
		inters:     append([]Interceptor{}, lq.inters...),
		predicates: append([]predicate.Lock{}, lq.predicates...),
		// clone intermediate query.
		sql:       lq.sql.Clone(),
		path:      lq.path,
		modifiers: append([]func(*sql.Selector){}, lq.modifiers...),
	}
}

// GroupBy is used to group vertices by one or more fields/columns.
// It is often used with aggregate functions, like: count, max, mean, min, sum.
//
// Example:
//
//	var v []struct {
//		Version uuid.UUID `json:"version,omitempty"`
//		Count int `json:"count,omitempty"`
//	}
//
//	client.Lock.Query().
//		GroupBy(lock.FieldVersion).
//		Aggregate(ent.Count()).
//		Scan(ctx, &v)
func (lq *LockQuery) GroupBy(field string, fields ...string) *LockGroupBy {
	lq.ctx.Fields = append([]string{field}, fields...)
	grbuild := &LockGroupBy{build: lq}
	grbuild.flds = &lq.ctx.Fields
	grbuild.label = lock.Label
	grbuild.scan = grbuild.Scan
	return grbuild
}

// Select allows the selection one or more fields/columns for the given query,
// instead of selecting all fields in the entity.
//
// Example:
//
//	var v []struct {
//		Version uuid.UUID `json:"version,omitempty"`
//	}
//
//	client.Lock.Query().
//		Select(lock.FieldVersion).
//		Scan(ctx, &v)
func (lq *LockQuery) Select(fields ...string) *LockSelect {
	lq.ctx.Fields = append(lq.ctx.Fields, fields...)
	sbuild := &LockSelect{LockQuery: lq}
	sbuild.label = lock.Label
	sbuild.flds, sbuild.scan = &lq.ctx.Fields, sbuild.Scan
	return sbuild
}

// Aggregate returns a LockSelect configured with the given aggregations.
func (lq *LockQuery) Aggregate(fns ...AggregateFunc) *LockSelect {
	return lq.Select().Aggregate(fns...)
}

func (lq *LockQuery) prepareQuery(ctx context.Context) error {
	for _, inter := range lq.inters {
		if inter == nil {
			return fmt.Errorf("ent: uninitialized interceptor (forgotten import ent/runtime?)")
		}
		if trv, ok := inter.(Traverser); ok {
			if err := trv.Traverse(ctx, lq); err != nil {
				return err
			}
		}
	}
	for _, f := range lq.ctx.Fields {
		if !lock.ValidColumn(f) {
			return &ValidationError{Name: f, err: fmt.Errorf("ent: invalid field %q for query", f)}
		}
	}
	if lq.path != nil {
		prev, err := lq.path(ctx)
		if err != nil {
			return err
		}
		lq.sql = prev
	}
	return nil
}

func (lq *LockQuery) sqlAll(ctx context.Context, hooks ...queryHook) ([]*Lock, error) {
	var (
		nodes = []*Lock{}
		_spec = lq.querySpec()
	)
	_spec.ScanValues = func(columns []string) ([]any, error) {
		return (*Lock).scanValues(nil, columns)
	}
	_spec.Assign = func(columns []string, values []any) error {
		node := &Lock{config: lq.config}
		nodes = append(nodes, node)
		return node.assignValues(columns, values)
	}
	if len(lq.modifiers) > 0 {
		_spec.Modifiers = lq.modifiers
	}
	for i := range hooks {
		hooks[i](ctx, _spec)
	}
	if err := sqlgraph.QueryNodes(ctx, lq.driver, _spec); err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nodes, nil
	}
	return nodes, nil
}

func (lq *LockQuery) sqlCount(ctx context.Context) (int, error) {
	_spec := lq.querySpec()
	if len(lq.modifiers) > 0 {
		_spec.Modifiers = lq.modifiers
	}
	_spec.Node.Columns = lq.ctx.Fields
	if len(lq.ctx.Fields) > 0 {
		_spec.Unique = lq.ctx.Unique != nil && *lq.ctx.Unique
	}
	return sqlgraph.CountNodes(ctx, lq.driver, _spec)
}

func (lq *LockQuery) querySpec() *sqlgraph.QuerySpec {
	_spec := sqlgraph.NewQuerySpec(lock.Table, lock.Columns, sqlgraph.NewFieldSpec(lock.FieldID, field.TypeString))
	_spec.From = lq.sql
	if unique := lq.ctx.Unique; unique != nil {
		_spec.Unique = *unique
	} else if lq.path != nil {
		_spec.Unique = true
	}
	if fields := lq.ctx.Fields; len(fields) > 0 {
		_spec.Node.Columns = make([]string, 0, len(fields))
		_spec.Node.Columns = append(_spec.Node.Columns, lock.FieldID)
		for i := range fields {
			if fields[i] != lock.FieldID {
				_spec.Node.Columns = append(_spec.Node.Columns, fields[i])
			}
		}
	}
	if ps := lq.predicates; len(ps) > 0 {
		_spec.Predicate = func(selector *sql.Selector) {
			for i := range ps {
				ps[i](selector)
			}
		}
	}
	if limit := lq.ctx.Limit; limit != nil {
		_spec.Limit = *limit
	}
	if offset := lq.ctx.Offset; offset != nil {
		_spec.Offset = *offset
	}
	if ps := lq.order; len(ps) > 0 {
		_spec.Order = func(selector *sql.Selector) {
			for i := range ps {
				ps[i](selector)
			}
		}
	}
	return _spec
}

func (lq *LockQuery) sqlQuery(ctx context.Context) *sql.Selector {
	builder := sql.Dialect(lq.driver.Dialect())
	t1 := builder.Table(lock.Table)
	columns := lq.ctx.Fields
	if len(columns) == 0 {
		columns = lock.Columns
	}
	selector := builder.Select(t1.Columns(columns...)...).From(t1)
	if lq.sql != nil {
		selector = lq.sql
		selector.Select(selector.Columns(columns...)...)
	}
	if lq.ctx.Unique != nil && *lq.ctx.Unique {
		selector.Distinct()
	}
	for _, m := range lq.modifiers {
		m(selector)
	}
	for _, p := range lq.predicates {
		p(selector)
	}
	for _, p := range lq.order {
		p(selector)
	}
	if offset := lq.ctx.Offset; offset != nil {
		// limit is mandatory for offset clause. We start
		// with default value, and override it below if needed.
		selector.Offset(*offset).Limit(math.MaxInt32)
	}
	if limit := lq.ctx.Limit; limit != nil {
		selector.Limit(*limit)
	}
	return selector
}

// Modify adds a query modifier for attaching custom logic to queries.
func (lq *LockQuery) Modify(modifiers ...func(s *sql.Selector)) *LockSelect {
	lq.modifiers = append(lq.modifiers, modifiers...)
	return lq.Select()
}

// LockGroupBy is the group-by builder for Lock entities.
type LockGroupBy struct {
	selector
	build *LockQuery
}

// Aggregate adds the given aggregation functions to the group-by query.
func (lgb *LockGroupBy) Aggregate(fns ...AggregateFunc) *LockGroupBy {
	lgb.fns = append(lgb.fns, fns...)
	return lgb
}

// Scan applies the selector query and scans the result into the given value.
func (lgb *LockGroupBy) Scan(ctx context.Context, v any) error {
	ctx = setContextOp(ctx, lgb.build.ctx, ent.OpQueryGroupBy)
	if err := lgb.build.prepareQuery(ctx); err != nil {
		return err
	}
	return scanWithInterceptors[*LockQuery, *LockGroupBy](ctx, lgb.build, lgb, lgb.build.inters, v)
}

func (lgb *LockGroupBy) sqlScan(ctx context.Context, root *LockQuery, v any) error {
	selector := root.sqlQuery(ctx).Select()
	aggregation := make([]string, 0, len(lgb.fns))
	for _, fn := range lgb.fns {
		aggregation = append(aggregation, fn(selector))
	}
	if len(selector.SelectedColumns()) == 0 {
		columns := make([]string, 0, len(*lgb.flds)+len(lgb.fns))
		for _, f := range *lgb.flds {
			columns = append(columns, selector.C(f))
		}
		columns = append(columns, aggregation...)
		selector.Select(columns...)
	}
	selector.GroupBy(selector.Columns(*lgb.flds...)...)
	if err := selector.Err(); err != nil {
		return err
	}
	rows := &sql.Rows{}
	query, args := selector.Query()
	if err := lgb.build.driver.Query(ctx, query, args, rows); err != nil {
		return err
	}
	defer rows.Close()
	return sql.ScanSlice(rows, v)
}

// LockSelect is the builder for selecting fields of Lock entities.
type LockSelect struct {
	*LockQuery
	selector
}

// Aggregate adds the given aggregation functions to the selector query.
func (ls *LockSelect) Aggregate(fns ...AggregateFunc) *LockSelect {
	ls.fns = append(ls.fns, fns...)
	return ls
}

// Scan applies the selector query and scans the result into the given value.
func (ls *LockSelect) Scan(ctx context.Context, v any) error {
	ctx = setContextOp(ctx, ls.ctx, ent.OpQuerySelect)
	if err := ls.prepareQuery(ctx); err != nil {
		return err
	}
	return scanWithInterceptors[*LockQuery, *LockSelect](ctx, ls.LockQuery, ls, ls.inters, v)
}

func (ls *LockSelect) sqlScan(ctx context.Context, root *LockQuery, v any) error {
	selector := root.sqlQuery(ctx)
	aggregation := make([]string, 0, len(ls.fns))
	for _, fn := range ls.fns {
		aggregation = append(aggregation, fn(selector))
	}
	switch n := len(*ls.selector.flds); {
	case n == 0 && len(aggregation) > 0:
		selector.Select(aggregation...)
	case n != 0 && len(aggregation) > 0:
		selector.AppendSelect(aggregation...)
	}
	rows := &sql.Rows{}
	query, args := selector.Query()
	if err := ls.driver.Query(ctx, query, args, rows); err != nil {
		return err
	}
	defer rows.Close()
	return sql.ScanSlice(rows, v)
}

// Modify adds a query modifier for attaching custom logic to queries.
func (ls *LockSelect) Modify(modifiers ...func(s *sql.Selector)) *LockSelect {
	ls.modifiers = append(ls.modifiers, modifiers...)
	return ls
}
