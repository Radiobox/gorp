package gorp

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
)

// A Filter is a type that can be used as a sub-section of a where
// clause.
type Filter interface {
	// Where should take a structColumnMap, a dialect, and the index
	// to start binding at, and return the string to be added to the
	// where clause, a slice of query arguments in the where clause,
	// and any errors encountered.
	Where(structMap structColumnMap, dialect Dialect, startBindIdx int) (string, []interface{}, error)
}

// A combinedFilter is a filter that has more than one sub-filter.
// This is mainly for things like AND or OR operations.
type combinedFilter struct {
	subFilters []Filter
}

// joinFilters joins all of the sub-filters' where clauses into a
// single where clause.
func (filter *combinedFilter) joinFilters(separator string, structMap structColumnMap, dialect Dialect, startBindIdx int) (string, []interface{}, error) {
	buffer := bytes.Buffer{}
	args := make([]interface{}, 0, len(filter.subFilters))
	if len(filter.subFilters) > 1 {
		buffer.WriteString("(")
	}
	for index, subFilter := range filter.subFilters {
		nextWhere, nextArgs, err := subFilter.Where(structMap, dialect, startBindIdx+len(args))
		if err != nil {
			return "", nil, err
		}
		args = append(args, nextArgs...)
		if index != 0 {
			buffer.WriteString(separator)
		}
		buffer.WriteString(nextWhere)
	}
	if len(filter.subFilters) > 1 {
		buffer.WriteString(")")
	}
	return buffer.String(), args, nil
}

// Add adds one or more filters to the slice of sub-filters.
func (filter *combinedFilter) Add(filters ...Filter) {
	filter.subFilters = append(filter.subFilters, filters...)
}

// An andFilter is a combinedFilter that will have its sub-filters
// joined using AND.
type andFilter struct {
	combinedFilter
}

func (filter *andFilter) Where(structMap structColumnMap, dialect Dialect, startBindIdx int) (string, []interface{}, error) {
	return filter.joinFilters(" and ", structMap, dialect, startBindIdx)
}

// An orFilter is a combinedFilter that will have its sub-filters
// joined using OR.
type orFilter struct {
	combinedFilter
}

func (filter *orFilter) Where(structMap structColumnMap, dialect Dialect, startBindIdx int) (string, []interface{}, error) {
	return filter.joinFilters(" or ", structMap, dialect, startBindIdx)
}

// A comparisonFilter is a filter that compares a field to a value.
type comparisonFilter struct {
	addr       interface{}
	comparison string
	value      interface{}
}

func (filter *comparisonFilter) Where(structMap structColumnMap, dialect Dialect, startBindIdx int) (string, []interface{}, error) {
	column, err := structMap.columnForPointer(filter.addr)
	if err != nil {
		return "", nil, err
	}
	bindVar := dialect.BindVar(startBindIdx)
	return column + filter.comparison + bindVar, []interface{}{filter.value}, nil
}

// A notFilter is a filter that inverts another filter.
type notFilter struct {
	filter Filter
}

func (filter *notFilter) Where(structMap structColumnMap, dialect Dialect, startBindIdx int) (string, []interface{}, error) {
	whereStr, args, err := filter.filter.Where(structMap, dialect, startBindIdx)
	if err != nil {
		return "", nil, err
	}
	return "NOT " + whereStr, args, nil
}

// A nullFilter is a filter that compares a field to null
type nullFilter struct {
	addr interface{}
}

func (filter *nullFilter) Where(structMap structColumnMap, dialect Dialect, startBindIdx int) (string, []interface{}, error) {
	column, err := structMap.columnForPointer(filter.addr)
	if err != nil {
		return "", nil, err
	}
	return column + " IS NULL", nil, nil
}

// A notNullFilter is a filter that compares a field to null
type notNullFilter struct {
	addr interface{}
}

func (filter *notNullFilter) Where(structMap structColumnMap, dialect Dialect, startBindIdx int) (string, []interface{}, error) {
	column, err := structMap.columnForPointer(filter.addr)
	if err != nil {
		return "", nil, err
	}
	return column + " IS NOT NULL", nil, nil
}

// Or returns a filter that will OR all passed in filters
func Or(filters ...Filter) Filter {
	return &orFilter{combinedFilter{filters}}
}

// And returns a filter that will AND all passed in filters
func And(filters ...Filter) Filter {
	return &andFilter{combinedFilter{filters}}
}

// Not returns a filter that will NOT the passed in filter
func Not(filter Filter) Filter {
	return &notFilter{filter}
}

// Null returns a filter for fieldPtr IS NULL
func Null(fieldPtr interface{}) Filter {
	return &nullFilter{fieldPtr}
}

// NotNull returns a filter for fieldPtr IS NOT NULL
func NotNull(fieldPtr interface{}) Filter {
	return &notNullFilter{fieldPtr}
}

// Equal returns a filter for fieldPtr == value
func Equal(fieldPtr interface{}, value interface{}) Filter {
	return &comparisonFilter{fieldPtr, "=", value}
}

// NotEqual returns a filter for fieldPtr != value
func NotEqual(fieldPtr interface{}, value interface{}) Filter {
	return &comparisonFilter{fieldPtr, "!=", value}
}

// Less returns a filter for fieldPtr < value
func Less(fieldPtr interface{}, value interface{}) Filter {
	return &comparisonFilter{fieldPtr, "<", value}
}

// LessOrEqual returns a filter for fieldPtr <= value
func LessOrEqual(fieldPtr interface{}, value interface{}) Filter {
	return &comparisonFilter{fieldPtr, "<=", value}
}

// Greater returns a filter for fieldPtr > value
func Greater(fieldPtr interface{}, value interface{}) Filter {
	return &comparisonFilter{fieldPtr, "=", value}
}

// GreaterOrEqual returns a filter for fieldPtr >= value
func GreaterOrEqual(fieldPtr interface{}, value interface{}) Filter {
	return &comparisonFilter{fieldPtr, "=", value}
}

// An Updater is a query that can execute UPDATE statements.
type Updater interface {
	Update() (rowsUpdated int64, err error)
}

// A Deleter is a query that can execute DELETE statements.
type Deleter interface {
	Delete() (rowsDeleted int64, err error)
}

// An Inserter is a query that can execute INSERT statements.
type Inserter interface {
	Insert() error
}

// A Selector is a query that can execute SELECT statements.
type Selector interface {
	Select() (results []interface{}, err error)
}

// A Receiver is a query that can execute statements with ORDER BY and
// GROUP BY clauses.
type Receiver interface {
	OrderBy(fieldPtr interface{}, direction string) SelectQuery
	GroupBy(fieldPtr interface{}) SelectQuery
	Limit(int64) SelectQuery
	Offset(int64) SelectQuery
}

// An Assigner is a query that can set columns to values.
type Assigner interface {
	Assign(fieldPtr interface{}, value interface{}) AssignQuery
}

// A Wherer is a query that can execute statements with a WHERE
// clause.
type Wherer interface {
	Where() WhereQuery
}

// A SelectQuery is a query that can only execute SELECT statements.
type SelectQuery interface {
	Receiver
	Selector
}

// An UpdateQuery is a query that can only execute UPDATE statements.
type UpdateQuery interface {
	// Filter is used for queries that are more complex than a few
	// ANDed constraints.
	Filter(Filter) UpdateQuery

	// Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual,
	// and NotNull are all what you would expect.  Use them for adding
	// constraints to a query.  More than one constraint will be ANDed
	// together.
	Equal(fieldPtr interface{}, value interface{}) UpdateQuery
	NotEqual(fieldPtr interface{}, value interface{}) UpdateQuery
	Less(fieldPtr interface{}, value interface{}) UpdateQuery
	LessOrEqual(fieldPtr interface{}, value interface{}) UpdateQuery
	Greater(fieldPtr interface{}, value interface{}) UpdateQuery
	GreaterOrEqual(fieldPtr interface{}, value interface{}) UpdateQuery
	NotNull(fieldPtr interface{}) UpdateQuery
	Null(fieldPtr interface{}) UpdateQuery

	// An UpdateQuery has both assignments and a where clause, which
	// means the only query type it could be is an UPDATE statement.
	Updater
}

// An AssignQuery is a query that may set values.
type AssignQuery interface {
	Assigner
	Where() UpdateQuery
	Inserter
	Updater
}

// A WhereQuery is a query that does not set any values, but may have
// a where clause.
type WhereQuery interface {
	// Filter is used for queries that are more complex than a few
	// ANDed constraints.
	Filter(Filter) WhereQuery

	// Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual,
	// and NotNull are all what you would expect.  Use them for adding
	// constraints to a query.  More than one constraint will be ANDed
	// together.
	Equal(fieldPtr interface{}, value interface{}) WhereQuery
	NotEqual(fieldPtr interface{}, value interface{}) WhereQuery
	Less(fieldPtr interface{}, value interface{}) WhereQuery
	LessOrEqual(fieldPtr interface{}, value interface{}) WhereQuery
	Greater(fieldPtr interface{}, value interface{}) WhereQuery
	GreaterOrEqual(fieldPtr interface{}, value interface{}) WhereQuery
	NotNull(fieldPtr interface{}) WhereQuery
	Null(fieldPtr interface{}) WhereQuery

	// A WhereQuery should be used when a where clause was requested
	// right off the bat, which means there have been no calls to
	// Assign.  Only delete and select statements can have a where
	// clause without doing assignment.
	Receiver
	Deleter
	Selector
}

// A Query is the base query type - as methods are called, the type of
// query will gradually be restricted based on which types of queries
// are capable of performing the requested operations.
//
// For example, UPDATE statements may both set values and have a where
// clause, but SELECT and DELETE statements cannot set values, and
// INSERT statements cannot have a WHERE clause.  SELECT statements
// are the only types that can have a GROUP BY, ORDER BY, or LIMIT
// clause.
//
// Because of this design, the following would actually be a compile
// error:
//
//     t := new(myType)
//     q, err := dbmap.Query(t).
//         Assign(&t.Foo, "test").
//         Where().
//         Less(&t.Created, time.Now()).
//         Insert()
//
// Since the return value from Assign() is an AssignQuery, the return value
// from Where() will be an UpdateQuery, which doesn't have an Insert()
// method.
type Query interface {
	// A query that has had no methods called can both perform
	// assignments and still have a where clause.
	Assigner
	Wherer

	// Updates and inserts need at least one assignment, so they won't
	// be allowed until Assign has been called.  However, select and
	// delete statements can be called without any where clause, so
	// they are allowed here.
	//
	// We should probably have a configuration variable to determine
	// whether delete statements without a where clause are allowed,
	// to prevent people from just deleting everything in their table.
	// On the other hand, they should be checking the count they get
	// back to ensure they deleted exactly what they wanted to delete.
	Receiver
	Deleter
	Selector
}

type fieldColumnMap struct {
	// addr should be the address (pointer value) of the field within
	// the struct being used to construct this query.
	addr interface{}

	// column should be the column that matches the field that addr
	// points to.
	column *ColumnMap

	// quotedColumn should be the pre-quoted column string for this
	// column.
	quotedColumn string
}

type structColumnMap []fieldColumnMap

// columnForPointer takes an interface value (which should be a
// pointer to one of the fields on the value that is being used as a
// reference for query construction) and returns the pre-quoted column
// name that should be used to reference that value in queries.
func (structMap structColumnMap) columnForPointer(fieldPtr interface{}) (string, error) {
	for _, fieldMap := range structMap {
		if fieldMap.addr == fieldPtr {
			if fieldMap.column.Transient {
				return "", errors.New("gorp: Cannot run queries against transient columns")
			}
			return fieldMap.quotedColumn, nil
		}
	}
	return "", errors.New("gorp: Cannot find a field matching the passed in pointer")
}

// A QueryPlan is a Query.  It returns itself on most method calls;
// the one exception is Assign(), which returns an AssignQueryPlan (a type of
// QueryPlan that implements AssignQuery instead of Query).  The return
// types of the methods on this struct help prevent silly errors like
// trying to run a SELECT statement that tries to Assign() values - that
// type of nonsense will result in compile errors.
//
// QueryPlans must be prepared and executed using an allocated struct
// as reference.  Again, this is intended to catch stupid mistakes
// (like typos in column names) at compile time.  Unfortunately, it
// makes the syntax a little unintuitive; but I haven't been able to
// come up with a better way to do it.
//
// For details about what you need in order to generate a query with
// this logic, see DbMap.Query().
type QueryPlan struct {
	// Errors is a slice of error valuues encountered during query
	// construction.  This is to allow cascading method calls, e.g.
	//
	//     someModel := new(OurModel)
	//     results, err := dbMap.Query(someModel).
	//         Where().
	//         Greater(&someModel.CreatedAt, yesterday).
	//         Less(&someModel.CreatedAt, time.Now()).
	//         Order(&someModel.CreatedAt, gorp.Descending).
	//         Select()
	//
	// The first time that a method call returns an error (most likely
	// Select(), Insert(), Delete(), or Update()), this field will be
	// checked for errors that occurred during query construction, and
	// if it is non-empty, the first error in the list will be
	// returned immediately.
	Errors []error

	table          *TableMap
	executor       SqlExecutor
	target         reflect.Value
	targetColMap   structColumnMap
	assignCols     []string
	assignBindVars []string
	filters        *andFilter
	orderBy        []string
	groupBy        []string
	limit          int64
	offset         int64
	args           []interface{}
}

// query generates a Query for a target model.  The target that is
// passed in must be a pointer to a struct, and will be used as a
// reference for query construction.
func query(m *DbMap, exec SqlExecutor, target interface{}) Query {
	plan := &QueryPlan{
		executor: exec,
	}

	targetVal := reflect.ValueOf(target)
	if targetVal.Kind() != reflect.Ptr || targetVal.Elem().Kind() != reflect.Struct {
		plan.Errors = append(plan.Errors, errors.New("gorp: Cannot create query plan - target value must be a pointer to a struct"))
		return plan
	}
	plan.target = targetVal

	targetTable, err := m.tableFor(plan.target.Type().Elem(), false)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return plan
	}
	plan.table = targetTable

	if err = plan.mapColumns(plan.target); err != nil {
		plan.Errors = append(plan.Errors, err)
	}
	return plan
}

// mapColumns creates a list of field addresses and column maps, to
// make looking up the column for a field address easier.  Note that
// it doesn't do any special handling for overridden fields, because
// passing the address of a field that has been overridden is
// difficult to do accidentally.
func (plan *QueryPlan) mapColumns(value reflect.Value) (err error) {
	value = value.Elem()
	valueType := value.Type()
	if plan.targetColMap == nil {
		plan.targetColMap = make(structColumnMap, 0, value.NumField())
	}
	for i := 0; i < value.NumField(); i++ {
		fieldType := valueType.Field(i)
		fieldVal := value.Field(i)
		if fieldType.Anonymous {
			if fieldVal.Kind() != reflect.Ptr {
				fieldVal = fieldVal.Addr()
			}
			plan.mapColumns(fieldVal)
		} else {
			col := plan.table.ColMap(fieldType.Name)
			quotedCol := plan.table.dbmap.Dialect.QuoteField(col.ColumnName)
			fieldMap := fieldColumnMap{
				addr:         fieldVal.Addr().Interface(),
				column:       col,
				quotedColumn: quotedCol,
			}
			plan.targetColMap = append(plan.targetColMap, fieldMap)
		}
	}
	return
}

// Assign sets up an assignment operation to assign the passed in
// value to the passed in field pointer.  This is used for creating
// UPDATE or INSERT queries.
func (plan *QueryPlan) Assign(fieldPtr interface{}, value interface{}) AssignQuery {
	assignPlan := &AssignQueryPlan{QueryPlan: *plan}
	return assignPlan.Assign(fieldPtr, value)
}

// Where doesn't do anything more than simply switching to where
// clause generation.  This is mainly here to make syntax cleaner,
// because queries are harder to read without it.
func (plan *QueryPlan) Where() WhereQuery {
	return plan
}

// Filter will add a Filter to the list of filters on this query.  The
// default method of combining filters on a query is by AND - if you
// want OR, you can use the following syntax:
//
//     q = q.Filter(gorp.Or(gorp.Equal(&field.Id, id), gorp.Less(&field.Priority, 3)))
//
func (plan *QueryPlan) Filter(filter Filter) WhereQuery {
	if plan.filters == nil {
		plan.filters = new(andFilter)
	}
	plan.filters.Add(filter)
	return plan
}

// Equal adds a column = value comparison to the where clause.
func (plan *QueryPlan) Equal(fieldPtr interface{}, value interface{}) WhereQuery {
	return plan.Filter(Equal(fieldPtr, value))
}

// NotEqual adds a column != value comparison to the where clause.
func (plan *QueryPlan) NotEqual(fieldPtr interface{}, value interface{}) WhereQuery {
	return plan.Filter(NotEqual(fieldPtr, value))
}

// Less adds a column < value comparison to the where clause.
func (plan *QueryPlan) Less(fieldPtr interface{}, value interface{}) WhereQuery {
	return plan.Filter(Less(fieldPtr, value))
}

// LessOrEqual adds a column <= value comparison to the where clause.
func (plan *QueryPlan) LessOrEqual(fieldPtr interface{}, value interface{}) WhereQuery {
	return plan.Filter(LessOrEqual(fieldPtr, value))
}

// Greater adds a column > value comparison to the where clause.
func (plan *QueryPlan) Greater(fieldPtr interface{}, value interface{}) WhereQuery {
	return plan.Filter(Greater(fieldPtr, value))
}

// GreaterOrEqual adds a column >= value comparison to the where clause.
func (plan *QueryPlan) GreaterOrEqual(fieldPtr interface{}, value interface{}) WhereQuery {
	return plan.Filter(GreaterOrEqual(fieldPtr, value))
}

// Null adds a column IS NULL comparison to the where clause
func (plan *QueryPlan) Null(fieldPtr interface{}) WhereQuery {
	return plan.Filter(Null(fieldPtr))
}

// NotNull adds a column IS NOT NULL comparison to the where clause
func (plan *QueryPlan) NotNull(fieldPtr interface{}) WhereQuery {
	return plan.Filter(NotNull(fieldPtr))
}

// OrderBy adds a column to the order by clause.  The direction is
// optional - you may pass in an empty string to order in the default
// direction for the given column.
func (plan *QueryPlan) OrderBy(fieldPtr interface{}, direction string) SelectQuery {
	column, err := plan.targetColMap.columnForPointer(fieldPtr)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return plan
	}
	switch strings.ToLower(direction) {
	case "asc", "desc":
	case "":
	default:
		plan.Errors = append(plan.Errors, errors.New(`gorp: Order by direction must be empty string, "asc", or "desc"`))
		return plan
	}
	plan.orderBy = append(plan.orderBy, column)
	return plan
}

// GroupBy adds a column to the group by clause.
func (plan *QueryPlan) GroupBy(fieldPtr interface{}) SelectQuery {
	column, err := plan.targetColMap.columnForPointer(fieldPtr)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return plan
	}
	plan.groupBy = append(plan.groupBy, column)
	return plan
}

// Limit sets the limit clause of the query.
func (plan *QueryPlan) Limit(limit int64) SelectQuery {
	plan.limit = limit
	return plan
}

// Offset sets the offset clause of the query.
func (plan *QueryPlan) Offset(offset int64) SelectQuery {
	plan.offset = offset
	return plan
}

func (plan *QueryPlan) whereClause() (string, error) {
	where, whereArgs, err := plan.filters.Where(plan.targetColMap, plan.table.dbmap.Dialect, len(plan.args))
	if err != nil {
		return "", err
	}
	if where != "" {
		plan.args = append(plan.args, whereArgs...)
		return " where " + where, nil
	}
	return "", nil
}

// Select will run this query plan as a SELECT statement.
func (plan *QueryPlan) Select() ([]interface{}, error) {
	if len(plan.Errors) > 0 {
		return nil, plan.Errors[0]
	}
	buffer := bytes.Buffer{}
	buffer.WriteString("select ")
	for index, col := range plan.table.columns {
		if !col.Transient {
			if index != 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(plan.table.dbmap.Dialect.QuoteField(col.ColumnName))
		}
	}
	buffer.WriteString(" from ")
	buffer.WriteString(plan.table.dbmap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName))
	whereClause, err := plan.whereClause()
	if err != nil {
		return nil, err
	}
	buffer.WriteString(whereClause)
	for index, orderBy := range plan.orderBy {
		if index == 0 {
			buffer.WriteString(" order by ")
		} else {
			buffer.WriteString(", ")
		}
		buffer.WriteString(orderBy)
	}
	for index, groupBy := range plan.groupBy {
		if index == 0 {
			buffer.WriteString(" group by ")
		} else {
			buffer.WriteString(", ")
		}
		buffer.WriteString(groupBy)
	}
	if plan.limit > 0 {
		buffer.WriteString(" limit ")
		buffer.WriteString(plan.table.dbmap.Dialect.BindVar(len(plan.args)))
		plan.args = append(plan.args, plan.limit)
	}
	if plan.offset > 0 {
		buffer.WriteString(" offset ")
		buffer.WriteString(plan.table.dbmap.Dialect.BindVar(len(plan.args)))
		plan.args = append(plan.args, plan.offset)
	}
	return plan.executor.Select(plan.target.Interface(), buffer.String(), plan.args...)
}

// Insert will run this query plan as an INSERT statement.
func (plan *QueryPlan) Insert() error {
	if len(plan.Errors) > 0 {
		return plan.Errors[0]
	}
	buffer := bytes.Buffer{}
	buffer.WriteString("insert into ")
	buffer.WriteString(plan.table.dbmap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName))
	buffer.WriteString(" (")
	for i, col := range plan.assignCols {
		if i > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString(col)
	}
	buffer.WriteString(") values (")
	for i, bindVar := range plan.assignBindVars {
		if i > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString(bindVar)
	}
	buffer.WriteString(")")
	_, err := plan.executor.Exec(buffer.String(), plan.args...)
	return err
}

// Update will run this query plan as an UPDATE statement.
func (plan *QueryPlan) Update() (int64, error) {
	if len(plan.Errors) > 0 {
		return -1, plan.Errors[0]
	}
	buffer := bytes.Buffer{}
	buffer.WriteString("update ")
	buffer.WriteString(plan.table.dbmap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName))
	buffer.WriteString(" set ")
	for i, col := range plan.assignCols {
		bindVar := plan.assignBindVars[i]
		if i > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString(col)
		buffer.WriteString("=")
		buffer.WriteString(bindVar)
	}
	whereClause, err := plan.whereClause()
	if err != nil {
		return -1, err
	}
	buffer.WriteString(whereClause)
	res, err := plan.executor.Exec(buffer.String(), plan.args...)
	if err != nil {
		return -1, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return -1, err
	}
	return rows, nil
}

// Delete will run this query plan as a DELETE statement.
func (plan *QueryPlan) Delete() (int64, error) {
	if len(plan.Errors) > 0 {
		return -1, plan.Errors[0]
	}
	buffer := bytes.Buffer{}
	buffer.WriteString("delete from ")
	buffer.WriteString(plan.table.dbmap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName))
	whereClause, err := plan.whereClause()
	if err != nil {
		return -1, err
	}
	buffer.WriteString(whereClause)
	res, err := plan.executor.Exec(buffer.String(), plan.args...)
	if err != nil {
		return -1, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return -1, err
	}
	return rows, nil
}

// An AssignQueryPlan is, for all intents and purposes, a QueryPlan.
// The only difference is the return type of Where() and all of the
// various where clause operations.  This is intended to be used for
// queries that have had Assign() called, to make it a compile error
// if you try to call Select() on a query that has had both Assign()
// and Where() called.
//
// All documentation for QueryPlan applies to AssignQueryPlan, too.
type AssignQueryPlan struct {
	QueryPlan
}

func (plan *AssignQueryPlan) Assign(fieldPtr interface{}, value interface{}) AssignQuery {
	column, err := plan.targetColMap.columnForPointer(fieldPtr)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return plan
	}
	plan.assignCols = append(plan.assignCols, column)
	plan.assignBindVars = append(plan.assignBindVars, plan.table.dbmap.Dialect.BindVar(len(plan.args)))
	plan.args = append(plan.args, value)
	return plan
}

func (plan *AssignQueryPlan) Where() UpdateQuery {
	return plan
}

func (plan *AssignQueryPlan) Filter(filter Filter) UpdateQuery {
	plan.QueryPlan.Filter(filter)
	return plan
}

func (plan *AssignQueryPlan) Equal(fieldPtr interface{}, value interface{}) UpdateQuery {
	plan.QueryPlan.Equal(fieldPtr, value)
	return plan
}

func (plan *AssignQueryPlan) NotEqual(fieldPtr interface{}, value interface{}) UpdateQuery {
	plan.QueryPlan.NotEqual(fieldPtr, value)
	return plan
}

func (plan *AssignQueryPlan) Less(fieldPtr interface{}, value interface{}) UpdateQuery {
	plan.QueryPlan.Less(fieldPtr, value)
	return plan
}

func (plan *AssignQueryPlan) LessOrEqual(fieldPtr interface{}, value interface{}) UpdateQuery {
	plan.QueryPlan.LessOrEqual(fieldPtr, value)
	return plan
}

func (plan *AssignQueryPlan) Greater(fieldPtr interface{}, value interface{}) UpdateQuery {
	plan.QueryPlan.Greater(fieldPtr, value)
	return plan
}

func (plan *AssignQueryPlan) GreaterOrEqual(fieldPtr interface{}, value interface{}) UpdateQuery {
	plan.QueryPlan.GreaterOrEqual(fieldPtr, value)
	return plan
}

func (plan *AssignQueryPlan) Null(fieldPtr interface{}) UpdateQuery {
	plan.QueryPlan.Null(fieldPtr)
	return plan
}

func (plan *AssignQueryPlan) NotNull(fieldPtr interface{}) UpdateQuery {
	plan.QueryPlan.NotNull(fieldPtr)
	return plan
}
