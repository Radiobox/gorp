package gorp

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
)

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
	// Execute the select statement, return the results as a slice of
	// the type that was used to create the query.
	Select() (results []interface{}, err error)

	// Execute the select statement, but use the passed in slice
	// pointer as the target to append to.
	SelectToTarget(target interface{}) error
}

// A SelectManipulator is a query that will return a list of results
// which can be manipulated.
type SelectManipulator interface {
	OrderBy(fieldPtr interface{}, direction string) SelectQuery
	GroupBy(fieldPtr interface{}) SelectQuery
	Limit(int64) SelectQuery
	Offset(int64) SelectQuery
}

// An Assigner is a query that can set columns to values.
type Assigner interface {
	Assign(fieldPtr interface{}, value interface{}) AssignQuery
}

// A Joiner is a query that can add tables as join clauses.
type Joiner interface {
	Join(table interface{}) JoinQuery
}

// An AssignJoiner is a Joiner with an assigner return type, for
// insert or update statements with a FROM clause.
type AssignJoiner interface {
	Join(table interface{}) AssignJoinQuery
}

// A Wherer is a query that can execute statements with a WHERE
// clause.
type Wherer interface {
	Where(...Filter) WhereQuery
}

// An AssignWherer is a Wherer with an assigner return type.
type AssignWherer interface{
	Where(...Filter) UpdateQuery
}

// A SelectQuery is a query that can only execute SELECT statements.
type SelectQuery interface {
	SelectManipulator
	Selector
}

// An UpdateQuery is a query that can only execute UPDATE statements.
type UpdateQuery interface {
	// Filter is used for queries that are more complex than a few
	// ANDed constraints.
	Filter(...Filter) UpdateQuery

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
	AssignJoiner
	AssignWherer
	Inserter
	Updater
}

// An AssignJoinQuery is a clone of JoinQuery, but for UPDATE and
// INSERT statements instead of DELETE and SELECT.
type AssignJoinQuery interface {
	AssignJoiner

	On(...Filter) AssignJoinQuery

	Equal(fieldPtr interface{}, value interface{}) AssignJoinQuery
	NotEqual(fieldPtr interface{}, value interface{}) AssignJoinQuery
	Less(fieldPtr interface{}, value interface{}) AssignJoinQuery
	LessOrEqual(fieldPtr interface{}, value interface{}) AssignJoinQuery
	Greater(fieldPtr interface{}, value interface{}) AssignJoinQuery
	GreaterOrEqual(fieldPtr interface{}, value interface{}) AssignJoinQuery
	NotNull(fieldPtr interface{}) AssignJoinQuery
	Null(fieldPtr interface{}) AssignJoinQuery

	AssignWherer
	Updater
}

// A JoinQuery is a query that uses join operations to compare values
// between tables.
type JoinQuery interface {
	Joiner

	// On for a JoinQuery is equivalent to Filter for a WhereQuery.
	On(...Filter) JoinQuery

	// These methods should be roughly equivalent to those of a
	// WhereQuery, except they add to the ON clause instead of the
	// WHERE clause.
	Equal(fieldPtr interface{}, value interface{}) JoinQuery
	NotEqual(fieldPtr interface{}, value interface{}) JoinQuery
	Less(fieldPtr interface{}, value interface{}) JoinQuery
	LessOrEqual(fieldPtr interface{}, value interface{}) JoinQuery
	Greater(fieldPtr interface{}, value interface{}) JoinQuery
	GreaterOrEqual(fieldPtr interface{}, value interface{}) JoinQuery
	NotNull(fieldPtr interface{}) JoinQuery
	Null(fieldPtr interface{}) JoinQuery

	Wherer
	Deleter
	Selector
}

// A WhereQuery is a query that does not set any values, but may have
// a where clause.
type WhereQuery interface {
	// Filter is used for queries that are more complex than a few
	// ANDed constraints.
	Filter(...Filter) WhereQuery

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
	SelectManipulator
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
	Joiner
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
	SelectManipulator
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

	// quotedTable should be the pre-quoted table string for this
	// column.
	quotedTable string

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
	fieldMap, err := structMap.fieldMapForPointer(fieldPtr)
	if err != nil {
		return "", err
	}
	return fieldMap.quotedColumn, nil
}

// tableColumnForPointer takes an interface value (which should be a
// pointer to one of the fields on the value that is being used as a
// reference for query construction) and returns the pre-quoted
// table.column name that should be used to reference that value in
// some types of queries (mostly where statements and select queries).
func (structMap structColumnMap) tableColumnForPointer(fieldPtr interface{}) (string, error) {
	fieldMap, err := structMap.fieldMapForPointer(fieldPtr)
	if err != nil {
		return "", err
	}
	return fieldMap.quotedTable + "." + fieldMap.quotedColumn, nil
}

// fieldMapForPointer takes a pointer to a struct field and returns
// the fieldColumnMap for that struct field.
func (structMap structColumnMap) fieldMapForPointer(fieldPtr interface{}) (*fieldColumnMap, error) {
	for _, fieldMap := range structMap {
		if fieldMap.addr == fieldPtr {
			if fieldMap.column.Transient {
				return nil, errors.New("gorp: Cannot run queries against transient columns")
			}
			return &fieldMap, nil
		}
	}
	return nil, errors.New("gorp: Cannot find a field matching the passed in pointer")
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
	dbMap          *DbMap
	executor       SqlExecutor
	target         reflect.Value
	colMap         structColumnMap
	joins          []*joinFilter
	assignCols     []string
	assignBindVars []string
	filters        MultiFilter
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
		dbMap:    m,
		executor: exec,
	}

	targetVal := reflect.ValueOf(target)
	targetTable, err := plan.mapTable(targetVal)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return plan
	}
	plan.target = targetVal
	plan.table = targetTable
	return plan
}

func (plan *QueryPlan) mapTable(targetVal reflect.Value) (*TableMap, error) {
	if targetVal.Kind() != reflect.Ptr || targetVal.Elem().Kind() != reflect.Struct {
		return nil, errors.New("gorp: Cannot create query plan - target value must be a pointer to a struct")
	}

	targetTable, err := plan.dbMap.tableFor(targetVal.Type().Elem(), false)
	if err != nil {
		return nil, err
	}

	if err = plan.mapColumns(targetTable, targetVal); err != nil {
		return nil, err
	}
	return targetTable, nil
}

// mapColumns creates a list of field addresses and column maps, to
// make looking up the column for a field address easier.  Note that
// it doesn't do any special handling for overridden fields, because
// passing the address of a field that has been overridden is
// difficult to do accidentally.
func (plan *QueryPlan) mapColumns(table *TableMap, value reflect.Value) (err error) {
	value = value.Elem()
	valueType := value.Type()
	if plan.colMap == nil {
		plan.colMap = make(structColumnMap, 0, value.NumField())
	}
	quotedTableName := table.dbmap.Dialect.QuotedTableForQuery(table.SchemaName, table.TableName)
	for i := 0; i < value.NumField(); i++ {
		fieldType := valueType.Field(i)
		fieldVal := value.Field(i)
		if fieldType.Anonymous {
			if fieldVal.Kind() != reflect.Ptr {
				fieldVal = fieldVal.Addr()
			}
			plan.mapColumns(table, fieldVal)
		} else if fieldType.PkgPath == "" {
			col := table.ColMap(fieldType.Name)
			quotedCol := table.dbmap.Dialect.QuoteField(col.ColumnName)
			fieldMap := fieldColumnMap{
				addr:         fieldVal.Addr().Interface(),
				column:       col,
				quotedTable:  quotedTableName,
				quotedColumn: quotedCol,
			}
			plan.colMap = append(plan.colMap, fieldMap)
		}
	}
	return
}

// Assign sets up an assignment operation to assign the passed in
// value to the passed in field pointer.  This is used for creating
// UPDATE or INSERT queries.
func (plan *QueryPlan) Assign(fieldPtr interface{}, value interface{}) AssignQuery {
	assignPlan := &AssignQueryPlan{QueryPlan: plan}
	return assignPlan.Assign(fieldPtr, value)
}

func (plan *QueryPlan) storeJoin() {
	if lastJoinFilter, ok := plan.filters.(*joinFilter); ok {
		if plan.joins == nil {
			plan.joins = make([]*joinFilter, 0, 2)
		}
		plan.joins = append(plan.joins, lastJoinFilter)
		plan.filters = nil
	}
}

func (plan *QueryPlan) Join(target interface{}) JoinQuery {
	plan.storeJoin()
	table, err := plan.mapTable(reflect.ValueOf(target))
	if err != nil {
		plan.Errors = append(plan.Errors, err)
	}
	quotedTable := table.dbmap.Dialect.QuotedTableForQuery(table.SchemaName, table.TableName)
	plan.filters = &joinFilter{quotedJoinTable: quotedTable}
	return &JoinQueryPlan{QueryPlan: plan}
}

func (plan *QueryPlan) On(filters ...Filter) JoinQuery {
	plan.filters.Add(filters...)
	return &JoinQueryPlan{QueryPlan: plan}
}

// Where stores any join filter and allocates a new and filter to use
// for WHERE clause creation.  If you pass filters to it, they will be
// passed to plan.Filter().
func (plan *QueryPlan) Where(filters ...Filter) WhereQuery {
	plan.storeJoin()
	plan.filters = new(andFilter)
	plan.Filter(filters...)
	return plan
}

// Filter will add a Filter to the list of filters on this query.  The
// default method of combining filters on a query is by AND - if you
// want OR, you can use the following syntax:
//
//     query.Filter(gorp.Or(gorp.Equal(&field.Id, id), gorp.Less(&field.Priority, 3)))
//
func (plan *QueryPlan) Filter(filters ...Filter) WhereQuery {
	plan.filters.Add(filters...)
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
	column, err := plan.colMap.tableColumnForPointer(fieldPtr)
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
	column, err := plan.colMap.tableColumnForPointer(fieldPtr)
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
	where, whereArgs, err := plan.filters.Where(plan.colMap, plan.table.dbmap.Dialect, len(plan.args))
	if err != nil {
		return "", err
	}
	if where != "" {
		plan.args = append(plan.args, whereArgs...)
		return " where " + where, nil
	}
	return "", nil
}

func (plan *QueryPlan) selectJoinClause() (string, error) {
	buffer := bytes.Buffer{}
	for _, join := range plan.joins {
		joinClause, joinArgs, err := join.JoinClause(plan.colMap, plan.table.dbmap.Dialect, len(plan.args))
		if err != nil {
			return "", err
		}
		buffer.WriteString(joinClause)
		plan.args = append(plan.args, joinArgs...)
	}
	return buffer.String(), nil
}

// Select will run this query plan as a SELECT statement.
func (plan *QueryPlan) Select() ([]interface{}, error) {
	query, err := plan.selectQuery()
	if err != nil {
		return nil, err
	}
	return plan.executor.Select(plan.target.Interface(), query, plan.args...)
}

// SelectToTarget will run this query plan as a SELECT statement, and
// append results directly to the passed in slice pointer.
func (plan *QueryPlan) SelectToTarget(target interface{}) error {
	targetType := reflect.TypeOf(target)
	if targetType.Kind() != reflect.Ptr || targetType.Elem().Kind() != reflect.Slice {
		return errors.New("SelectToTarget must be run with a pointer to a slice as its target")
	}
	query, err := plan.selectQuery()
	if err != nil {
		return err
	}
	_, err = plan.executor.Select(target, query, plan.args...)
	return err
}

func (plan *QueryPlan) selectQuery() (string, error) {
	if len(plan.Errors) > 0 {
		return "", plan.Errors[0]
	}
	quotedTable := plan.table.dbmap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName)
	buffer := bytes.Buffer{}
	buffer.WriteString("select ")
	for index, col := range plan.table.columns {
		if !col.Transient {
			if index != 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(quotedTable)
			buffer.WriteString(".")
			buffer.WriteString(plan.table.dbmap.Dialect.QuoteField(col.ColumnName))
		}
	}
	buffer.WriteString(" from ")
	buffer.WriteString(quotedTable)
	joinClause, err := plan.selectJoinClause()
	if err != nil {
		return "", err
	}
	buffer.WriteString(joinClause)
	whereClause, err := plan.whereClause()
	if err != nil {
		return "", err
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
	if plan.offset > 0 {
		buffer.WriteString(" offset ")
		buffer.WriteString(plan.table.dbmap.Dialect.BindVar(len(plan.args)))
		plan.args = append(plan.args, plan.offset)
	}
	if plan.limit > 0 {
		buffer.WriteString(" fetch next (")
		buffer.WriteString(plan.table.dbmap.Dialect.BindVar(len(plan.args)))
		plan.args = append(plan.args, plan.limit)
		buffer.WriteString(") rows only")
	}
	return buffer.String(), nil
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

// joinFromAndWhereClause will return the from and where clauses for
// joined tables, for use in UPDATE and DELETE statements.
func (plan *QueryPlan) joinFromAndWhereClause() (from, where string, err error) {
	fromSlice := make([]string, 0, len(plan.joins))
	whereBuffer := bytes.Buffer{}
	for _, join := range plan.joins {
		fromSlice = append(fromSlice, join.quotedJoinTable)
		whereClause, whereArgs, err := join.Where(plan.colMap, plan.table.dbmap.Dialect, len(plan.args))
		if err != nil {
			return "", "", err
		}
		whereBuffer.WriteString(whereClause)
		plan.args = append(plan.args, whereArgs...)
	}
	return strings.Join(fromSlice, ", "), whereBuffer.String(), nil
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
	joinTables, joinWhereClause, err := plan.joinFromAndWhereClause()
	if err != nil {
		return -1, nil
	}
	if joinTables != "" {
		buffer.WriteString(" from ")
		buffer.WriteString(joinTables)
	}
	whereClause, err := plan.whereClause()
	if err != nil {
		return -1, err
	}
	if joinWhereClause != "" {
		if whereClause == "" {
			whereClause = " where"
		}
		whereClause += " " + joinWhereClause
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
	joinTables, joinWhereClause, err := plan.joinFromAndWhereClause()
	if err != nil {
		return -1, err
	}
	if joinTables != "" {
		buffer.WriteString(" using ")
		buffer.WriteString(joinTables)
	}
	whereClause, err := plan.whereClause()
	if err != nil {
		return -1, err
	}
	if joinWhereClause != "" {
		if whereClause == "" {
			whereClause = " where"
		}
		whereClause += " " + joinWhereClause
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

// A JoinQueryPlan is a QueryPlan, except with some return values
// changed so that it will match the JoinQuery interface.
type JoinQueryPlan struct {
	*QueryPlan
}

func (plan *JoinQueryPlan) Equal(fieldPtr interface{}, value interface{}) JoinQuery {
	plan.QueryPlan.Equal(fieldPtr, value)
	return plan
}

func (plan *JoinQueryPlan) NotEqual(fieldPtr interface{}, value interface{}) JoinQuery {
	plan.QueryPlan.NotEqual(fieldPtr, value)
	return plan
}

func (plan *JoinQueryPlan) Less(fieldPtr interface{}, value interface{}) JoinQuery {
	plan.QueryPlan.Less(fieldPtr, value)
	return plan
}

func (plan *JoinQueryPlan) LessOrEqual(fieldPtr interface{}, value interface{}) JoinQuery {
	plan.QueryPlan.LessOrEqual(fieldPtr, value)
	return plan
}

func (plan *JoinQueryPlan) Greater(fieldPtr interface{}, value interface{}) JoinQuery {
	plan.QueryPlan.Greater(fieldPtr, value)
	return plan
}

func (plan *JoinQueryPlan) GreaterOrEqual(fieldPtr interface{}, value interface{}) JoinQuery {
	plan.QueryPlan.GreaterOrEqual(fieldPtr, value)
	return plan
}

func (plan *JoinQueryPlan) Null(fieldPtr interface{}) JoinQuery {
	plan.QueryPlan.Null(fieldPtr)
	return plan
}

func (plan *JoinQueryPlan) NotNull(fieldPtr interface{}) JoinQuery {
	plan.QueryPlan.NotNull(fieldPtr)
	return plan
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
	*QueryPlan
}

func (plan *AssignQueryPlan) Assign(fieldPtr interface{}, value interface{}) AssignQuery {
	column, err := plan.colMap.columnForPointer(fieldPtr)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return plan
	}
	plan.assignCols = append(plan.assignCols, column)
	plan.assignBindVars = append(plan.assignBindVars, plan.table.dbmap.Dialect.BindVar(len(plan.args)))
	plan.args = append(plan.args, value)
	return plan
}

func (plan *AssignQueryPlan) Join(table interface{}) AssignJoinQuery {
	plan.QueryPlan.Join(table)
	return &AssignJoinQueryPlan{plan}
}

func (plan *AssignQueryPlan) Where(filters ...Filter) UpdateQuery {
	plan.QueryPlan.Where(filters...)
	return plan
}

func (plan *AssignQueryPlan) Filter(filters ...Filter) UpdateQuery {
	plan.QueryPlan.Filter(filters...)
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

// An AssignJoinQueryPlan is equivalent to an AssignQueryPlan, with
// different return types to match AssignJoinQuery.
type AssignJoinQueryPlan struct {
	*AssignQueryPlan
}

func (plan *AssignJoinQueryPlan) On(filters ...Filter) AssignJoinQuery {
	plan.AssignQueryPlan.On(filters...)
	return plan
}

func (plan *AssignJoinQueryPlan) Equal(fieldPtr interface{}, value interface{}) AssignJoinQuery {
	plan.QueryPlan.Equal(fieldPtr, value)
	return plan
}

func (plan *AssignJoinQueryPlan) NotEqual(fieldPtr interface{}, value interface{}) AssignJoinQuery {
	plan.QueryPlan.NotEqual(fieldPtr, value)
	return plan
}

func (plan *AssignJoinQueryPlan) Less(fieldPtr interface{}, value interface{}) AssignJoinQuery {
	plan.QueryPlan.Less(fieldPtr, value)
	return plan
}

func (plan *AssignJoinQueryPlan) LessOrEqual(fieldPtr interface{}, value interface{}) AssignJoinQuery {
	plan.QueryPlan.LessOrEqual(fieldPtr, value)
	return plan
}

func (plan *AssignJoinQueryPlan) Greater(fieldPtr interface{}, value interface{}) AssignJoinQuery {
	plan.QueryPlan.Greater(fieldPtr, value)
	return plan
}

func (plan *AssignJoinQueryPlan) GreaterOrEqual(fieldPtr interface{}, value interface{}) AssignJoinQuery {
	plan.QueryPlan.GreaterOrEqual(fieldPtr, value)
	return plan
}

func (plan *AssignJoinQueryPlan) Null(fieldPtr interface{}) AssignJoinQuery {
	plan.QueryPlan.Null(fieldPtr)
	return plan
}

func (plan *AssignJoinQueryPlan) NotNull(fieldPtr interface{}) AssignJoinQuery {
	plan.QueryPlan.NotNull(fieldPtr)
	return plan
}
