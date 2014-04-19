package gorp

import (
	"reflect"
	"errors"
	"strings"
	"bytes"
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

// A Setter is a query that can set columns to values.
type Setter interface {
	Set(fieldPtr interface{}, value interface{}) SetQuery
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
	Equal(fieldPtr interface{}, value interface{}) UpdateQuery
	Less(fieldPtr interface{}, value interface{}) UpdateQuery
	LessOrEqual(fieldPtr interface{}, value interface{}) UpdateQuery
	Greater(fieldPtr interface{}, value interface{}) UpdateQuery
	GreaterOrEqual(fieldPtr interface{}, value interface{}) UpdateQuery
	Updater
}

// A SetQuery is a query that may set values.
type SetQuery interface {
	Setter
	Where() UpdateQuery
	Inserter
	Updater
}

// A WhereQuery is a query that does not set any values, but may have
// a where clause.
type WhereQuery interface {
	Equal(fieldPtr interface{}, value interface{}) WhereQuery
	Less(fieldPtr interface{}, value interface{}) WhereQuery
	LessOrEqual(fieldPtr interface{}, value interface{}) WhereQuery
	Greater(fieldPtr interface{}, value interface{}) WhereQuery
	GreaterOrEqual(fieldPtr interface{}, value interface{}) WhereQuery
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
//         Set(&t.Foo, "test").
//         Where().
//         Less(&t.Created, time.Now()).
//         Insert()
//
// Since the return value from Set() is a SetQuery, the return value
// from Where() will be an UpdateQuery, which doesn't have an Insert()
// method.
type Query interface {
	Setter
	Wherer
	Receiver
	Inserter
	Updater
	Deleter
	Selector
}

// A QueryPlan is a Query.  It returns itself on most method calls;
// the one exception is Set(), which returns a SetQueryPlan (a type of
// QueryPlan that implements SetQuery instead of Query).  The returned
// type of the methods on this struct helps prevent silly errors like
// trying to run a SELECT statement that tries to Set() values by
// turning that type of nonsense into compile errors.
type QueryPlan struct {
	// During query construction, any errors encountered will be
	// stored on this field.  This is to allow cascading method calls,
	// e.g.
	//
	//     query := dbMap.Query(someModel).
	//         Select().
	//         Greater(&someModel.CreatedAt, yesterday).
	//         Less(&someModel.CreatedAt, time.Now()).
	//         Order(&someModel.CreatedAt, gorp.Descending)
	//     results, err := plan.Execute()
	//
	// The error returned by Execute() will be the first value in the
	// Errors slice if it is non-empty; any errors returned by the
	// database upon executing the query; or nil if everything went
	// correctly.
	//
	// Note that Execute() is called on the sub-type returned by your
	// initial Select(), Update(), etc method call, and thus will have
	// different return types depending on the type of query
	// requested.
	Errors []error

	table *TableMap
	executor SqlExecutor
	target reflect.Value
	assignCols []string
	assignBindVars []string
	where []string
	orderBy string
	orderByDirection string
	groupBy string
	limit int64
	offset int64
	args []interface{}
}

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
	return plan
}

func findFieldType(value reflect.Value, fieldPtr interface{}) *reflect.StructField {
	value = value.Elem()
	valueType := value.Type()
	for i := 0; i < value.NumField(); i++ {
		fieldType := valueType.Field(i)
		fieldVal := value.Field(i)
		if fieldType.Anonymous {
			if fieldVal.Kind() != reflect.Ptr {
				fieldVal = fieldVal.Addr()
			}
			if foundField := findFieldType(fieldVal, fieldPtr); foundField != nil {
				return foundField
			}
		} else if fieldVal.Addr().Interface() == fieldPtr {
			return &fieldType
		}
	}
	return nil
}

func (plan *QueryPlan) ColumnForPointer(fieldPtr interface{}) (col *ColumnMap, err error) {
	defer func() {
		if r := recover(); r != nil {
			// plan.table.ColMap() will panic if it can't find a
			// column matching the passed in name.  We would rather
			// return an error here, since we're dealing with user
			// input.
			err = errors.New("gorp: The passed in field does not seem to match a column in the applicable table")
		}
	}()
	targetFieldPtr := findFieldType(plan.target, fieldPtr)
	if targetFieldPtr == nil {
		err = errors.New("gorp: Could not find a field matching the passed in field pointer")
		return
	}
	targetField := *targetFieldPtr
	col = plan.table.ColMap(targetField.Name)
	return
}

func (plan *QueryPlan) columnOperatorString(fieldPtr interface{}, operator string) (string, error) {
	column, err := plan.ColumnForPointer(fieldPtr)
	if err != nil {
		return "", err
	}
	if column == nil {
		return "", errors.New("gorp: Could not find matching column")
	}
	if column.Transient {
		return "", errors.New("gorp: Cannot query transient column")
	}
	colStr := plan.table.dbmap.Dialect.QuoteField(column.ColumnName)
	valStr := plan.table.dbmap.Dialect.BindVar(len(plan.args))
	return colStr + operator + valStr, nil
}

func (plan *QueryPlan) addWhere(fieldPtr interface{}, operator string, value interface{}) {
	whereStr, err := plan.columnOperatorString(fieldPtr, operator)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
	}
	plan.where = append(plan.where, whereStr)
	plan.args = append(plan.args, value)
}

func (plan *QueryPlan) Set(fieldPtr interface{}, value interface{}) SetQuery {
	column, err := plan.ColumnForPointer(fieldPtr)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return &SetQueryPlan{QueryPlan: *plan}
	}
	plan.assignCols = append(plan.assignCols, plan.table.dbmap.Dialect.QuoteField(column.ColumnName))
	plan.assignBindVars = append(plan.assignBindVars, plan.table.dbmap.Dialect.BindVar(len(plan.args)))
	plan.args = append(plan.args, value)
	return &SetQueryPlan{QueryPlan: *plan}
}

func (plan *QueryPlan) Where() WhereQuery {
	return plan
}

func (plan *QueryPlan) Equal(fieldPtr interface{}, value interface{}) WhereQuery {
	plan.addWhere(fieldPtr, "=", value)
	return plan
}

func (plan *QueryPlan) Less(fieldPtr interface{}, value interface{}) WhereQuery {
	plan.addWhere(fieldPtr, "<", value)
	return plan
}

func (plan *QueryPlan) LessOrEqual(fieldPtr interface{}, value interface{}) WhereQuery {
	plan.addWhere(fieldPtr, "<=", value)
	return plan
}

func (plan *QueryPlan) Greater(fieldPtr interface{}, value interface{}) WhereQuery {
	plan.addWhere(fieldPtr, ">", value)
	return plan
}

func (plan *QueryPlan) GreaterOrEqual(fieldPtr interface{}, value interface{}) WhereQuery {
	plan.addWhere(fieldPtr, ">=", value)
	return plan
}

func (plan *QueryPlan) OrderBy(fieldPtr interface{}, direction string) SelectQuery {
	column, err := plan.ColumnForPointer(fieldPtr)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return plan
	}
	plan.orderBy = column.ColumnName
	plan.orderByDirection = direction
	return plan
}

func (plan *QueryPlan) GroupBy(fieldPtr interface{}) SelectQuery {
	column, err := plan.ColumnForPointer(fieldPtr)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return plan
	}
	plan.groupBy = column.ColumnName
	return plan
}

func (plan *QueryPlan) Limit(limit int64) SelectQuery {
	plan.limit = limit
	return plan
}

func (plan *QueryPlan) Offset(offset int64) SelectQuery {
	plan.offset = offset
	return plan
}

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
	for index, whereEntry := range plan.where {
		if index == 0 {
			buffer.WriteString(" where ")
		} else {
			buffer.WriteString(" and ")
		}
		buffer.WriteString(whereEntry)
	}
	if plan.orderBy != "" {
		buffer.WriteString(" order by ")
		buffer.WriteString(plan.table.dbmap.Dialect.QuoteField(plan.orderBy))
		switch strings.ToLower(plan.orderByDirection) {
		case "desc", "asc":
			buffer.WriteString(plan.orderByDirection)
		case "":
		default:
			plan.Errors = append(plan.Errors, errors.New("gorp: Order by direction is invalid"))
			return nil, plan.Errors[0]
		}
	}
	if plan.groupBy != "" {
		buffer.WriteString(" group by ")
		buffer.WriteString(plan.groupBy)
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
	for index, whereEntry := range plan.where {
		if index == 0 {
			buffer.WriteString(" where ")
		} else {
			buffer.WriteString(" and ")
		}
		buffer.WriteString(whereEntry)
	}
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

func (plan *QueryPlan) Delete() (int64, error) {
	if len(plan.Errors) > 0 {
		return -1, plan.Errors[0]
	}
	buffer := bytes.Buffer{}
	buffer.WriteString("delete from ")
	buffer.WriteString(plan.table.dbmap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName))
	for index, whereEntry := range plan.where {
		if index == 0 {
			buffer.WriteString(" where ")
		} else {
			buffer.WriteString(" and ")
		}
		buffer.WriteString(whereEntry)
	}
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

type SetQueryPlan struct {
	QueryPlan
}

func (plan *SetQueryPlan) Where() UpdateQuery {
	return plan
}

func (plan *SetQueryPlan) Equal(fieldPtr interface{}, value interface{}) UpdateQuery {
	plan.QueryPlan.Equal(fieldPtr, value)
	return plan
}

func (plan *SetQueryPlan) Less(fieldPtr interface{}, value interface{}) UpdateQuery {
	plan.QueryPlan.Less(fieldPtr, value)
	return plan
}

func (plan *SetQueryPlan) LessOrEqual(fieldPtr interface{}, value interface{}) UpdateQuery {
	plan.QueryPlan.LessOrEqual(fieldPtr, value)
	return plan
}

func (plan *SetQueryPlan) Greater(fieldPtr interface{}, value interface{}) UpdateQuery {
	plan.QueryPlan.Greater(fieldPtr, value)
	return plan
}

func (plan *SetQueryPlan) GreaterOrEqual(fieldPtr interface{}, value interface{}) UpdateQuery {
	plan.QueryPlan.GreaterOrEqual(fieldPtr, value)
	return plan
}
