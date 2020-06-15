package DBDriver

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
	"regexp"
	"time"
)

type Page struct {
	First int
	Prev  int
	Page  int
	Next  int
	Last  int
	Size  int
	Total int
}

type DBDriver interface {
	Open() error
	Close() error
	Query() (*sql.Rows, error)
	Exec() (sql.Result, error)
	QueryMap(string, map[string]interface{}) ([]map[string]interface{}, error)
	FindById(string, int) (map[string]interface{}, error)
	FindOne(string, map[string]interface{}, string) (map[string]interface{}, error)
	Exists(string, map[string]interface{}) bool
	Count(string, map[string]interface{}) (int, error)
	GetList(string, map[string]interface{}, string) ([]map[string]interface{}, error)
	GetPage(string, map[string]interface{}, string, int, int) ([]map[string]interface{}, *Page, error)
	Insert(string, map[string]interface{}) (int64, error)
	Update(string, map[string]interface{}, map[string]interface{}) (int64, error)
	Save(string, map[string]interface{}) (int64, error)
	Delete(string, map[string]interface{}) (int64, error)
	DeleteById(string, int) (int64, error)
	Begin() error
	RollBack() error
	Commit() error
	QueryTx(string, ...interface{}) (*sql.Rows, error)
	ExecTx(string, ...interface{}) (sql.Result, error)
	GetInsertSql(string, map[string]interface{}) (string, error)
	GetUpdateSql(string, map[string]interface{}, map[string]interface{}) (string, error)
	WhereFromQuery(map[string]interface{}) (string, error)
}

func CheckOrderBy(orderBy string) bool {
	compile := regexp.MustCompile("(?i)^([a-zA-Z]+? +?(desc|asc) *?)(,[a-zA-Z]+? +?(asc|desc) *?)*?$")
	find := compile.FindStringIndex(orderBy)
	if find != nil {
		return true
	}
	return false
}

func WhereFromQuery(query map[string]interface{}) (string, error) {
	s := ""
	split := " where "
	for k, v := range query {
		if IsSimpleType(v) {
			s += split + " " + k + "=" + SqlQuote(v)
			split = " and "
		}
	}
	return s, nil
}
func GetInsertSql(tableName string, post map[string]interface{}) (string, error) {
	s, columns, values := "", "", ""
	split := ""
	for k, v := range post {
		if IsSimpleType(v) {
			columns += split + k
			s += split + SqlQuote(v)
			split = ", "
		}
	}
	if columns != "" {
		s = "insert into " + tableName + "(" + columns + ") values (" + values + ")"
	}
	return s, nil
}
func GetUpdateSQL(tableName string, post map[string]interface{}, query map[string]interface{}) (string, error) {
	s := ""
	split := "update " + tableName + " set "
	for k, v := range post {
		if IsSimpleType(v) {
			s += split + " " + k + "=" + SqlQuote(v)
			split = ", "
		}
	}
	where, _ := WhereFromQuery(query)
	return s + where, nil
}
func ReturnMapFromResult(rows *sql.Rows) (map[string]interface{}, error) {
	var err error
	columns, _ := rows.Columns()
	scanArgs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	rowsMap := make([]map[string]interface{}, 0, 10)
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		rowMap := make(map[string]interface{})
		for i, col := range values {
			if col != nil {
				rowMap[columns[i]] = col
			}
		}
		rowsMap = append(rowsMap, rowMap)
	}
	if err = rows.Err(); err != nil {
		return map[string]interface{}{}, err
	}
	_ = rows.Close()
	return rowsMap[0], nil
}
func ReturnListFromResults(rows *sql.Rows) ([]map[string]interface{}, error) {
	var err error
	columns, _ := rows.Columns()
	scanArgs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	rowsMap := make([]map[string]interface{}, 0, 10)
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		rowMap := make(map[string]interface{})
		for i, col := range values {
			if col != nil {
				rowMap[columns[i]] = col
			}
		}
		rowsMap = append(rowsMap, rowMap)
	}
	if err = rows.Err(); err != nil {
		return []map[string]interface{}{}, err
	}
	_ = rows.Close()
	return rowsMap, nil
}
func SqlQuote(x interface{}) string {
	if NoSqlQuoteNeeded(x) {
		return fmt.Sprintf("%v", x)
	} else {
		return fmt.Sprintf("'%v'", x)
	}
}

func IsSimpleType(a interface{}) bool {
	switch a.(type) {
	case int, int8, int16, int32, int64:
		return true
	case uint, uint8, uint16, uint32, uint64:
		return true
	case float32, float64:
		return true
	case bool:
		return true
	case string:
		return true
	}

	t := reflect.TypeOf(a)

	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	case reflect.Float32, reflect.Float64:
		return true
	case reflect.Bool:
		return true
	case reflect.String:
		return true
	}

	return false
}
func NoSqlQuoteNeeded(a interface{}) bool {
	switch a.(type) {
	case int, int8, int16, int32, int64:
		return true
	case uint, uint8, uint16, uint32, uint64:
		return true
	case float32, float64:
		return true
	case bool:
		return true
	case string:
		return false
	case time.Time, *time.Time:
		return false
	}

	t := reflect.TypeOf(a)

	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	case reflect.Float32, reflect.Float64:
		return true
	case reflect.Bool:
		return true
	case reflect.String:
		return false
	}

	return false
}
