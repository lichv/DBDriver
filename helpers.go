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
	FindById(string, int, string) (map[string]interface{}, error)
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

func checkOrderBy(orderBy string) bool {
	compile := regexp.MustCompile("(?i)^([a-zA-Z]+? +?(desc|asc) *?)(,[a-zA-Z]+? +?(asc|desc) *?)*?$")
	find := compile.FindStringIndex(orderBy)
	if find != nil {
		return true
	}
	return false
}

func sqlQuote(x interface{}) string {
	if noSQLQuoteNeeded(x) {
		return fmt.Sprintf("%v", x)
	} else {
		return fmt.Sprintf("'%v'", x)
	}
}

func isSimpleType(a interface{}) bool {
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
func noSQLQuoteNeeded(a interface{}) bool {
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
