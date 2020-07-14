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
	First int64 `json:"first"`
	Prev  int64 `json:"prev"`
	Page  int64 `json:"page"`
	Next  int64 `json:"next"`
	Last  int64 `json:"last"`
	Size  int64 `json:"size"`
	Total int64 `json:"total"`
}

type DBDriver interface {
	Open() error
	Close() error
	Query(string, ...interface{}) (*sql.Rows, error)
	Exec(string, ...interface{}) (sql.Result, error)
	ShowSql() error
	HideSql() error
	QueryMap(string, map[string]interface{}) (*sql.Rows, error)
	FindById(string, int64) (*sql.Rows, error)
	FindOne(string, map[string]interface{}, string) (*sql.Rows, error)
	Exists(string, map[string]interface{}) bool
	Count(string, map[string]interface{}) (int64, error)
	GetList(string, map[string]interface{}, string) (*sql.Rows, error)
	GetPage(string, map[string]interface{}, string, int64, int64) (*sql.Rows, *Page, error)
	Insert(string, map[string]interface{}) (int64, error)
	Update(string, map[string]interface{}, map[string]interface{}) (int64, error)
	Save(string, map[string]interface{}) (int64, error)
	Delete(string, map[string]interface{}) (int64, error)
	DeleteById(string, int64) (int64, error)
	Begin() error
	RollBack() error
	Commit() error
	QueryTX(string, ...interface{}) (*sql.Rows, error)
	ExecTX(string, ...interface{}) (sql.Result, error)
}

func CreateDBDriver(driverName string,host string, port int, user, password, dbname string) DBDriver {
	var dbDriver DBDriver
	if driverName =="mysql"{
		my := InitMysqlDriver(host,port,user,password,dbname)
		dbDriver = interface{}(my).(DBDriver)
	}else if driverName == "postgres"{
		po := InitPostgreDriver(host,port,user,password,dbname)
		dbDriver = interface{}(po).(DBDriver)
	}

	return dbDriver
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
		}else if reflect.TypeOf(v).Kind() == reflect.Map {
			m,ok:= v.(map[string]interface{})
			if !ok {
				continue
			}
			operater,ok := m["operater"]
			if !ok {
				continue
			}
			switch operater {
			case "=":
			case "!=":
			case ">":
			case ">=":
			case "<":
			case "<=":
				value,ok := m["value"]
				if !ok {
					continue
				}
				o,ok := operater.(string)
				if ok {
					v,ok := value.(string)
					if ok {
						s += split + " " + k + " " + o  + " " + SqlQuote(v)
						split = " and "
					}
				}
			case "like":
				value,ok := m["value"]
				if !ok {
					continue
				}
				o,ok := operater.(string)
				if ok {
					v,ok := value.(string)
					if ok {
						s += split + " " + k + " " + o + " "+ SqlQuote("%"+v+"%")
						split = " and "
					}
				}
			}
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
			values += split + SqlQuote(v)
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
	defer rows.Close()
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
			c ,ok := col.([]uint8)
			if ok {
				col = string(c)
			}
			if col != nil {
				rowMap[columns[i]] = col
			}
		}
		rowsMap = append(rowsMap, rowMap)
	}
	if err = rows.Err(); err != nil {
		return map[string]interface{}{}, err
	}
	return rowsMap[0], nil
}
func ReturnListFromResults(rows *sql.Rows) ([]map[string]interface{}, error) {
	var err error
	defer rows.Close()
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
			c ,ok := col.([]uint8)
			if ok {
				col = string(c)
			}
			if col != nil {
				rowMap[columns[i]] = col
			}
		}
		rowsMap = append(rowsMap, rowMap)
	}
	if err = rows.Err(); err != nil {
		return []map[string]interface{}{}, err
	}
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
