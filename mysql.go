package DBDriver

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"strconv"
	"time"
)

type MysqlDriver struct {
	DriverName     string
	DataSourceName string
	DB             *sql.DB
	SQLTX          *sql.Tx
}

func InitMysqlDriver(host string, port int, user, password, dbname string) *MysqlDriver {
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&autocommit=true", user, password, host, port, dbname, "utf8")
	db := &MysqlDriver{
		DriverName:     "mysql",
		DataSourceName: dataSourceName,
	}

	if err := db.Open(); err != nil {
		log.Panicln("Init mysql pool failed.", err.Error())
	}
	return db
}

func (db *MysqlDriver) Open() (err error) {
	db.DB, err = sql.Open(db.DriverName, db.DataSourceName)
	if err != nil {
		return err
	}
	if err = db.DB.Ping(); err != nil {
		return err
	}
	db.DB.SetMaxOpenConns(20)
	db.DB.SetMaxIdleConns(10)
	db.DB.SetConnMaxLifetime(time.Second * 10)
	return nil
}
func (db *MysqlDriver) Close() error {
	return db.DB.Close()
}
func (db *MysqlDriver) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.DB.Query(query, args...)
}
func (db *MysqlDriver) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.DB.Exec(query, args...)
}
func (db *MysqlDriver) QueryMap(tableName string, query map[string]interface{}) ([]map[string]interface{}, error) {
	s := "select * from " + tableName + " "
	where, _ := db.WhereFromQuery(query)
	s += " " + where
	rows, err := db.Query(s)
	if err != nil {
		return []map[string]interface{}{}, err
	}

	return db.returnResults(rows)
}
func (db *MysqlDriver) FindById(tableName string, id int, orderBy string) (map[string]interface{}, error) {
	s := "select * from " + tableName
	if !checkOrderBy(orderBy) {
		orderBy = ""
	}

	s += "where id =" + strconv.Itoa(id) + " limit 1 " + orderBy
	rows, err := db.Query(s)
	if err != nil {

		return nil, err
	}
	return db.returnResult(rows)
}
func (db *MysqlDriver) FindOne(tableName string, query map[string]interface{}, orderBy string) (map[string]interface{}, error) {
	s := "select * from " + tableName + " "
	if !checkOrderBy(orderBy) {
		orderBy = ""
	}
	where, _ := db.WhereFromQuery(query)
	rows, err := db.DB.Query(s + where)
	if err != nil {
		return nil, err
	}
	return db.returnResult(rows)
}

func (db *MysqlDriver) Exists(tableName string, query map[string]interface{}) bool {
	var count = 0
	s := "select count(1) as number from " + tableName + " "
	where, _ := db.WhereFromQuery(query)
	rows, err := db.DB.Query(s + where)
	if err != nil {
		return false
	}
	defer rows.Close()
	rows.Next()
	_ = rows.Scan(&count)
	return count > 0
}

func (db *MysqlDriver) Count(tableName string, query map[string]interface{}) (int, error) {
	var count = 0
	s := "select count(1) as number from " + tableName + " "
	where, _ := db.WhereFromQuery(query)
	rows, err := db.DB.Query(s + where)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	rows.Next()
	_ = rows.Scan(&count)
	return count, nil
}
func (db *MysqlDriver) GetList(tableName string, query map[string]interface{}, orderBy string) ([]map[string]interface{}, error) {
	s := "select * from " + tableName + " "
	if !checkOrderBy(orderBy) {
		orderBy = ""
	}
	where, _ := db.WhereFromQuery(query)
	rows, err := db.DB.Query(s + where)
	if err != nil {
		return nil, err
	}
	return db.returnResults(rows)
}
func (db *MysqlDriver) GetPage(tableName string, query map[string]interface{}, orderBy string, page, size int) ([]map[string]interface{}, *Page, error) {
	total, _ := db.Count(tableName, query)
	last := total/size + 1
	prev := 1
	if page > 2 {
		prev = page - 1
	}
	next := last
	if page < last-1 {
		next = page + 1
	}
	offset := (page - 1) * size
	s := "select * from " + tableName + " "
	if !checkOrderBy(orderBy) {
		orderBy = ""
	}
	where, _ := db.WhereFromQuery(query)
	sql2 := s + where
	if orderBy != "" {
		sql2 += "order by " + orderBy
	}
	sql2 += " limit " + strconv.Itoa(size) + " offset " + strconv.Itoa(offset)
	rows, err := db.DB.Query(sql2)
	if err != nil {
		return nil, nil, err
	}
	result, err := db.returnResults(rows)
	return result, &Page{First: 1, Prev: prev, Page: page, Next: next, Last: last, Size: size, Total: total}, nil
}

func (db *MysqlDriver) Insert(tableName string, post map[string]interface{}) (int64, error) {
	s, _ := db.GetInsertSql(tableName, post)
	exec, err := db.DB.Exec(s)
	if err != nil {
		return 0, err
	}
	return exec.LastInsertId()
}
func (db *MysqlDriver) Update(tableName string, post map[string]interface{}, query map[string]interface{}) (int64, error) {
	s, _ := db.GetUpdateSQL(tableName, post, query)
	exec, err := db.DB.Exec(s)
	if err != nil {
		return 0, err
	}
	return exec.RowsAffected()
}
func (db *MysqlDriver) Save(tableName string, post map[string]interface{}) (int64, error) {
	id, ok := post["id"]
	if ok {
		delete(post, "id")
		return db.Update(tableName, post, map[string]interface{}{"id": id})
	} else {
		return db.Insert(tableName, post)
	}
}
func (db *MysqlDriver) Delete(tableName string, query map[string]interface{}) (int64, error) {
	where, _ := db.WhereFromQuery(query)
	if where != "" {
		s := "delete from " + tableName + where
		exec, err := db.DB.Exec(s)
		if err != nil {
			return 0, err
		}
		return exec.RowsAffected()
	} else {
		return 0, nil
	}
}
func (db *MysqlDriver) DeleteById(tableName string, id int) (int64, error) {
	if id != 0 {
		s := "delete from " + tableName + " where id = ?"
		exec, err := db.DB.Exec(s, id)
		if err != nil {
			return 0, err
		}
		return exec.RowsAffected()
	} else {
		return 0, nil
	}
}
func (db *MysqlDriver) Begin() error {
	err := db.DB.Ping()
	if err != nil {
		return nil
	}
	db.SQLTX, err = db.DB.Begin()
	if err != nil {
		return err
	}
	return nil
}
func (db *MysqlDriver) RollBack() error {
	return db.SQLTX.Rollback()
}
func (db *MysqlDriver) Commit() error {
	return db.SQLTX.Commit()
}
func (db *MysqlDriver) QueryTX(query string, args ...interface{}) (*sql.Rows, error) {
	return db.SQLTX.Query(query, args...)
}
func (db *MysqlDriver) ExecTX(query string, args ...interface{}) (sql.Result, error) {
	return db.SQLTX.Exec(query, args...)
}
func (db *MysqlDriver) returnResult(rows *sql.Rows) (map[string]interface{}, error) {
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
	rows.Close()
	return rowsMap[0], nil
}
func (db *MysqlDriver) returnResults(rows *sql.Rows) ([]map[string]interface{}, error) {
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
	rows.Close()
	return rowsMap, nil
}
func (db *MysqlDriver) GetInsertSql(tableName string, post map[string]interface{}) (string, error) {
	s, colums, values := "", "", ""
	split := ""
	for k, v := range post {
		if isSimpleType(v) {
			colums += split + k
			s += split + sqlQuote(v)
			split = ", "
		}
	}
	if colums != "" {
		s = "insert into " + tableName + "(" + colums + ") values (" + values + ")"
	}

	return s, nil
}
func (db *MysqlDriver) GetUpdateSQL(tableName string, post map[string]interface{}, query map[string]interface{}) (string, error) {
	s := ""
	split := "update " + tableName + " set "
	for k, v := range post {
		if isSimpleType(v) {
			s += split + " " + k + "=" + sqlQuote(v)
			split = ", "
		}
	}
	where, _ := db.WhereFromQuery(query)
	return s + where, nil
}
func (db *MysqlDriver) WhereFromQuery(query map[string]interface{}) (string, error) {
	s := ""
	split := " where "
	for k, v := range query {
		if isSimpleType(v) {
			s += split + " " + k + "=" + sqlQuote(v)
			split = " and "
		}
	}

	return s, nil
}