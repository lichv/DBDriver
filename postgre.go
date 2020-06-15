package DBDriver

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"strconv"
	"time"
)

type PostgresDriver struct {
	DriverName     string
	DataSourceName string
	DB             *sql.DB
	SQLTX          *sql.Tx
}

func InitPostgreDriver(host string, port int, user, password, dbname string) *PostgresDriver {
	dataSourceName := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db := &PostgresDriver{
		DriverName:     "postgres",
		DataSourceName: dataSourceName,
	}

	if err := db.Open(); err != nil {
		log.Panicln("Init postgre pool failed.", err.Error())
	}
	return db
}

func (db *PostgresDriver) Open() (err error) {
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

func (db *PostgresDriver) Close() error {
	return db.DB.Close()
}

func (db *PostgresDriver) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.DB.Query(query, args...)
}

func (db *PostgresDriver) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.DB.Exec(query, args...)
}

func (db *PostgresDriver) QueryMap(tableName string, query map[string]interface{}) (*sql.Rows, error) {
	s := "select * from " + tableName + " "
	where, _ := WhereFromQuery(query)
	rows, err := db.Query(s+where)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (db *PostgresDriver) FindById(tableName string, id int64) (*sql.Rows, error) {
	s := "select * from " + tableName + " where id = $1 limit 1 "
	rows, err := db.Query(s,id)
	if err != nil {

		return nil, err
	}
	return rows, nil
}

func (db *PostgresDriver) FindOne(tableName string, query map[string]interface{}, orderBy string) (*sql.Rows, error) {
	s := "select * from " + tableName
	if !CheckOrderBy(orderBy) {
		orderBy = ""
	}
	where, _ := WhereFromQuery(query)
	rows, err := db.DB.Query(s + where)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (db *PostgresDriver) GetList(tableName string, query map[string]interface{}, orderBy string) (*sql.Rows, error) {
	s := "select * from " + tableName
	if !CheckOrderBy(orderBy) {
		orderBy = ""
	}
	where, _ := WhereFromQuery(query)
	rows, err := db.DB.Query(s + where)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (db *PostgresDriver) GetPage(tableName string, query map[string]interface{}, orderBy string, page, size int64) (*sql.Rows, *Page, error) {
	var total,last,prev,next int64
	total, _ = db.Count(tableName, query)
	last = total/size + 1
	prev = 1
	if page > 2 {
		prev = page - 1
	}else{
		page = 1
	}
	next = last
	if page < last-1 {
		next = page + 1
	}
	offset := (page - 1) * size
	s := "select * from " + tableName
	if !CheckOrderBy(orderBy) {
		orderBy = ""
	}
	where, _ := WhereFromQuery(query)
	sql2 := s + where
	if orderBy != "" {
		sql2 += " order by " + orderBy
	}
	sql2 += " limit $1 offset $2"
	rows, err := db.DB.Query(sql2,size,offset)
	if err != nil {
		return nil, nil, err
	}
	return rows, &Page{First: 1, Prev: prev, Page: page, Next: next, Last: last, Size: size, Total: total}, nil
}

func (db *PostgresDriver) Count(tableName string, query map[string]interface{}) (int64, error) {
	var count int64 = 0
	s := "select count(1) as number from " + tableName
	where, _ := WhereFromQuery(query)
	rows, err := db.DB.Query(s + where)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	rows.Next()
	_ = rows.Scan(&count)
	return count, nil
}

func (db *PostgresDriver) Exists(tableName string, query map[string]interface{}) bool {
	c, err := db.Count(tableName, query)
	if err != nil {
		return false
	}
	return c > 0
}

func (db *PostgresDriver) Insert(tableName string, post map[string]interface{}) (int64, error) {
	var newId int64
	s, _ := GetInsertSql(tableName, post)
	fmt.Println(s)
	err := db.DB.QueryRow(s + "  returning id").Scan(&newId)
	if err != nil {
		return 0, err
	}
	return newId, nil
}

func (db *PostgresDriver) Update(tableName string, post map[string]interface{}, query map[string]interface{}) (int64, error) {
	s, _ := GetUpdateSQL(tableName, post, query)
	exec, err := db.DB.Exec(s)
	if err != nil {
		return 0, err
	}
	return exec.RowsAffected()
}

func (db *PostgresDriver) Save(tableName string, post map[string]interface{}) (int64, error) {
	id, ok := post["id"]
	if ok {
		delete(post, "id")
		return db.Update(tableName, post, map[string]interface{}{"id": id})
	} else {
		return db.Insert(tableName, post)
	}
}

func (db *PostgresDriver) Delete(tableName string, query map[string]interface{}) (int64, error) {
	where, _ := WhereFromQuery(query)
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

func (db *PostgresDriver) DeleteById(tableName string, id int64) (int64, error) {
	if id != 0 {
		s := "delete from " + tableName + " where id = $1"
		exec, err := db.DB.Exec(s, id)
		if err != nil {
			return 0, err
		}
		return exec.RowsAffected()
	} else {
		return 0, nil
	}
}

func (db *PostgresDriver) Begin() error {
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

func (db *PostgresDriver) RollBack() error {
	return db.SQLTX.Rollback()
}

func (db *PostgresDriver) Commit() error {
	return db.SQLTX.Commit()
}

func (db *PostgresDriver) QueryTX(query string, args ...interface{}) (*sql.Rows, error) {
	return db.SQLTX.Query(query, args...)
}

func (db *PostgresDriver) ExecTX(query string, args ...interface{}) (sql.Result, error) {
	return db.SQLTX.Exec(query, args...)
}
