package zerosql

import (
	"github.com/go-sql-driver/mysql"
	"github.com/tal-tech/go-zero/core/stores/sqlx"
)

const (
	mysqlDriverName           = "mysql"
	duplicateEntryCode uint16 = 1062
)

// NewZeroMysql 利用gorm来适配go-zero的sqlx.
func NewZeroMysql(datasource string, opts ...ZeroSqlOption) sqlx.SqlConn {
	opts = append(opts, withMysqlAcceptable())
	return NewZeroSqlConn(mysqlDriverName, datasource, opts...)
}

func mysqlAcceptable(err error) bool {
	if err == nil {
		return true
	}

	myerr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}

	switch myerr.Number {
	case duplicateEntryCode:
		return true
	default:
		return false
	}
}

func withMysqlAcceptable() ZeroSqlOption {
	return func(conn *zerosqlConn) {
		conn.accept = mysqlAcceptable
	}
}
