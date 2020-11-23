package zerosql

import (
	"database/sql"

	"github.com/tal-tech/go-zero/core/breaker"
	"github.com/tal-tech/go-zero/core/stores/sqlx"
	"gorm.io/gorm"
)

type zerosqlConn struct {
	driverName string
	datasource string
	beginTx    beginnable
	brk        breaker.Breaker
	accept     func(error) bool
}

type ZeroSqlOption func(*zerosqlConn)

func NewZeroSqlConn(driverName, datasource string, opts ...ZeroSqlOption) sqlx.SqlConn {
	conn := &zerosqlConn{
		driverName: driverName,
		datasource: datasource,
		beginTx:    begin,
		brk:        breaker.NewBreaker(),
	}

	for _, opt := range opts {
		opt(conn)
	}

	return conn
}

func (db *zerosqlConn) Exec(query string, args ...interface{}) (result sql.Result, err error) {
	err = db.brk.DoWithAcceptable(func() error {
		var conn *gorm.DB
		conn, err = getGormSqlConn(getSqlConn(db.driverName, db.datasource))
		if err != nil {
			logInstanceError(db.datasource, err)
			return err
		}

		result, err = exec(conn, query, args...)
		return err
	}, db.acceptable)

	return
}

func (db *zerosqlConn) Prepare(query string) (stmt sqlx.StmtSession, err error) {
	err = db.brk.DoWithAcceptable(func() error {
		var conn *gorm.DB
		conn, err = getGormSqlConn(getSqlConn(db.driverName, db.datasource))
		if err != nil {
			logInstanceError(db.datasource, err)
			return err
		}

		tx := conn.Session(&gorm.Session{PrepareStmt: true})
		stmt = &zeroStmt{tx, query}
		return nil
	}, db.acceptable)

	return
}

func (db *zerosqlConn) QueryRow(v interface{}, query string, args ...interface{}) error {
	return db.queryRows(v, query, args...)
}

func (db *zerosqlConn) QueryRowPartial(v interface{}, query string, args ...interface{}) error {
	return db.queryRows(v, query, args...)
}

func (db *zerosqlConn) QueryRows(v interface{}, query string, args ...interface{}) error {
	return db.queryRows(v, query, args...)
}

func (db *zerosqlConn) QueryRowsPartial(v interface{}, query string, args ...interface{}) error {
	return db.queryRows(v, query, args...)
}

func (db *zerosqlConn) Transact(fn func(session sqlx.Session) error) error {
	return db.brk.DoWithAcceptable(func() error {
		return transact(db, db.beginTx, fn)
	}, db.acceptable)
}

func (db *zerosqlConn) acceptable(err error) bool {
	ok := err == nil || err == sql.ErrNoRows || err == sql.ErrTxDone
	if db.accept == nil {
		return ok
	} else {
		return ok || db.accept(err)
	}
}

func (db *zerosqlConn) queryRows(v interface{}, q string, args ...interface{}) error {
	var qerr error
	return db.brk.DoWithAcceptable(func() error {
		conn, err := getGormSqlConn(getSqlConn(db.driverName, db.datasource))
		if err != nil {
			logInstanceError(db.datasource, err)
			return err
		}

		return query(conn, v, q, args...)
	}, func(err error) bool {
		return qerr == err || db.acceptable(err)
	})
}
