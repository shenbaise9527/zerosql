package zerosql

import (
	"database/sql"
	"fmt"

	"github.com/tal-tech/go-zero/core/stores/sqlx"
	"gorm.io/gorm"
)

type (
	beginnable func(*gorm.DB) (trans, error)

	trans interface {
		sqlx.Session
		Commit() error
		Rollback() error
	}

	txSession struct {
		*gorm.DB
	}
)

func (t txSession) Exec(q string, args ...interface{}) (sql.Result, error) {
	return exec(t.DB, q, args...)
}

func (t txSession) Prepare(q string) (sqlx.StmtSession, error) {
	tx := t.DB.Session(&gorm.Session{PrepareStmt: true})
	stmt := &zeroStmt{tx, q}
	return stmt, nil
}

func (t txSession) QueryRow(v interface{}, q string, args ...interface{}) error {
	return query(t.DB, v, q, args...)
}

func (t txSession) QueryRowPartial(v interface{}, q string, args ...interface{}) error {
	return query(t.DB, v, q, args...)
}

func (t txSession) QueryRows(v interface{}, q string, args ...interface{}) error {
	return query(t.DB, v, q, args...)
}

func (t txSession) QueryRowsPartial(v interface{}, q string, args ...interface{}) error {
	return query(t.DB, v, q, args...)
}

func (t txSession) Commit() error {
	return t.DB.Commit().Error
}

func (t txSession) Rollback() error {
	return t.DB.Rollback().Error
}

func begin(db *gorm.DB) (trans, error) {
	txConn := db.Begin()
	err := txConn.Error
	if err != nil {
		return nil, err
	} else {
		return txSession{
			DB: txConn,
		}, nil
	}
}

func transact(db *zerosqlConn, b beginnable, fn func(sqlx.Session) error) (err error) {
	conn, err := getSqlConn(db.driverName, db.datasource)
	if err != nil {
		logInstanceError(db.datasource, err)
		return err
	}

	return transactOnConn(conn, b, fn)
}

func transactOnConn(conn *gorm.DB, b beginnable, fn func(sqlx.Session) error) (err error) {
	var tx trans
	tx, err = b(conn)
	if err != nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			if e := tx.Rollback(); e != nil {
				err = fmt.Errorf("recover from %#v, rollback failed: %s", p, e)
			} else {
				err = fmt.Errorf("recoveer from %#v", p)
			}
		} else if err != nil {
			if e := tx.Rollback(); e != nil {
				err = fmt.Errorf("transaction failed: %s, rollback failed: %s", err, e)
			}
		} else {
			err = tx.Commit()
		}
	}()

	return fn(tx)
}
