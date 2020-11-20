package zerosql

import (
	"database/sql"
	"errors"
	"time"

	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/core/timex"
	"gorm.io/gorm"
)

const slowThreshold = time.Millisecond * 500

type zeroStmt struct {
	db    *gorm.DB
	query string
}

func (s *zeroStmt) Close() error {
	stmtManager, ok := s.db.ConnPool.(*gorm.PreparedStmtDB)
	if !ok {
		return errors.New("sql: should assign PreparedStatement Manager back to database when using PrepareStmt mode")
	}

	stmtManager.Close()
	return nil
}

func (s *zeroStmt) Exec(args ...interface{}) (sql.Result, error) {
	return exec(s.db, s.query, args...)
}

func (s *zeroStmt) QueryRow(v interface{}, args ...interface{}) error {
	return query(s.db, v, s.query, args...)
}

func (s *zeroStmt) QueryRowPartial(v interface{}, args ...interface{}) error {
	return s.QueryRow(v, args...)
}

func (s *zeroStmt) QueryRows(v interface{}, args ...interface{}) error {
	return s.QueryRow(v, args...)
}

func (s *zeroStmt) QueryRowsPartial(v interface{}, args ...interface{}) error {
	return s.QueryRows(v, args...)
}

func exec(conn *gorm.DB, q string, args ...interface{}) (sql.Result, error) {
	stmt, err := format(q, args...)
	if err != nil {
		return nil, err
	}

	startTime := timex.Now()
	tx := conn.Exec(q, args...)
	result := &zerosqlResult{0, tx.RowsAffected, tx.Error}
	err = tx.Error
	duration := timex.Since(startTime)
	if duration > slowThreshold {
		logx.WithDuration(duration).Slowf("[SQL] exec: slowcall - %s", stmt)
	} else {
		logx.WithDuration(duration).Infof("sql exec: %s", stmt)
	}
	if err != nil {
		logSqlError(stmt, err)
	}

	return result, err
}

func query(conn *gorm.DB, v interface{}, q string, args ...interface{}) error {
	stmt, err := format(q, args...)
	if err != nil {
		return err
	}

	startTime := timex.Now()
	err = conn.Raw(q, args...).Find(v).Error
	duration := timex.Since(startTime)
	if duration > slowThreshold {
		logx.WithDuration(duration).Slowf("[SQL] query: slowcall - %s", stmt)
	} else {
		logx.WithDuration(duration).Infof("sql query: %s", stmt)
	}
	if err != nil {
		logSqlError(stmt, err)
	}

	return err
}
