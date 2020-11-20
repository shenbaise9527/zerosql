package zerosql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

func (s *zeroStmt) getStmt() (*gorm.PreparedStmtDB, error) {
	stmtManager, ok := s.db.ConnPool.(*gorm.PreparedStmtDB)
	if !ok {
		return nil, errors.New("sql: should assign PreparedStatement Manager back to database when using PrepareStmt mode")
	}

	return stmtManager, nil
}

func (s *zeroStmt) Close() error {
	stmtManager, err := s.getStmt()
	if err != nil {
		return err
	}

	stmtManager.Close()
	return nil
}

func (s *zeroStmt) Exec(args ...interface{}) (sql.Result, error) {
	stmtManager, err := s.getStmt()
	if err != nil {
		return nil, err
	}

	stmt := fmt.Sprint(args...)
	startTime := timex.Now()
	result, err := stmtManager.ExecContext(context.Background(), s.query, args...)
	duration := timex.Since(startTime)
	if duration > slowThreshold {
		logx.WithDuration(duration).Slowf("[SQL] execStmt: slowcall - %s", stmt)
	} else {
		logx.WithDuration(duration).Infof("sql execStmt: %s", stmt)
	}
	if err != nil {
		logSqlError(stmt, err)
	}

	return result, err
}

func (s *zeroStmt) QueryRow(v interface{}, args ...interface{}) error {
	stmtManager, err := s.getStmt()
	if err != nil {
		return err
	}

	stmt := fmt.Sprint(args...)
	startTime := timex.Now()
	row := stmtManager.QueryRowContext(context.Background(), s.query, args...)
	err = row.Scan(v)
	duration := timex.Since(startTime)
	if duration > slowThreshold {
		logx.WithDuration(duration).Slowf("[SQL] queryStmt: slowcall - %s", stmt)
	} else {
		logx.WithDuration(duration).Infof("sql queryStmt: %s", stmt)
	}
	if err != nil {
		logSqlError(stmt, err)
	}

	return err
}

func (s *zeroStmt) QueryRowPartial(v interface{}, args ...interface{}) error {
	return s.QueryRow(v, args...)
}

func (s *zeroStmt) QueryRows(v interface{}, args ...interface{}) error {
	stmtManager, err := s.getStmt()
	if err != nil {
		return err
	}

	stmt := fmt.Sprint(args...)
	startTime := timex.Now()
	rows, err := stmtManager.QueryContext(context.Background(), s.query, args...)
	duration := timex.Since(startTime)
	if duration > slowThreshold {
		logx.WithDuration(duration).Slowf("[SQL] queryStmt: slowcall - %s", stmt)
	} else {
		logx.WithDuration(duration).Infof("sql queryStmt: %s", stmt)
	}
	if err != nil {
		logSqlError(stmt, err)
	}

	defer rows.Close()
	return s.db.ScanRows(rows, v)
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
