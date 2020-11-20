package zerosql

import (
	"context"
	"time"

	"github.com/tal-tech/go-zero/core/logx"
	"gorm.io/gorm/logger"
)

type zeroLogger struct {
	logLevel logger.LogLevel
}

// NewLogger new logger.
func NewLogger() logger.Interface {
	return &zeroLogger{logger.Error}
}

// LogMode log mode
func (l *zeroLogger) LogMode(level logger.LogLevel) logger.Interface {
	l.logLevel = level
	return l
}

// Info print info
func (l zeroLogger) Info(ctx context.Context, msg string, data ...interface{}) {
	if l.logLevel >= logger.Info {
		logx.Infof(msg, data...)
	}
}

// Warn print warn messages
func (l zeroLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	if l.logLevel >= logger.Warn {
		logx.Infof(msg, data...)
	}
}

// Error print error messages
func (l zeroLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	if l.logLevel >= logger.Error {
		logx.Errorf(msg, data...)
	}
}

// Trace print sql message
func (l zeroLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
}
