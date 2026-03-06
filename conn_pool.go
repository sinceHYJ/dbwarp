package dbwarp

import (
	"context"
	"database/sql"
	"time"

	"gorm.io/gorm"
)

// ConnPool Implement a ConnPool for replace db.Statement.ConnPool in Gorm
type ConnPool struct {

	// db, This is global db instance
	sharding *WarpItem
	gorm.ConnPool

	// op indicates the operation type (read/write), used to select the correct sharded connection pool
	op Operation

	// stmt 保存原始 gorm.Statement 引用，用于 PrepareStmt 处理
	stmt *gorm.Statement
}

func (pool *ConnPool) String() string {
	return "gorm:sharding:conn_pool"
}

func (pool ConnPool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return pool.ConnPool.PrepareContext(ctx, query)
}

func (pool ConnPool) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	var (
		curTime = time.Now()
	)

	ftQuery, stQuery, table, dbIndex, err := pool.sharding.resolve(query, args...)
	if err != nil {
		return nil, err
	}
	pool.sharding.w.Logger.Info(ctx, "sharding resolve query", query, ftQuery, stQuery, table)
	pool.sharding.querys.Store("last_query", stQuery)

	// Get the correct connection pool based on sharding index
	targetPool := pool.sharding.getTargetPool(dbIndex, pool.op, pool.stmt)
	if targetPool == nil {
		targetPool = pool.ConnPool
	}

	var result sql.Result
	result, err = targetPool.ExecContext(ctx, stQuery, args...)
	pool.sharding.w.Logger.Trace(ctx, curTime, func() (sql string, rowsAffected int64) {
		if result != nil {
			rowsAffected, _ = result.RowsAffected()
		}

		return pool.sharding.w.Explain(stQuery, args...), rowsAffected
	}, pool.sharding.w.Error)

	return result, err
}

// https://github.com/go-gorm/gorm/blob/v1.21.11/callbacks/query.go#L18
func (pool ConnPool) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	var (
		curTime = time.Now()
	)

	_, stQuery, _, dbIndex, err := pool.sharding.resolve(query, args...)
	if err != nil {
		return nil, err
	}

	pool.sharding.querys.Store("last_query", stQuery)

	// Get the correct connection pool based on sharding index
	targetPool := pool.sharding.getTargetPool(dbIndex, pool.op, pool.stmt)
	if targetPool == nil {
		targetPool = pool.ConnPool
	}

	var rows *sql.Rows
	rows, err = targetPool.QueryContext(ctx, stQuery, args...)
	pool.sharding.w.Logger.Trace(ctx, curTime, func() (sql string, rowsAffected int64) {
		return pool.sharding.w.Explain(stQuery, args...), 0
	}, pool.sharding.w.Error)

	return rows, err
}

func (pool ConnPool) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	_, query, _, dbIndex, _ := pool.sharding.resolve(query, args...)
	pool.sharding.querys.Store("last_query", query)

	// Get the correct connection pool based on sharding index
	targetPool := pool.sharding.getTargetPool(dbIndex, pool.op, pool.stmt)
	if targetPool == nil {
		targetPool = pool.ConnPool
	}

	return targetPool.QueryRowContext(ctx, query, args...)
}

// BeginTx Implement ConnPoolBeginner.BeginTx
func (pool *ConnPool) BeginTx(ctx context.Context, opt *sql.TxOptions) (gorm.ConnPool, error) {
	if basePool, ok := pool.ConnPool.(gorm.ConnPoolBeginner); ok {
		return basePool.BeginTx(ctx, opt)
	}

	return pool, nil
}

// Implement TxCommitter.Commit
func (pool *ConnPool) Commit() error {
	if _, ok := pool.ConnPool.(*sql.Tx); ok {
		return nil
	}

	if basePool, ok := pool.ConnPool.(gorm.TxCommitter); ok {
		return basePool.Commit()
	}

	return nil
}

// Implement TxCommitter.Rollback
func (pool *ConnPool) Rollback() error {
	if _, ok := pool.ConnPool.(*sql.Tx); ok {
		return nil
	}

	if basePool, ok := pool.ConnPool.(gorm.TxCommitter); ok {
		return basePool.Rollback()
	}

	return nil
}

func (pool *ConnPool) Ping() error {
	return nil
}
