package cross_shard

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"gorm.io/gorm"
)

// ShardResult 分片查询结果
type ShardResult struct {
	ShardIndex int
	Rows       []map[string]interface{}
	Columns    []string
	Err        error
	Duration   time.Duration
}

// QueryTask 查询任务
type QueryTask struct {
	ShardIndex int
	Query      string
	Args       []interface{}
	Pool       gorm.ConnPool
}

// ExecutorConfig 执行器配置
type ExecutorConfig struct {
	MaxConcurrency int
	ShardTimeout   time.Duration
	TotalTimeout   time.Duration
	FailFast       bool
	RetryCount     int
	RetryDelay     time.Duration
}

// ParallelExecutor 并行执行器
type ParallelExecutor struct {
	config ExecutorConfig

	// 统计
	totalQueries   int64
	successQueries int64
	failedQueries  int64
	totalDuration  int64 // nanoseconds
}

// NewParallelExecutor 创建并行执行器
func NewParallelExecutor(config ExecutorConfig) *ParallelExecutor {
	// 设置默认值
	if config.MaxConcurrency <= 0 {
		config.MaxConcurrency = 8
	}
	if config.ShardTimeout <= 0 {
		config.ShardTimeout = 5 * time.Second
	}
	if config.TotalTimeout <= 0 {
		config.TotalTimeout = 30 * time.Second
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = 100 * time.Millisecond
	}

	return &ParallelExecutor{
		config: config,
	}
}

// Execute 并行执行查询
func (e *ParallelExecutor) Execute(ctx context.Context, tasks []*QueryTask) ([]*ShardResult, error) {
	if len(tasks) == 0 {
		return nil, nil
	}

	// 创建带超时的上下文
	ctx, cancel := context.WithTimeout(ctx, e.config.TotalTimeout)
	defer cancel()

	// 结果收集
	results := make([]*ShardResult, len(tasks))
	var firstErr error
	var errOnce sync.Once

	// 并发控制
	sem := make(chan struct{}, e.config.MaxConcurrency)
	var wg sync.WaitGroup

	// 完成信号
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// 并行执行
	for i, task := range tasks {
		wg.Add(1)

		go func(idx int, t *QueryTask) {
			defer wg.Done()

			// 获取信号量
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				results[idx] = &ShardResult{
					ShardIndex: t.ShardIndex,
					Err:        ctx.Err(),
				}
				return
			}

			// 执行查询（带重试）
			result := e.executeWithRetry(ctx, t)
			results[idx] = result

			// 记录错误
			if result.Err != nil && e.config.FailFast {
				errOnce.Do(func() {
					firstErr = result.Err
					cancel() // 取消其他查询
				})
			}
		}(i, task)
	}

	// 等待完成或超时
	select {
	case <-done:
		atomic.AddInt64(&e.totalQueries, int64(len(tasks)))
		atomic.AddInt64(&e.successQueries, int64(countSuccess(results)))

		if firstErr != nil {
			return results, firstErr
		}
		return results, nil

	case <-ctx.Done():
		return results, ctx.Err()
	}
}

// executeWithRetry 带重试的查询执行
func (e *ParallelExecutor) executeWithRetry(ctx context.Context, task *QueryTask) *ShardResult {
	var lastErr error

	for attempt := 0; attempt <= e.config.RetryCount; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(e.config.RetryDelay):
			case <-ctx.Done():
				return &ShardResult{
					ShardIndex: task.ShardIndex,
					Err:        ctx.Err(),
				}
			}
		}

		result := e.executeQuery(ctx, task)
		if result.Err == nil {
			return result
		}

		lastErr = result.Err

		// 判断是否可重试错误
		if !isRetryableError(result.Err) {
			break
		}
	}

	atomic.AddInt64(&e.failedQueries, 1)

	return &ShardResult{
		ShardIndex: task.ShardIndex,
		Err:        lastErr,
	}
}

// executeQuery 执行单次查询
func (e *ParallelExecutor) executeQuery(ctx context.Context, task *QueryTask) *ShardResult {
	start := time.Now()

	result := &ShardResult{
		ShardIndex: task.ShardIndex,
	}

	// 创建分片超时上下文
	shardCtx, cancel := context.WithTimeout(ctx, e.config.ShardTimeout)
	defer cancel()

	// 执行查询
	rows, err := task.Pool.QueryContext(shardCtx, task.Query, task.Args...)
	if err != nil {
		result.Err = err
		result.Duration = time.Since(start)
		return result
	}
	defer rows.Close()

	// 读取列信息
	columns, err := rows.Columns()
	if err != nil {
		result.Err = err
		result.Duration = time.Since(start)
		return result
	}
	result.Columns = columns

	// 读取数据
	result.Rows = make([]map[string]interface{}, 0)

	for rows.Next() {
		row := make(map[string]interface{})
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			result.Err = err
			result.Duration = time.Since(start)
			return result
		}

		// 转换类型
		for i, col := range columns {
			row[col] = convertValue(values[i])
		}

		result.Rows = append(result.Rows, row)
	}

	if err := rows.Err(); err != nil {
		result.Err = err
	}

	result.Duration = time.Since(start)
	atomic.AddInt64(&e.totalDuration, int64(result.Duration))

	return result
}

// isRetryableError 判断是否可重试
func isRetryableError(err error) bool {
	// 连接超时、网络错误等可重试
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return false
	}

	return false
}

// convertValue 转换数据库值类型
func convertValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	// 处理 []byte
	if bytes, ok := v.([]byte); ok {
		return string(bytes)
	}

	return v
}

// countSuccess 统计成功数量
func countSuccess(results []*ShardResult) int {
	count := 0
	for _, r := range results {
		if r.Err == nil {
			count++
		}
	}
	return count
}

// Stats 获取统计信息
func (e *ParallelExecutor) Stats() ExecutorStats {
	total := atomic.LoadInt64(&e.totalQueries)
	var avgDuration time.Duration
	if total > 0 {
		avgDuration = time.Duration(atomic.LoadInt64(&e.totalDuration) / total)
	}

	return ExecutorStats{
		TotalQueries:   total,
		SuccessQueries: atomic.LoadInt64(&e.successQueries),
		FailedQueries:  atomic.LoadInt64(&e.failedQueries),
		AvgDuration:    avgDuration,
	}
}

// ExecutorStats 执行器统计
type ExecutorStats struct {
	TotalQueries   int64
	SuccessQueries int64
	FailedQueries  int64
	AvgDuration    time.Duration
}

// String 格式化输出
func (s ExecutorStats) String() string {
	return fmt.Sprintf("ExecutorStats{total=%d, success=%d, failed=%d, avgDuration=%v}",
		s.TotalQueries, s.SuccessQueries, s.FailedQueries, s.AvgDuration)
}
