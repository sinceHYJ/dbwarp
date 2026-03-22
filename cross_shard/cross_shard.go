package cross_shard

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"gorm.io/gorm"
)

// CrossShardExecutor 跨分片查询执行器
type CrossShardExecutor struct {
	config    Config
	analyzers map[string]*QueryAnalyzer // table -> analyzer
	executor  *ParallelExecutor

	mu sync.RWMutex
}

// NewCrossShardExecutor 创建跨分片查询执行器
func NewCrossShardExecutor(config Config) *CrossShardExecutor {
	return &CrossShardExecutor{
		config:    config,
		analyzers: make(map[string]*QueryAnalyzer),
		executor: NewParallelExecutor(ExecutorConfig{
			MaxConcurrency: config.CrossShard.MaxConcurrency,
			ShardTimeout:   config.CrossShard.ShardTimeout,
			TotalTimeout:   config.CrossShard.TotalTimeout,
			FailFast:       config.CrossShard.FailFast,
			RetryCount:     config.CrossShard.RetryCount,
			RetryDelay:     config.CrossShard.RetryDelay,
		}),
	}
}

// RegisterTable 注册表分析器
func (e *CrossShardExecutor) RegisterTable(tableName, shardingKey string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.analyzers[tableName] = NewQueryAnalyzer(shardingKey, tableName)
}

// Execute 执行跨分片查询
func (e *CrossShardExecutor) Execute(
	ctx context.Context,
	query string,
	args []interface{},
	tableName string,
	getShardPools func() []gorm.ConnPool,
) (*QueryResult, error) {
	start := time.Now()

	// 1. 检查是否启用
	if !e.config.CrossShard.Enabled {
		return nil, errors.New("cross-shard query is disabled")
	}

	// 2. 获取分析器
	e.mu.RLock()
	analyzer, ok := e.analyzers[tableName]
	e.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("table %s not registered", tableName)
	}

	// 3. 分析 SQL
	analysis, err := analyzer.Analyze(query, args...)
	if err != nil {
		return nil, fmt.Errorf("SQL analysis failed: %w", err)
	}

	// 4. 如果不需要跨分片，返回 nil（走原有逻辑）
	if !analysis.IsCrossShard {
		return nil, nil
	}

	// 5. 安全检查
	if err := e.checkSafety(analysis); err != nil {
		return nil, err
	}

	// 6. 获取所有分片连接池
	pools := getShardPools()
	if len(pools) == 0 {
		return nil, errors.New("no available shards")
	}

	// 7. SQL 改写
	rewriter := NewSQLRewriter(query, analysis, len(pools))
	rewritten, err := rewriter.Rewrite()
	if err != nil {
		return nil, fmt.Errorf("SQL rewrite failed: %w", err)
	}

	// 8. 构建查询任务
	tasks := make([]*QueryTask, len(pools))
	for i, pool := range pools {
		tasks[i] = &QueryTask{
			ShardIndex: i,
			Query:      rewritten.Template,
			Args:       args,
			Pool:       pool,
		}
	}

	// 9. 并行执行
	shardResults, err := e.executor.Execute(ctx, tasks)
	if err != nil {
		return nil, fmt.Errorf("parallel execution failed: %w", err)
	}

	// 10. 结果合并
	mergeCtx := &MergeContext{
		MergeType:    determineMergeType(analysis),
		Aggregations: analysis.Aggregations,
		OrderBy:      analysis.OrderBy,
		GroupBy:      analysis.GroupBy,
		Limit:        analysis.Limit,
		Offset:       analysis.Offset,
	}

	merger := NewResultMerger(mergeCtx)
	merged, err := merger.Merge(shardResults)
	if err != nil {
		return nil, fmt.Errorf("result merge failed: %w", err)
	}

	// 11. 处理 AVG 改写的合并
	if len(rewritten.OriginalAVGs) > 0 {
		merged = e.recoverAVG(merged, rewritten.OriginalAVGs)
	}

	// 12. 构建结果
	result := &QueryResult{
		Duration:    time.Since(start),
		ShardCount:  len(pools),
		SuccessCount: countSuccess(shardResults),
	}

	// 根据合并类型设置结果
	switch merged := merged.(type) {
	case map[string]interface{}:
		result.Aggregates = merged
		result.Rows = []map[string]interface{}{merged}
	case []map[string]interface{}:
		result.Rows = merged
		result.Total = int64(len(merged))
	}

	// 13. 检查结果限制
	if result.Total > int64(e.config.CrossShard.MaxResultRows) {
		return nil, fmt.Errorf("result rows exceed limit (%d > %d)",
			result.Total, e.config.CrossShard.MaxResultRows)
	}

	// 14. 记录慢查询
	if result.Duration > e.config.CrossShard.SlowQueryThreshold {
		e.logSlowQuery(query, result)
	}

	return result, nil
}

// checkSafety 安全检查
func (e *CrossShardExecutor) checkSafety(analysis *QueryAnalysis) error {
	// 检查是否允许全分片扫描
	if !e.config.CrossShard.AllowFullScan {
		return errors.New("cross-shard query not allowed, please add sharding key")
	}

	// 检查是否有 JOIN 或子查询
	if analysis.HasJoin {
		return errors.New("cross-shard query with JOIN is not supported")
	}
	if analysis.HasSubQuery {
		return errors.New("cross-shard query with subquery is not supported")
	}

	return nil
}

// determineMergeType 确定合并类型
func determineMergeType(analysis *QueryAnalysis) MergeType {
	if len(analysis.Aggregations) > 0 && len(analysis.GroupBy) == 0 {
		return MergeTypeAggregate
	}
	if len(analysis.GroupBy) > 0 {
		return MergeTypeGroupBy
	}
	if len(analysis.OrderBy) > 0 {
		return MergeTypeSorted
	}
	return MergeTypeNone
}

// recoverAVG 恢复 AVG 结果
func (e *CrossShardExecutor) recoverAVG(merged interface{}, avgInfos []AVGRewriteInfo) interface{} {
	switch m := merged.(type) {
	case map[string]interface{}:
		// 聚合结果
		for _, info := range avgInfos {
			sum := toFloat64(m[info.SumAlias])
			count := toFloat64(m[info.CountAlias])

			var avg float64
			if count > 0 {
				avg = sum / count
			}

			m[info.OriginalAlias] = avg
			delete(m, info.SumAlias)
			delete(m, info.CountAlias)
		}
		return m

	case []map[string]interface{}:
		// GROUP BY 结果
		for _, row := range m {
			for _, info := range avgInfos {
				sum := toFloat64(row[info.SumAlias])
				count := toFloat64(row[info.CountAlias])

				var avg float64
				if count > 0 {
					avg = sum / count
				}

				row[info.OriginalAlias] = avg
				delete(row, info.SumAlias)
				delete(row, info.CountAlias)
			}
		}
		return m
	}

	return merged
}

// logSlowQuery 记录慢查询
func (e *CrossShardExecutor) logSlowQuery(query string, result *QueryResult) {
	// 简单的日志输出，实际可以使用结构化日志
	fmt.Printf("[SlowQuery] duration=%v, shards=%d, rows=%d, sql=%s\n",
		result.Duration, result.ShardCount, result.Total, truncate(query, 100))
}

// truncate 截断字符串
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// QueryResult 查询结果
type QueryResult struct {
	Rows         []map[string]interface{}
	Aggregates   map[string]interface{}
	Total        int64
	Duration     time.Duration
	ShardCount   int
	SuccessCount int
}

// IsAggregate 是否是聚合结果
func (r *QueryResult) IsAggregate() bool {
	return r.Aggregates != nil
}

// Stats 获取执行器统计
func (e *CrossShardExecutor) Stats() ExecutorStats {
	return e.executor.Stats()
}
