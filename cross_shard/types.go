package cross_shard

// QueryAnalysis SQL 分析结果（扩展版）
type QueryAnalysis struct {
	// 基础信息
	QueryType   QueryType
	OriginalSQL string

	// 表信息
	TableName      string
	ShardingKey    string
	HasShardingKey bool

	// 聚合信息
	Aggregations []AggregationFunc

	// 排序信息
	OrderBy []OrderByClause

	// 分组信息
	GroupBy []string

	// 分页信息
	Limit  *int
	Offset int

	// 查询特征
	IsCrossShard     bool
	IsAggregateQuery bool
	HasJoin          bool
	HasSubQuery      bool
}

// IsCrossShard 判断是否跨分片查询
func (a *QueryAnalysis) IsCrossShardQuery() bool {
	return a.IsCrossShard
}

// HasAggregation 判断是否有聚合函数
func (a *QueryAnalysis) HasAggregation() bool {
	return len(a.Aggregations) > 0
}

// HasGroupBy 判断是否有 GROUP BY
func (a *QueryAnalysis) HasGroupBy() bool {
	return len(a.GroupBy) > 0
}

// HasOrderBy 判断是否有 ORDER BY
func (a *QueryAnalysis) HasOrderBy() bool {
	return len(a.OrderBy) > 0
}

// HasLimit 判断是否有 LIMIT
func (a *QueryAnalysis) HasLimit() bool {
	return a.Limit != nil
}

// Analyze 执行 SQL 分析（QueryAnalyzer 的方法）
func (a *QueryAnalyzer) Analyze(query string, args ...interface{}) (*QueryAnalysis, error) {
	analysis := &QueryAnalysis{
		OriginalSQL: query,
		ShardingKey: a.shardingKey,
		TableName:   a.tableName,
	}

	// 简化的分析逻辑
	// 实际实现需要完整解析 SQL
	analysis.QueryType = QueryTypeSelect

	// TODO: 完整的 SQL 解析和分析
	// 这里只做简化处理，实际需要调用 analyzeSelect 等方法

	return analysis, nil
}

// MergeType 合并类型常量
const (
	MergeTypeNone      MergeType = iota // 无需合并
	MergeTypeAggregate                  // 聚合合并
	MergeTypeSorted                     // 排序合并
	MergeTypeGroupBy                    // GROUP BY 合并
)

// AVGRewriteInfo AVG 改写信息（用于 SQL 改写）
type AVGRewriteInfo struct {
	OriginalAlias string
	SumAlias      string
	CountAlias    string
	Column        string
}

// SQLRewriter SQL 改写器（补充方法）
type SQLRewriterExt struct {
	originalSQL string
	analysis    *QueryAnalysis
	shardCount  int
}

// NewSQLRewriterExt 创建扩展的 SQL 改写器
func NewSQLRewriterExt(originalSQL string, analysis *QueryAnalysis, shardCount int) *SQLRewriterExt {
	return &SQLRewriterExt{
		originalSQL: originalSQL,
		analysis:    analysis,
		shardCount:  shardCount,
	}
}

// Rewrite 执行 SQL 改写
func (r *SQLRewriterExt) Rewrite() (*RewrittenSQL, error) {
	rewriter := NewSQLRewriter(r.originalSQL, r.analysis, r.shardCount)
	return rewriter.Rewrite()
}

// countSuccess 统计成功数量（辅助函数）
func countSuccess(results []*ShardResult) int {
	count := 0
	for _, r := range results {
		if r.Err == nil {
			count++
		}
	}
	return count
}

// toFloat64 转换为 float64（辅助函数，已在 merger.go 中定义）
// 这里声明以避免重复定义
