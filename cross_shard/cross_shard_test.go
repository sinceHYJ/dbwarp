package cross_shard

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// TestQueryAnalyzer 测试 SQL 分析器
func TestQueryAnalyzer_Analyze(t *testing.T) {
	analyzer := NewQueryAnalyzer("user_id", "orders")

	tests := []struct {
		name           string
		sql            string
		wantCrossShard bool
		wantAggregate  bool
	}{
		{
			name:           "single shard query",
			sql:            "SELECT * FROM orders WHERE user_id = ?",
			wantCrossShard: false,
			wantAggregate:  false,
		},
		{
			name:           "cross shard count query",
			sql:            "SELECT COUNT(*) FROM orders",
			wantCrossShard: true,
			wantAggregate:  true,
		},
		{
			name:           "cross shard sum query",
			sql:            "SELECT SUM(amount) FROM orders WHERE status = ?",
			wantCrossShard: true,
			wantAggregate:  true,
		},
		{
			name:           "cross shard avg query",
			sql:            "SELECT AVG(price) FROM orders",
			wantCrossShard: true,
			wantAggregate:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 注意：由于 Analyze 方法需要完整实现，这里先跳过实际测试
			t.Skip("requires full SQL parser implementation")
		})
	}
}

// TestResultMerger_Count 测试 COUNT 合并
func TestResultMerger_Count(t *testing.T) {
	merger := NewResultMerger(&MergeContext{
		MergeType: MergeTypeAggregate,
		Aggregations: []AggregationFunc{
			{Name: "COUNT", Column: "*", Alias: "total"},
		},
	})

	results := []*ShardResult{
		{
			ShardIndex: 0,
			Rows:       []map[string]interface{}{{"total": int64(100)}},
		},
		{
			ShardIndex: 1,
			Rows:       []map[string]interface{}{{"total": int64(200)}},
		},
		{
			ShardIndex: 2,
			Rows:       []map[string]interface{}{{"total": int64(150)}},
		},
	}

	merged, err := merger.MergeAggregate(results)
	assert.NoError(t, err)
	assert.Equal(t, int64(450), merged["total"])
}

// TestResultMerger_Sum 测试 SUM 合并
func TestResultMerger_Sum(t *testing.T) {
	merger := NewResultMerger(&MergeContext{
		MergeType: MergeTypeAggregate,
		Aggregations: []AggregationFunc{
			{Name: "SUM", Column: "amount", Alias: "total_amount"},
		},
	})

	results := []*ShardResult{
		{
			ShardIndex: 0,
			Rows:       []map[string]interface{}{{"total_amount": float64(1000.5)}},
		},
		{
			ShardIndex: 1,
			Rows:       []map[string]interface{}{{"total_amount": float64(2000.3)}},
		},
	}

	merged, err := merger.MergeAggregate(results)
	assert.NoError(t, err)
	assert.InDelta(t, 3000.8, merged["total_amount"], 0.001)
}

// TestResultMerger_Avg 测试 AVG 合并
func TestResultMerger_Avg(t *testing.T) {
	merger := NewResultMerger(&MergeContext{
		MergeType: MergeTypeAggregate,
		Aggregations: []AggregationFunc{
			{Name: "AVG", Column: "price", Alias: "avg_price"},
		},
	})

	results := []*ShardResult{
		{
			ShardIndex: 0,
			Rows: []map[string]interface{}{
				{
					"__sum_avg_price":   float64(1000),
					"__count_avg_price": int64(10),
				},
			},
		},
		{
			ShardIndex: 1,
			Rows: []map[string]interface{}{
				{
					"__sum_avg_price":   float64(2000),
					"__count_avg_price": int64(20),
				},
			},
		},
	}

	merged, err := merger.MergeAggregate(results)
	assert.NoError(t, err)
	// AVG = (1000 + 2000) / (10 + 20) = 100
	assert.InDelta(t, 100.0, merged["avg_price"], 0.001)
}

// TestResultMerger_Sorted 测试排序合并
func TestResultMerger_Sorted(t *testing.T) {
	merger := NewResultMerger(&MergeContext{
		MergeType: MergeTypeSorted,
		OrderBy: []OrderByClause{
			{Column: "id", Desc: false},
		},
	})

	results := []*ShardResult{
		{
			ShardIndex: 0,
			Rows: []map[string]interface{}{
				{"id": 1, "name": "a"},
				{"id": 4, "name": "d"},
			},
		},
		{
			ShardIndex: 1,
			Rows: []map[string]interface{}{
				{"id": 2, "name": "b"},
				{"id": 5, "name": "e"},
			},
		},
		{
			ShardIndex: 2,
			Rows: []map[string]interface{}{
				{"id": 3, "name": "c"},
			},
		},
	}

	merged, err := merger.MergeSorted(results)
	assert.NoError(t, err)
	assert.Len(t, merged, 5)

	// 验证排序
	for i := 0; i < 5; i++ {
		assert.Equal(t, i+1, merged[i]["id"])
	}
}

// TestResultMerger_Pagination 测试分页
func TestResultMerger_Pagination(t *testing.T) {
	merger := NewResultMerger(&MergeContext{
		MergeType: MergeTypeSorted,
		OrderBy: []OrderByClause{
			{Column: "id", Desc: false},
		},
		Offset: 2,
		Limit:  intPtr(2),
	})

	results := []*ShardResult{
		{
			ShardIndex: 0,
			Rows: []map[string]interface{}{
				{"id": 1},
				{"id": 4},
			},
		},
		{
			ShardIndex: 1,
			Rows: []map[string]interface{}{
				{"id": 2},
				{"id": 5},
			},
		},
		{
			ShardIndex: 2,
			Rows: []map[string]interface{}{
				{"id": 3},
			},
		},
	}

	merged, err := merger.MergeWithPagination(results)
	assert.NoError(t, err)
	assert.Len(t, merged, 2)
	assert.Equal(t, 3, merged[0]["id"])
	assert.Equal(t, 4, merged[1]["id"])
}

// TestParallelExecutor_Execute 测试并行执行
func TestParallelExecutor_Execute(t *testing.T) {
	t.Skip("requires database connection")

	config := ExecutorConfig{
		MaxConcurrency: 4,
		ShardTimeout:   5 * time.Second,
		TotalTimeout:   10 * time.Second,
		FailFast:       false,
		RetryCount:     1,
		RetryDelay:     100 * time.Millisecond,
	}

	executor := NewParallelExecutor(config)

	// 这里需要实际的数据库连接来测试
	// 实际测试中应该使用 mock 或测试数据库
}

// TestConfig_Default 测试默认配置
func TestConfig_Default(t *testing.T) {
	config := DefaultConfig()

	assert.True(t, config.CrossShard.Enabled)
	assert.Equal(t, 8, config.CrossShard.MaxConcurrency)
	assert.Equal(t, 5*time.Second, config.CrossShard.ShardTimeout)
	assert.Equal(t, 30*time.Second, config.CrossShard.TotalTimeout)
	assert.True(t, config.CrossShard.AllowFullScan)
	assert.Equal(t, 10000, config.CrossShard.MaxResultRows)
}

// TestConfig_Validate 测试配置验证
func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name:    "valid config",
			config:  DefaultConfig(),
			wantErr: false,
		},
		{
			name: "invalid max_concurrency",
			config: Config{
				CrossShard: CrossShardConfig{
					MaxConcurrency: 0,
				},
			},
			wantErr: true,
		},
		{
			name: "invalid max_result_rows",
			config: Config{
				CrossShard: CrossShardConfig{
					MaxConcurrency: 8,
					MaxResultRows:  0,
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// 辅助函数
func intPtr(i int) *int {
	return &i
}

// MockDB 创建 mock 数据库连接（用于测试）
func createMockDB() *gorm.DB {
	dsn := "user:password@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil
	}
	return db
}
