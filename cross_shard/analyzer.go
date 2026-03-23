package cross_shard

import (
	"fmt"
	"strings"

	"github.com/longbridgeapp/sqlparser"
)

// QueryType 查询类型
type QueryType int

const (
	QueryTypeUnknown QueryType = iota
	QueryTypeSelect
	QueryTypeInsert
	QueryTypeUpdate
	QueryTypeDelete
)

// AggregationFunc 聚合函数
type AggregationFunc struct {
	Name     string // COUNT, SUM, AVG, MAX, MIN
	Column   string // 列名，COUNT(*) 时为 "*"
	Alias    string // AS 后的别名
	Distinct bool   // 是否 DISTINCT
}

// OrderByClause 排序子句
type OrderByClause struct {
	Column string
	Desc   bool // true = DESC, false = ASC
}

// QueryAnalysis SQL 分析结果
type QueryAnalysis struct {
	// 基础信息
	QueryType   QueryType
	OriginalSQL string

	// 表信息
	TableName     string
	ShardingKey   string // 分片键列名
	HasShardingKey bool   // WHERE 中是否包含分片键

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
	IsCrossShard     bool // 是否跨分片
	IsAggregateQuery bool // 是否聚合查询
	HasJoin          bool // 是否有 JOIN
	HasSubQuery      bool // 是否有子查询
}

// QueryAnalyzer SQL 分析器
type QueryAnalyzer struct {
	shardingKey string
	tableName   string
}

// NewQueryAnalyzer 创建 SQL 分析器
func NewQueryAnalyzer(shardingKey, tableName string) *QueryAnalyzer {
	return &QueryAnalyzer{
		shardingKey: shardingKey,
		tableName:   tableName,
	}
}

// Analyze 分析 SQL
func (a *QueryAnalyzer) Analyze(query string, args ...interface{}) (*QueryAnalysis, error) {
	analysis := &QueryAnalysis{
		OriginalSQL: query,
	}

	// 解析 SQL
	expr, err := sqlparser.NewParser(strings.NewReader(query)).ParseStatement()
	if err != nil {
		return nil, &AnalysisError{
			SQL:   query,
			Cause: err,
			Msg:   "failed to parse SQL",
		}
	}

	// 根据语句类型分发
	switch stmt := expr.(type) {
	case *sqlparser.SelectStatement:
		analysis.QueryType = QueryTypeSelect
		err = a.analyzeSelect(stmt, analysis)

	case *sqlparser.InsertStatement:
		analysis.QueryType = QueryTypeInsert
		// INSERT 不需要跨分片查询分析

	case *sqlparser.UpdateStatement:
		analysis.QueryType = QueryTypeUpdate
		// UPDATE 不需要跨分片查询分析

	case *sqlparser.DeleteStatement:
		analysis.QueryType = QueryTypeDelete
		// DELETE 不需要跨分片查询分析

	default:
		return nil, &AnalysisError{
			SQL: query,
			Msg: "unsupported SQL type",
		}
	}

	if err != nil {
		return nil, err
	}

	// 判断是否跨分片
	analysis.IsCrossShard = !analysis.HasShardingKey && analysis.QueryType == QueryTypeSelect

	// 判断是否聚合查询
	analysis.IsAggregateQuery = len(analysis.Aggregations) > 0

	return analysis, nil
}

// analyzeSelect 分析 SELECT 语句
func (a *QueryAnalyzer) analyzeSelect(stmt *sqlparser.SelectStatement, analysis *QueryAnalysis) error {
	// 1. 提取表名
	if tableName, ok := extractTableName(stmt.FromItems); ok {
		analysis.TableName = tableName
	}

	// 2. 检查 Hint（如 nosharding）
	if stmt.Hint != nil && stmt.Hint.Value == "nosharding" {
		analysis.HasShardingKey = true // 强制单分片
		return nil
	}

	// 3. 提取聚合函数
	for _, col := range stmt.Columns {
		if agg := extractAggregation(col); agg != nil {
			analysis.Aggregations = append(analysis.Aggregations, *agg)
		}
	}

	// 4. 检查 WHERE 中的分片键
	if stmt.Condition != nil {
		analysis.HasShardingKey = containsShardingKey(stmt.Condition, a.shardingKey)
	}

	// 5. 提取 ORDER BY
	for _, order := range stmt.OrderBy {
		analysis.OrderBy = append(analysis.OrderBy, OrderByClause{
			Column: extractColumnName(order.X),
			Desc:   order.Desc,
		})
	}

	// 6. 提取 GROUP BY
	if stmt.GroupBy != nil {
		for _, col := range stmt.GroupBy {
			analysis.GroupBy = append(analysis.GroupBy, extractColumnName(col))
		}
	}

	// 7. 提取 LIMIT / OFFSET
	if stmt.Limit != nil {
		limit := int(stmt.Limit.Limit)
		analysis.Limit = &limit
		if stmt.Limit.Offset != nil {
			analysis.Offset = int(*stmt.Limit.Offset)
		}
	}

	return nil
}

// extractTableName 提取表名
func extractTableName(fromItems sqlparser.FromItems) (string, bool) {
	if tableName, ok := fromItems.(*sqlparser.TableName); ok {
		return tableName.Name.Name, true
	}
	return "", false
}

// extractAggregation 从列表达式中提取聚合函数
func extractAggregation(col *sqlparser.ResultColumn) *AggregationFunc {
	if col == nil {
		return nil
	}

	// 处理 expr.Call（函数调用）
	if call, ok := col.Expr.(*sqlparser.Call); ok {
		funcName := strings.ToUpper(call.Name.Name)

		// 判断是否是聚合函数
		if isAggregateFunc(funcName) {
			agg := &AggregationFunc{
				Name: funcName,
			}

			// 提取参数
			if len(call.Args) > 0 {
				if ident, ok := call.Args[0].(*sqlparser.Ident); ok {
					agg.Column = ident.Name
				} else if _, ok := call.Args[0].(*sqlparser.StarExpr); ok {
					agg.Column = "*"
				}
			}

			// 提取别名
			if col.Alias != nil {
				agg.Alias = col.Alias.Name
			}

			return agg
		}
	}

	return nil
}

// isAggregateFunc 判断是否是聚合函数
func isAggregateFunc(name string) bool {
	aggregates := map[string]bool{
		"COUNT": true,
		"SUM":   true,
		"AVG":   true,
		"MAX":   true,
		"MIN":   true,
	}
	return aggregates[name]
}

// containsShardingKey 检查条件中是否包含分片键
func containsShardingKey(expr sqlparser.Expr, shardingKey string) bool {
	found := false

	_ = sqlparser.Walk(sqlparser.VisitFunc(func(node sqlparser.Node) error {
		if binary, ok := node.(*sqlparser.BinaryExpr); ok {
			// 检查是否是 x = value 的形式
			if binary.Op == sqlparser.EQ {
				if ident, ok := binary.X.(*sqlparser.Ident); ok {
					if ident.Name == shardingKey {
						found = true
						return nil
					}
				}
				// 也检查 QualifiedRef（如 table.column）
				if qr, ok := binary.X.(*sqlparser.QualifiedRef); ok {
					if qr.Column.Name == shardingKey {
						found = true
						return nil
					}
				}
			}
		}
		return nil
	}), expr)

	return found
}

// extractColumnName 提取列名
func extractColumnName(expr sqlparser.Expr) string {
	if expr == nil {
		return ""
	}

	switch e := expr.(type) {
	case *sqlparser.Ident:
		return e.Name
	case *sqlparser.QualifiedRef:
		return e.Column.Name
	}

	return ""
}

// AnalysisError 分析错误
type AnalysisError struct {
	SQL   string
	Cause error
	Msg   string
}

func (e *AnalysisError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("SQL analysis error: %s (SQL: %s, cause: %v)", e.Msg, e.SQL, e.Cause)
	}
	return fmt.Sprintf("SQL analysis error: %s (SQL: %s)", e.Msg, e.SQL)
}

func (e *AnalysisError) Unwrap() error {
	return e.Cause
}
