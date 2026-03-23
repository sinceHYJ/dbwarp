package cross_shard

import (
	"fmt"
	"strings"

	"github.com/longbridgeapp/sqlparser"
)

// AVGRewriteInfo AVG 改写信息
type AVGRewriteInfo struct {
	OriginalAlias string
	SumAlias      string
	CountAlias    string
	Column        string
}

// RewrittenSQL 改写后的 SQL
type RewrittenSQL struct {
	Template     string
	NeedMerge    bool
	MergeType    MergeType
	OriginalAVGs []AVGRewriteInfo
}

// SQLRewriter SQL 改写器
type SQLRewriter struct {
	originalSQL string
	analysis    *QueryAnalysis
	shardCount  int
}

// NewSQLRewriter 创建 SQL 改写器
func NewSQLRewriter(originalSQL string, analysis *QueryAnalysis, shardCount int) *SQLRewriter {
	return &SQLRewriter{
		originalSQL: originalSQL,
		analysis:    analysis,
		shardCount:  shardCount,
	}
}

// Rewrite 改写 SQL
func (r *SQLRewriter) Rewrite() (*RewrittenSQL, error) {
	result := &RewrittenSQL{
		Template:  r.originalSQL,
		NeedMerge: r.analysis.IsCrossShard,
	}

	if !r.analysis.IsCrossShard {
		return result, nil
	}

	// 确定合并类型
	if len(r.analysis.Aggregations) > 0 && len(r.analysis.GroupBy) == 0 {
		result.MergeType = MergeTypeAggregate
	} else if len(r.analysis.GroupBy) > 0 {
		result.MergeType = MergeTypeGroupBy
	} else if len(r.analysis.OrderBy) > 0 {
		result.MergeType = MergeTypeSorted
	}

	// 改写 AVG
	if r.hasAVG() {
		rewritten, info := r.rewriteAVG()
		result.Template = rewritten
		result.OriginalAVGs = info
	}

	// 优化 LIMIT
	if r.analysis.Limit != nil {
		result.Template = r.optimizeLimit(result.Template)
	}

	return result, nil
}

// hasAVG 检查是否有 AVG 函数
func (r *SQLRewriter) hasAVG() bool {
	for _, agg := range r.analysis.Aggregations {
		if agg.Name == "AVG" {
			return true
		}
	}
	return false
}

// rewriteAVG 改写 AVG 为 SUM + COUNT
func (r *SQLRewriter) rewriteAVG() (string, []AVGRewriteInfo) {
	var infoList []AVGRewriteInfo

	// 解析 SQL
	expr, err := sqlparser.NewParser(strings.NewReader(r.originalSQL)).ParseStatement()
	if err != nil {
		return r.originalSQL, nil
	}

	selectStmt, ok := expr.(*sqlparser.SelectStatement)
	if !ok {
		return r.originalSQL, nil
	}

	// 替换 AVG
	newColumns := make([]*sqlparser.ResultColumn, 0)
	replaced := false

	for _, col := range selectStmt.Columns {
		if call, ok := col.Expr.(*sqlparser.Call); ok {
			if strings.ToUpper(call.Name.Name) == "AVG" {
				replaced = true

				// 获取列名
				column := "*"
				if len(call.Args) > 0 {
					if ident, ok := call.Args[0].(*sqlparser.Ident); ok {
						column = ident.Name
					}
				}

				// 生成别名
				originalAlias := "avg_result"
				if col.Alias != nil {
					originalAlias = col.Alias.Name
				}

				sumAlias := fmt.Sprintf("__sum_%s", originalAlias)
				countAlias := fmt.Sprintf("__count_%s", originalAlias)

				// 创建 SUM 列
				sumCol := &sqlparser.ResultColumn{
					Expr: &sqlparser.Call{
						Name: &sqlparser.Ident{Name: "SUM"},
						Args: call.Args,
					},
					Alias: &sqlparser.Ident{Name: sumAlias},
				}

				// 创建 COUNT 列
				countCol := &sqlparser.ResultColumn{
					Expr: &sqlparser.Call{
						Name: &sqlparser.Ident{Name: "COUNT"},
						Args: call.Args,
					},
					Alias: &sqlparser.Ident{Name: countAlias},
				}

				newColumns = append(newColumns, sumCol, countCol)

				// 记录改写信息
				infoList = append(infoList, AVGRewriteInfo{
					OriginalAlias: originalAlias,
					SumAlias:      sumAlias,
					CountAlias:    countAlias,
					Column:        column,
				})

				continue
			}
		}

		newColumns = append(newColumns, col)
	}

	if !replaced {
		return r.originalSQL, nil
	}

	selectStmt.Columns = newColumns

	return selectStmt.String(), infoList
}

// optimizeLimit 优化 LIMIT 子句
func (r *SQLRewriter) optimizeLimit(query string) string {
	// 对于跨分片查询，每个分片需要取 offset + limit 条
	// 这样才能保证合并后的结果正确

	perShardLimit := r.analysis.Offset
	if r.analysis.Limit != nil {
		perShardLimit += *r.analysis.Limit
	}

	// 解析并修改 LIMIT
	expr, err := sqlparser.NewParser(strings.NewReader(query)).ParseStatement()
	if err != nil {
		return query
	}

	if selectStmt, ok := expr.(*sqlparser.SelectStatement); ok {
		// 设置新的 LIMIT（去掉 OFFSET）
		limit := int64(perShardLimit)
		selectStmt.Limit = &sqlparser.LimitClause{
			Limit: limit,
			// 不设置 Offset，因为合并时会处理
		}

		return selectStmt.String()
	}

	return query
}

// addLimitToQuery 添加 LIMIT 到查询
func addLimitToQuery(query string, limit int) string {
	// 检查是否已有 LIMIT
	if strings.Contains(strings.ToUpper(query), "LIMIT") {
		return query
	}

	// 解析 SQL
	expr, err := sqlparser.NewParser(strings.NewReader(query)).ParseStatement()
	if err != nil {
		return query + fmt.Sprintf(" LIMIT %d", limit)
	}

	if selectStmt, ok := expr.(*sqlparser.SelectStatement); ok {
		selectStmt.Limit = &sqlparser.LimitClause{
			Limit: int64(limit),
		}
		return selectStmt.String()
	}

	return query + fmt.Sprintf(" LIMIT %d", limit)
}
