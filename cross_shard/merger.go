package cross_shard

import (
	"container/heap"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// MergeType 合并类型
type MergeType int

const (
	MergeTypeNone MergeType = iota
	MergeTypeAggregate
	MergeTypeSorted
	MergeTypeGroupBy
)

// MergeContext 合并上下文
type MergeContext struct {
	MergeType    MergeType
	Aggregations []AggregationFunc
	OrderBy      []OrderByClause
	GroupBy      []string
	Limit        *int
	Offset       int
	Columns      []string
}

// ResultMerger 结果合并器
type ResultMerger struct {
	ctx *MergeContext
}

// NewResultMerger 创建结果合并器
func NewResultMerger(ctx *MergeContext) *ResultMerger {
	return &ResultMerger{ctx: ctx}
}

// Merge 合并结果
func (m *ResultMerger) Merge(results []*ShardResult) (interface{}, error) {
	if len(results) == 0 {
		return nil, nil
	}

	switch m.ctx.MergeType {
	case MergeTypeAggregate:
		return m.MergeAggregate(results)

	case MergeTypeSorted:
		if m.ctx.Limit != nil {
			return m.MergeWithPagination(results)
		}
		return m.MergeSorted(results)

	case MergeTypeGroupBy:
		return m.MergeGroupBy(results)

	default:
		return m.MergeSorted(results)
	}
}

// MergeAggregate 合并聚合结果
func (m *ResultMerger) MergeAggregate(results []*ShardResult) (map[string]interface{}, error) {
	if len(results) == 0 {
		return nil, nil
	}

	merged := make(map[string]interface{})

	for _, agg := range m.ctx.Aggregations {
		var value interface{}
		var err error

		switch agg.Name {
		case "COUNT":
			value, err = m.mergeCount(results, agg)
		case "SUM":
			value, err = m.mergeSum(results, agg)
		case "AVG":
			value, err = m.mergeAvg(results, agg)
		case "MAX":
			value, err = m.mergeMax(results, agg)
		case "MIN":
			value, err = m.mergeMin(results, agg)
		}

		if err != nil {
			return nil, err
		}

		// 使用别名或函数名作为 key
		key := agg.Alias
		if key == "" {
			key = agg.Name
		}
		merged[key] = value
	}

	return merged, nil
}

// mergeCount 合并 COUNT
func (m *ResultMerger) mergeCount(results []*ShardResult, agg AggregationFunc) (int64, error) {
	var total int64

	for _, result := range results {
		if result.Err != nil || len(result.Rows) == 0 {
			continue
		}

		// 从第一行提取 COUNT 值
		row := result.Rows[0]
		if v, ok := row[agg.Alias]; ok {
			total += toInt64(v)
		} else if v, ok := row[agg.Name]; ok {
			total += toInt64(v)
		} else if v, ok := row["COUNT("+agg.Column+")"]; ok {
			total += toInt64(v)
		} else if v, ok := row["count("+agg.Column+")"]; ok {
			total += toInt64(v)
		}
	}

	return total, nil
}

// mergeSum 合并 SUM
func (m *ResultMerger) mergeSum(results []*ShardResult, agg AggregationFunc) (float64, error) {
	var total float64

	for _, result := range results {
		if result.Err != nil || len(result.Rows) == 0 {
			continue
		}

		row := result.Rows[0]
		if v, ok := row[agg.Alias]; ok {
			total += toFloat64(v)
		} else if v, ok := row[agg.Name]; ok {
			total += toFloat64(v)
		} else if v, ok := row["SUM("+agg.Column+")"]; ok {
			total += toFloat64(v)
		}
	}

	return total, nil
}

// mergeAvg 合并 AVG（需要 SUM 和 COUNT）
func (m *ResultMerger) mergeAvg(results []*ShardResult, agg AggregationFunc) (float64, error) {
	var sum float64
	var count int64

	for _, result := range results {
		if result.Err != nil || len(result.Rows) == 0 {
			continue
		}

		row := result.Rows[0]

		// 提取 SUM
		if v, ok := row["SUM("+agg.Column+")"]; ok {
			sum += toFloat64(v)
		} else if v, ok := row["sum("+agg.Column+")"]; ok {
			sum += toFloat64(v)
		} else if v, ok := row["__sum_"+agg.Alias]; ok {
			sum += toFloat64(v)
		}

		// 提取 COUNT
		if v, ok := row["COUNT("+agg.Column+")"]; ok {
			count += toInt64(v)
		} else if v, ok := row["count("+agg.Column+")"]; ok {
			count += toInt64(v)
		} else if v, ok := row["__count_"+agg.Alias]; ok {
			count += toInt64(v)
		}
	}

	if count == 0 {
		return 0, nil
	}

	return sum / float64(count), nil
}

// mergeMax 合并 MAX
func (m *ResultMerger) mergeMax(results []*ShardResult, agg AggregationFunc) (interface{}, error) {
	var maxValue interface{}
	var hasValue bool

	for _, result := range results {
		if result.Err != nil || len(result.Rows) == 0 {
			continue
		}

		row := result.Rows[0]
		var v interface{}

		if val, ok := row[agg.Alias]; ok {
			v = val
		} else if val, ok := row[agg.Name]; ok {
			v = val
		} else if val, ok := row["MAX("+agg.Column+")"]; ok {
			v = val
		} else {
			continue
		}

		if !hasValue {
			maxValue = v
			hasValue = true
		} else if compareValues(v, maxValue) > 0 {
			maxValue = v
		}
	}

	return maxValue, nil
}

// mergeMin 合并 MIN
func (m *ResultMerger) mergeMin(results []*ShardResult, agg AggregationFunc) (interface{}, error) {
	var minValue interface{}
	var hasValue bool

	for _, result := range results {
		if result.Err != nil || len(result.Rows) == 0 {
			continue
		}

		row := result.Rows[0]
		var v interface{}

		if val, ok := row[agg.Alias]; ok {
			v = val
		} else if val, ok := row[agg.Name]; ok {
			v = val
		} else if val, ok := row["MIN("+agg.Column+")"]; ok {
			v = val
		} else {
			continue
		}

		if !hasValue {
			minValue = v
			hasValue = true
		} else if compareValues(v, minValue) < 0 {
			minValue = v
		}
	}

	return minValue, nil
}

// MergeSorted 排序合并（归并排序）
func (m *ResultMerger) MergeSorted(results []*ShardResult) ([]map[string]interface{}, error) {
	if len(results) == 0 {
		return nil, nil
	}

	// 如果只有一个分片，直接返回
	if len(results) == 1 {
		return results[0].Rows, nil
	}

	// 使用堆进行多路归并
	h := NewRowHeap(m.ctx.OrderBy)

	// 初始化：每个分片的第一行入堆
	for shardIdx, result := range results {
		if result.Err != nil || len(result.Rows) == 0 {
			continue
		}

		heap.Push(h, HeapItem{
			Row:        result.Rows[0],
			ShardIndex: shardIdx,
			RowIndex:   0,
		})
	}

	// 归并
	merged := make([]map[string]interface{}, 0, m.estimateTotalRows(results))

	for h.Len() > 0 {
		// 取出最小/最大元素
		item := heap.Pop(h).(HeapItem)
		merged = append(merged, item.Row)

		// 从同一分片取下一行
		nextRowIndex := item.RowIndex + 1
		if nextRowIndex < len(results[item.ShardIndex].Rows) {
			heap.Push(h, HeapItem{
				Row:        results[item.ShardIndex].Rows[nextRowIndex],
				ShardIndex: item.ShardIndex,
				RowIndex:   nextRowIndex,
			})
		}
	}

	return merged, nil
}

// MergeWithPagination 带分页的合并
func (m *ResultMerger) MergeWithPagination(results []*ShardResult) ([]map[string]interface{}, error) {
	// 1. 先合并排序
	merged, err := m.MergeSorted(results)
	if err != nil {
		return nil, err
	}

	total := len(merged)

	// 2. 计算分页边界
	start := m.ctx.Offset
	if start >= total {
		return []map[string]interface{}{}, nil
	}

	end := total
	if m.ctx.Limit != nil {
		end = start + *m.ctx.Limit
		if end > total {
			end = total
		}
	}

	return merged[start:end], nil
}

// MergeGroupBy GROUP BY 合并
func (m *ResultMerger) MergeGroupBy(results []*ShardResult) ([]map[string]interface{}, error) {
	if len(results) == 0 {
		return nil, nil
	}

	// 1. 合并相同 group key 的数据
	groupedData := make(map[string][]map[string]interface{})
	groupKeys := make([]string, 0) // 保持顺序

	for _, result := range results {
		if result.Err != nil {
			continue
		}

		for _, row := range result.Rows {
			// 生成 group key
			key := m.buildGroupKey(row)

			if _, exists := groupedData[key]; !exists {
				groupKeys = append(groupKeys, key)
			}

			groupedData[key] = append(groupedData[key], row)
		}
	}

	// 2. 对每个 group 重新计算聚合值
	finalResult := make([]map[string]interface{}, 0, len(groupedData))

	for _, key := range groupKeys {
		rows := groupedData[key]

		// 复制第一行的非聚合列
		resultRow := make(map[string]interface{})
		for k, v := range rows[0] {
			// 跳过聚合列
			if m.isAggregateColumn(k) {
				continue
			}
			resultRow[k] = v
		}

		// 重新计算聚合值
		for _, agg := range m.ctx.Aggregations {
			value := m.computeAggregate(agg, rows)

			colName := agg.Alias
			if colName == "" {
				colName = agg.Name
			}
			resultRow[colName] = value
		}

		finalResult = append(finalResult, resultRow)
	}

	return finalResult, nil
}

// buildGroupKey 构建分组键
func (m *ResultMerger) buildGroupKey(row map[string]interface{}) string {
	var parts []string

	for _, col := range m.ctx.GroupBy {
		v := row[col]
		parts = append(parts, fmt.Sprintf("%v", v))
	}

	return strings.Join(parts, "|")
}

// isAggregateColumn 判断是否是聚合列
func (m *ResultMerger) isAggregateColumn(col string) bool {
	for _, agg := range m.ctx.Aggregations {
		if col == agg.Name || col == agg.Alias {
			return true
		}
		if strings.Contains(col, agg.Name+"(") {
			return true
		}
	}
	return false
}

// computeAggregate 计算聚合值
func (m *ResultMerger) computeAggregate(agg AggregationFunc, rows []map[string]interface{}) interface{} {
	switch agg.Name {
	case "COUNT":
		return int64(len(rows))

	case "SUM":
		var total float64
		for _, row := range rows {
			if v, ok := row[agg.Column]; ok {
				total += toFloat64(v)
			}
		}
		return total

	case "AVG":
		var sum float64
		for _, row := range rows {
			if v, ok := row[agg.Column]; ok {
				sum += toFloat64(v)
			}
		}
		return sum / float64(len(rows))

	case "MAX":
		var maxVal interface{}
		for i, row := range rows {
			if v, ok := row[agg.Column]; ok {
				if i == 0 {
					maxVal = v
				} else if compareValues(v, maxVal) > 0 {
					maxVal = v
				}
			}
		}
		return maxVal

	case "MIN":
		var minVal interface{}
		for i, row := range rows {
			if v, ok := row[agg.Column]; ok {
				if i == 0 {
					minVal = v
				} else if compareValues(v, minVal) < 0 {
					minVal = v
				}
			}
		}
		return minVal
	}

	return nil
}

// estimateTotalRows 估算总行数
func (m *ResultMerger) estimateTotalRows(results []*ShardResult) int {
	total := 0
	for _, r := range results {
		total += len(r.Rows)
	}
	return total
}

// HeapItem 堆元素
type HeapItem struct {
	Row        map[string]interface{}
	ShardIndex int
	RowIndex   int
}

// RowHeap 行堆
type RowHeap struct {
	items   []HeapItem
	orderBy []OrderByClause
}

// NewRowHeap 创建行堆
func NewRowHeap(orderBy []OrderByClause) *RowHeap {
	return &RowHeap{
		items:   make([]HeapItem, 0),
		orderBy: orderBy,
	}
}

func (h *RowHeap) Len() int {
	return len(h.items)
}

func (h *RowHeap) Less(i, j int) bool {
	for _, clause := range h.orderBy {
		vi, oki := h.items[i].Row[clause.Column]
		vj, okj := h.items[j].Row[clause.Column]

		if !oki || !okj {
			continue
		}

		cmp := compareValues(vi, vj)
		if cmp == 0 {
			continue
		}

		if clause.Desc {
			return cmp > 0
		}
		return cmp < 0
	}

	return false
}

func (h *RowHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *RowHeap) Push(x interface{}) {
	h.items = append(h.items, x.(HeapItem))
}

func (h *RowHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	return item
}

// 辅助函数

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int64:
		return val
	case int32:
		return int64(val)
	case float64:
		return int64(val)
	case float32:
		return int64(val)
	case string:
		i, _ := strconv.ParseInt(val, 10, 64)
		return i
	case []byte:
		i, _ := strconv.ParseInt(string(val), 10, 64)
		return i
	}
	return 0
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case int32:
		return float64(val)
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	case []byte:
		f, _ := strconv.ParseFloat(string(val), 64)
		return f
	}
	return 0
}

func compareValues(a, b interface{}) int {
	// 处理 nil
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// 类型转换后比较
	switch a.(type) {
	case int, int64, int32, float64, float32:
		af := toFloat64(a)
		bf := toFloat64(b)
		if af < bf {
			return -1
		} else if af > bf {
			return 1
		}
		return 0

	case string:
		as, _ := a.(string)
		bs, _ := b.(string)
		return strings.Compare(as, bs)

	case []byte:
		as := string(a.([]byte))
		bs := string(b.([]byte))
		return strings.Compare(as, bs)

	case time.Time:
		at, _ := a.(time.Time)
		bt, _ := b.(time.Time)
		if at.Before(bt) {
			return -1
		} else if at.After(bt) {
			return 1
		}
		return 0
	}

	return 0
}
