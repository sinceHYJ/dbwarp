package dbwarp

import (
	"strconv"
	"strings"
	"testing"

	"github.com/longbridgeapp/sqlparser"
)

// Helper function to convert any to int64
func toInt64(v any) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int64:
		return val
	case string:
		i, _ := strconv.ParseInt(val, 10, 64)
		return i
	default:
		return 0
	}
}

// Sharding algorithm for tests: user_id % shards
func createShardingAlgorithm(shards int) func(any) (string, error) {
	return func(columnValue any) (string, error) {
		v := toInt64(columnValue)
		return "_" + strconv.FormatInt(v%int64(shards), 10), nil
	}
}

func TestNonInsertValue_IN_SameShard(t *testing.T) {
	// Setup: 10 shards based on user_id % 10
	rule := &ShardingRule{
		ShardingKey:            "user_id",
		TableNumberOfShards:    10,
		ShardingTableAlgorithm: createShardingAlgorithm(10),
	}

	item := &WarpItem{rule: rule}

	// SQL: WHERE user_id IN (?, ?, ?)
	// Values: 1, 11, 21 -> all have suffix _1 (1%10=1, 11%10=1, 21%10=1)
	query := "SELECT * FROM order_info WHERE user_id IN (?, ?, ?)"
	expr, err := sqlparser.NewParser(strings.NewReader(query)).ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	stmt, ok := expr.(*sqlparser.SelectStatement)
	if !ok {
		t.Fatalf("Expected SelectStatement, got %T", expr)
	}

	// Args: 1, 11, 21 - all map to shard _1
	values, _, keyFound, err := item.nonInsertValue("user_id", stmt.Condition, 1, 11, 21)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if !keyFound {
		t.Fatal("Expected keyFound to be true")
	}
	if len(values) != 3 {
		t.Errorf("Expected 3 values, got: %d", len(values))
	}
	if values[0] != 1 {
		t.Errorf("Expected first value to be 1, got: %v", values[0])
	}
}

func TestNonInsertValue_IN_CrossShard(t *testing.T) {
	// Setup: 10 shards based on user_id % 10
	rule := &ShardingRule{
		ShardingKey:            "user_id",
		TableNumberOfShards:    10,
		ShardingTableAlgorithm: createShardingAlgorithm(10),
	}

	item := &WarpItem{rule: rule}

	// SQL: WHERE user_id IN (?, ?, ?)
	query := "SELECT * FROM order_info WHERE user_id IN (?, ?, ?)"
	expr, err := sqlparser.NewParser(strings.NewReader(query)).ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	stmt, ok := expr.(*sqlparser.SelectStatement)
	if !ok {
		t.Fatalf("Expected SelectStatement, got %T", expr)
	}

	// Args: 1, 2, 3 -> map to different shards (_1, _2, _3)
	// nonInsertValue should still return values, validation is in resolve()
	values, _, keyFound, err := item.nonInsertValue("user_id", stmt.Condition, 1, 2, 3)
	if err != nil {
		t.Fatalf("Expected no error from nonInsertValue, got: %v", err)
	}
	if !keyFound {
		t.Fatal("Expected keyFound to be true")
	}
	if len(values) != 3 {
		t.Errorf("Expected 3 values, got: %d", len(values))
	}
}

func TestNonInsertValue_IN_LiteralValues(t *testing.T) {
	// Setup: 10 shards based on user_id % 10
	rule := &ShardingRule{
		ShardingKey:            "user_id",
		TableNumberOfShards:    10,
		ShardingTableAlgorithm: createShardingAlgorithm(10),
	}

	item := &WarpItem{rule: rule}

	// SQL with literal values: WHERE user_id IN (1, 11, 21)
	query := "SELECT * FROM order_info WHERE user_id IN (1, 11, 21)"
	expr, err := sqlparser.NewParser(strings.NewReader(query)).ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	stmt, ok := expr.(*sqlparser.SelectStatement)
	if !ok {
		t.Fatalf("Expected SelectStatement, got %T", expr)
	}

	// No args needed for literal values
	values, _, keyFound, err := item.nonInsertValue("user_id", stmt.Condition)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if !keyFound {
		t.Fatal("Expected keyFound to be true")
	}
	if len(values) != 3 {
		t.Errorf("Expected 3 values, got: %d", len(values))
	}
	if values[0] != "1" {
		t.Errorf("Expected first value to be '1' (string), got: %v", values[0])
	}
}

func TestNonInsertValue_IN_SingleValue(t *testing.T) {
	// Setup: 10 shards based on user_id % 10
	rule := &ShardingRule{
		ShardingKey:            "user_id",
		TableNumberOfShards:    10,
		ShardingTableAlgorithm: createShardingAlgorithm(10),
	}

	item := &WarpItem{rule: rule}

	// SQL with single value: WHERE user_id IN (?)
	query := "SELECT * FROM order_info WHERE user_id IN (?)"
	expr, err := sqlparser.NewParser(strings.NewReader(query)).ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	stmt, ok := expr.(*sqlparser.SelectStatement)
	if !ok {
		t.Fatalf("Expected SelectStatement, got %T", expr)
	}

	values, _, keyFound, err := item.nonInsertValue("user_id", stmt.Condition, 5)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if !keyFound {
		t.Fatal("Expected keyFound to be true")
	}
	if len(values) != 1 {
		t.Errorf("Expected 1 value, got: %d", len(values))
	}
	if values[0] != 5 {
		t.Errorf("Expected value to be 5, got: %v", values[0])
	}
}

func TestNonInsertValue_IN_MixedShardBoundary(t *testing.T) {
	// Setup: 4 shards (0-3) based on user_id % 4
	rule := &ShardingRule{
		ShardingKey:            "user_id",
		TableNumberOfShards:    4,
		ShardingTableAlgorithm: createShardingAlgorithm(4),
	}

	item := &WarpItem{rule: rule}

	// SQL: WHERE user_id IN (?, ?, ?)
	query := "SELECT * FROM order_info WHERE user_id IN (?, ?, ?)"
	expr, err := sqlparser.NewParser(strings.NewReader(query)).ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	stmt, ok := expr.(*sqlparser.SelectStatement)
	if !ok {
		t.Fatalf("Expected SelectStatement, got %T", expr)
	}

	// Test: 4, 8, 12 -> all % 4 == 0, same shard
	values, _, keyFound, err := item.nonInsertValue("user_id", stmt.Condition, 4, 8, 12)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if !keyFound {
		t.Fatal("Expected keyFound to be true")
	}
	if len(values) != 3 {
		t.Errorf("Expected 3 values, got: %d", len(values))
	}

	// Test: 4, 5, 6 -> different shards (validation happens in resolve, not here)
	values, _, keyFound, err = item.nonInsertValue("user_id", stmt.Condition, 4, 5, 6)
	if err != nil {
		t.Fatalf("Expected no error from nonInsertValue, got: %v", err)
	}
	if !keyFound {
		t.Fatal("Expected keyFound to be true")
	}
	if len(values) != 3 {
		t.Errorf("Expected 3 values, got: %d", len(values))
	}
}

func TestNonInsertValue_EqualOperator(t *testing.T) {
	// Ensure = operator still works and returns single value in slice
	rule := &ShardingRule{
		ShardingKey:            "user_id",
		TableNumberOfShards:    10,
		ShardingTableAlgorithm: createShardingAlgorithm(10),
	}

	item := &WarpItem{rule: rule}

	query := "SELECT * FROM order_info WHERE user_id = ?"
	expr, err := sqlparser.NewParser(strings.NewReader(query)).ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	stmt, ok := expr.(*sqlparser.SelectStatement)
	if !ok {
		t.Fatalf("Expected SelectStatement, got %T", expr)
	}

	values, _, keyFound, err := item.nonInsertValue("user_id", stmt.Condition, 5)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if !keyFound {
		t.Fatal("Expected keyFound to be true")
	}
	if len(values) != 1 {
		t.Errorf("Expected 1 value, got: %d", len(values))
	}
	if values[0] != 5 {
		t.Errorf("Expected value to be 5, got: %v", values[0])
	}
}

func TestResolve_IN_SameShard(t *testing.T) {
	// Integration test: test resolve function with IN clause
	rule := &ShardingRule{
		ShardingKey:            "user_id",
		TableNumberOfShards:    10,
		ShardingTableAlgorithm: createShardingAlgorithm(10),
	}

	item := &WarpItem{rule: rule}

	// All values map to shard _1
	query := "SELECT * FROM order_info WHERE user_id IN (?, ?, ?)"
	ftQuery, stQuery, tableName, dbIndex, err := item.resolve(query, 1, 11, 21)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if ftQuery != query {
		t.Errorf("Expected ftQuery to be original query, got: %s", ftQuery)
	}
	if tableName != "order_info" {
		t.Errorf("Expected tableName to be 'order_info', got: %s", tableName)
	}
	if dbIndex != -1 {
		t.Errorf("Expected dbIndex to be -1 (no db sharding), got: %d", dbIndex)
	}
	if !strings.Contains(stQuery, "order_info_1") {
		t.Errorf("Expected stQuery to contain 'order_info_1', got: %s", stQuery)
	}
}

func TestResolve_IN_CrossShard(t *testing.T) {
	// Integration test: test resolve function with cross-shard IN clause
	rule := &ShardingRule{
		ShardingKey:            "user_id",
		TableNumberOfShards:    10,
		ShardingTableAlgorithm: createShardingAlgorithm(10),
	}

	item := &WarpItem{rule: rule}

	// Values map to different shards
	query := "SELECT * FROM order_info WHERE user_id IN (?, ?, ?)"
	_, _, _, _, err := item.resolve(query, 1, 2, 3)
	if err != ErrInClauseCrossShard {
		t.Errorf("Expected ErrInClauseCrossShard, got: %v", err)
	}
}

func TestResolve_EqualOperator(t *testing.T) {
	// Integration test: test resolve function with = operator
	rule := &ShardingRule{
		ShardingKey:            "user_id",
		TableNumberOfShards:    10,
		ShardingTableAlgorithm: createShardingAlgorithm(10),
	}

	item := &WarpItem{rule: rule}

	query := "SELECT * FROM order_info WHERE user_id = ?"
	ftQuery, stQuery, tableName, dbIndex, err := item.resolve(query, 5)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if ftQuery != query {
		t.Errorf("Expected ftQuery to be original query, got: %s", ftQuery)
	}
	if tableName != "order_info" {
		t.Errorf("Expected tableName to be 'order_info', got: %s", tableName)
	}
	if dbIndex != -1 {
		t.Errorf("Expected dbIndex to be -1 (no db sharding), got: %d", dbIndex)
	}
	if !strings.Contains(stQuery, "order_info_5") {
		t.Errorf("Expected stQuery to contain 'order_info_5', got: %s", stQuery)
	}
}
