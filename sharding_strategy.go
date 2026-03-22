package dbwarp

import (
	"fmt"
	"time"
)

// ShardingStrategy defines the interface for sharding strategies
// All strategies must implement this interface to be used with ShardingRule
type ShardingStrategy interface {
	// GetSuffix returns the table/database suffix based on the sharding key value
	// This matches the signature: func(columnValue any) (suffix string, err error)
	GetSuffix(columnValue any) (string, error)
}

// EnumShardingStrategy maps enum values to table suffixes
// Example: map[string]string{"beijing": "_bj", "shanghai": "_sh"}
type EnumShardingStrategy struct {
	// EnumMap maps enum values to suffixes
	EnumMap map[string]string
	// DefaultSuffix is used when value not in map
	DefaultSuffix string
}

func (s *EnumShardingStrategy) GetSuffix(columnValue any) (string, error) {
	str, ok := columnValue.(string)
	if !ok {
		return "", fmt.Errorf("enum sharding requires string value, got %T", columnValue)
	}

	if suffix, exists := s.EnumMap[str]; exists {
		return suffix, nil
	}

	if s.DefaultSuffix != "" {
		return s.DefaultSuffix, nil
	}

	return "", fmt.Errorf("value '%s' not found in enum map and no default suffix defined", str)
}

// DateShardingStrategy shards tables by date
type DateShardingStrategy struct {
	// Format is the date format for suffix (e.g., "20060102" for daily, "200601" for monthly)
	Format string
	// ValueFormat is the expected input format (default: time.RFC3339)
	ValueFormat string
}

func (s *DateShardingStrategy) GetSuffix(value any) (string, error) {
	var t time.Time
	var err error

	switch v := value.(type) {
	case time.Time:
		t = v
	case string:
		valueFormat := s.ValueFormat
		if valueFormat == "" {
			valueFormat = time.RFC3339
		}
		t, err = time.Parse(valueFormat, v)
		if err != nil {
			return "", fmt.Errorf("failed to parse date string '%s': %w", v, err)
		}
	case int64:
		t = time.Unix(v, 0)
	case int:
		t = time.Unix(int64(v), 0)
	default:
		return "", fmt.Errorf("date sharding requires time.Time/string/int64 value, got %T", value)
	}

	format := s.Format
	if format == "" {
		format = "20060102" // Default to daily
	}

	return "_" + t.Format(format), nil
}

// RangeShardingStrategy shards tables by numeric ranges
type RangeShardingStrategy struct {
	// Ranges defines the boundaries, e.g., []int64{0, 1000, 5000, 10000}
	// Values from 0-999 -> suffix "_0"
	// Values from 1000-4999 -> suffix "_1"
	// Values from 5000-9999 -> suffix "_2"
	Ranges []int64
	// SuffixFunc generates suffix from range index (default: "_index")
	SuffixFunc func(index int) string
}

func (s *RangeShardingStrategy) GetSuffix(value any) (string, error) {
	var num int64
	switch v := value.(type) {
	case int:
		num = int64(v)
	case int32:
		num = int64(v)
	case int64:
		num = v
	case uint:
		num = int64(v)
	case uint32:
		num = int64(v)
	case uint64:
		num = int64(v)
	case float64:
		num = int64(v)
	case float32:
		num = int64(v)
	default:
		return "", fmt.Errorf("range sharding requires numeric value, got %T", value)
	}

	// Find the appropriate range
	for i := 0; i < len(s.Ranges)-1; i++ {
		if num >= s.Ranges[i] && num < s.Ranges[i+1] {
			return s.getSuffix(i), nil
		}
	}

	// Value is beyond the last range boundary
	if num >= s.Ranges[len(s.Ranges)-1] {
		return s.getSuffix(len(s.Ranges) - 1), nil
	}

	// Value is below the first range boundary
	return s.getSuffix(0), nil
}

func (s *RangeShardingStrategy) getSuffix(index int) string {
	if s.SuffixFunc != nil {
		return s.SuffixFunc(index)
	}
	return fmt.Sprintf("_%d", index)
}

// HashShardingStrategy shards by hash modulo (existing behavior)
type HashShardingStrategy struct {
	Shards int
}

func (s *HashShardingStrategy) GetSuffix(value any) (string, error) {
	var num int64
	switch v := value.(type) {
	case int:
		num = int64(v)
	case int32:
		num = int64(v)
	case int64:
		num = v
	case uint:
		num = int64(v)
	case uint32:
		num = int64(v)
	case uint64:
		num = int64(v)
	case string:
		// Hash string to int
		hash := int64(0)
		for _, c := range v {
			hash = hash*31 + int64(c)
		}
		num = hash
	default:
		return "", fmt.Errorf("hash sharding requires numeric or string value, got %T", value)
	}

	if s.Shards <= 0 {
		return "", fmt.Errorf("hash sharding requires positive shards count")
	}

	index := num % int64(s.Shards)
	if index < 0 {
		index = -index
	}

	return fmt.Sprintf("_%d", index), nil
}
