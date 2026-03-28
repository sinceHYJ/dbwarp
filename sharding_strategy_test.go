package dbwarp

import (
	"fmt"
	"testing"
	"time"
)

func TestEnumShardingStrategy(t *testing.T) {
	strategy := &EnumShardingStrategy{
		EnumMap: map[string]string{
			"beijing":  "_bj",
			"shanghai": "_sh",
			"guangzhou": "_gz",
		},
		DefaultSuffix: "_other",
	}

	tests := []struct {
		value    any
		expected string
		hasError bool
	}{
		{"beijing", "_bj", false},
		{"shanghai", "_sh", false},
		{"guangzhou", "_gz", false},
		{"shenzhen", "_other", false},
		{123, "", true}, // should error
	}

	for _, tt := range tests {
		suffix, err := strategy.GetSuffix(tt.value)
		if tt.hasError {
			if err == nil {
				t.Errorf("expected error for value %v, got nil", tt.value)
			}
		} else {
			if err != nil {
				t.Errorf("unexpected error for value %v: %v", tt.value, err)
			}
			if suffix != tt.expected {
				t.Errorf("expected suffix %s, got %s", tt.expected, suffix)
			}
		}
	}
}

func TestDateShardingStrategy(t *testing.T) {
	// Daily sharding
	dailyStrategy := &DateShardingStrategy{
		Format: "20060102",
	}

	// Monthly sharding
	monthlyStrategy := &DateShardingStrategy{
		Format: "200601",
	}

	// Test with time.Time
	t1 := time.Date(2026, 3, 22, 10, 0, 0, 0, time.Local)
	suffix, err := dailyStrategy.GetSuffix(t1)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if suffix != "_20260322" {
		t.Errorf("expected suffix _20260322, got %s", suffix)
	}

	// Test monthly
	suffix, err = monthlyStrategy.GetSuffix(t1)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if suffix != "_202603" {
		t.Errorf("expected suffix _202603, got %s", suffix)
	}

	// Test with string
	suffix, err = dailyStrategy.GetSuffix("2026-03-22T10:00:00Z")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if suffix != "_20260322" {
		t.Errorf("expected suffix _20260322, got %s", suffix)
	}

	// Test with timestamp
	suffix, err = dailyStrategy.GetSuffix(int64(1711094400))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	// Just check it doesn't error
	if suffix == "" {
		t.Errorf("expected non-empty suffix")
	}
}

func TestRangeShardingStrategy(t *testing.T) {
	strategy := &RangeShardingStrategy{
		Ranges: []int64{0, 1000, 5000, 10000},
	}

	tests := []struct {
		value    any
		expected string
	}{
		{0, "_0"},
		{999, "_0"},
		{1000, "_1"},
		{4999, "_1"},
		{5000, "_2"},
		{9999, "_2"},
		{10000, "_3"},
		{15000, "_3"},
		{-100, "_0"}, // below first range
	}

	for _, tt := range tests {
		suffix, err := strategy.GetSuffix(tt.value)
		if err != nil {
			t.Errorf("unexpected error for value %v: %v", tt.value, err)
			continue
		}
		if suffix != tt.expected {
			t.Errorf("value %v: expected suffix %s, got %s", tt.value, tt.expected, suffix)
		}
	}
}

func TestRangeShardingStrategyWithCustomSuffix(t *testing.T) {
	strategy := &RangeShardingStrategy{
		Ranges: []int64{0, 100, 500},
		SuffixFunc: func(index int) string {
			return fmt.Sprintf("_range_%d", index)
		},
	}

	suffix, err := strategy.GetSuffix(150)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if suffix != "_range_1" {
		t.Errorf("expected suffix _range_1, got %s", suffix)
	}
}

func TestHashShardingStrategy(t *testing.T) {
	strategy := &HashShardingStrategy{
		Shards: 10,
	}

	// Test with int
	suffix, err := strategy.GetSuffix(123)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if suffix != "_3" {
		t.Errorf("expected suffix _3, got %s", suffix)
	}

	// Test with string
	suffix, err = strategy.GetSuffix("user123")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	// Just verify it's a valid suffix format
	if len(suffix) < 2 {
		t.Errorf("invalid suffix format: %s", suffix)
	}

	// Test with negative number
	suffix, err = strategy.GetSuffix(-123)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if suffix != "_3" {
		t.Errorf("expected suffix _3 for negative, got %s", suffix)
	}
}
