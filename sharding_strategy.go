package dbwarp

import (
	"errors"
	"hash/crc32"
	"sort"
	"strconv"
	"time"
)

// ShardingStrategy defines the interface for sharding strategies.
type ShardingStrategy interface {
	GetSuffix(columnValue any) (string, error)
}

// EnumShardingStrategy maps enum values to table suffixes.
type EnumShardingStrategy struct {
	EnumMap       map[string]string
	DefaultSuffix string
}

func (s *EnumShardingStrategy) GetSuffix(columnValue any) (string, error) {
	str, ok := columnValue.(string)
	if !ok {
		return "", errors.New("enum sharding requires string value")
	}

	if suffix, exists := s.EnumMap[str]; exists {
		return suffix, nil
	}

	if s.DefaultSuffix != "" {
		return s.DefaultSuffix, nil
	}

	return "", errors.New("value not found in enum map and no default suffix defined")
}

// DateShardingStrategy shards tables by date.
type DateShardingStrategy struct {
	Format      string
	ValueFormat string
}

// GetSuffix returns the table suffix based on the date value.
// Supported value types:
//   - time.Time: directly used
//   - string: parsed using ValueFormat (default: time.RFC3339)
//   - int64/int: treated as Unix timestamp (seconds)
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
			return "", err
		}
	case int64:
		t = time.Unix(v, 0)
	case int:
		t = time.Unix(int64(v), 0)
	default:
		return "", errors.New("date sharding requires time.Time/string/int64 value")
	}

	format := s.Format
	if format == "" {
		format = "20060102"
	}

	return "_" + t.Format(format), nil
}

// RangeShardingStrategy shards tables by numeric ranges.
type RangeShardingStrategy struct {
	Ranges     []int64
	SuffixFunc func(index int) string
}

func (s *RangeShardingStrategy) GetSuffix(value any) (string, error) {
	if len(s.Ranges) == 0 {
		return "", errors.New("ranges must not be empty")
	}

	num, err := toInt64(value)
	if err != nil {
		return "", errors.New("range sharding requires numeric value")
	}

	// Binary search for range index
	idx := sort.Search(len(s.Ranges), func(i int) bool {
		return s.Ranges[i] > num
	})

	// idx is the position where num would be inserted
	// Convert to range index: values in [Ranges[i-1], Ranges[i]) belong to range i-1
	var rangeIdx int
	if idx == 0 {
		rangeIdx = 0
	} else if idx >= len(s.Ranges) {
		rangeIdx = len(s.Ranges) - 1
	} else {
		rangeIdx = idx - 1
	}

	return s.getSuffix(rangeIdx), nil
}

func (s *RangeShardingStrategy) getSuffix(index int) string {
	if s.SuffixFunc != nil {
		return s.SuffixFunc(index)
	}
	return "_" + strconv.FormatInt(int64(index), 10)
}

// HashShardingStrategy shards by hash modulo.
type HashShardingStrategy struct {
	Shards int
}

func (s *HashShardingStrategy) GetSuffix(value any) (string, error) {
	if s.Shards <= 0 {
		return "", errors.New("hash sharding requires positive shards count")
	}

	var num int64
	switch v := value.(type) {
	case string:
		// Use CRC32 for consistent hashing with existing code
		num = int64(crc32.ChecksumIEEE([]byte(v)))
	default:
		var err error
		num, err = toInt64(v)
		if err != nil {
			return "", errors.New("hash sharding requires numeric or string value")
		}
	}

	index := num % int64(s.Shards)
	if index < 0 {
		index = -index
	}

	return "_" + strconv.FormatInt(index, 10), nil
}

// toInt64 converts various numeric types to int64.
func toInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	default:
		return 0, errors.New("cannot convert to int64")
	}
}
