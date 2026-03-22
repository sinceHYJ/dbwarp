package cross_shard

import (
	"errors"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 跨分片查询完整配置
type Config struct {
	// 跨分片查询配置
	CrossShard CrossShardConfig `yaml:"cross_shard" json:"cross_shard"`

	// 连接池配置
	Pool PoolConfig `yaml:"pool" json:"pool"`
}

// CrossShardConfig 跨分片查询配置
type CrossShardConfig struct {
	// 开关
	Enabled bool `yaml:"enabled" json:"enabled"`

	// 并发控制
	MaxConcurrency int `yaml:"max_concurrency" json:"max_concurrency"`

	// 超时设置
	ShardTimeout time.Duration `yaml:"shard_timeout" json:"shard_timeout"`
	TotalTimeout time.Duration `yaml:"total_timeout" json:"total_timeout"`

	// 安全限制
	AllowFullScan     bool `yaml:"allow_full_scan" json:"allow_full_scan"`
	MaxShardScanCount int  `yaml:"max_shard_scan_count" json:"max_shard_scan_count"`
	MaxResultRows     int  `yaml:"max_result_rows" json:"max_result_rows"`

	// 重试策略
	RetryCount int           `yaml:"retry_count" json:"retry_count"`
	RetryDelay time.Duration `yaml:"retry_delay" json:"retry_delay"`
	FailFast   bool          `yaml:"fail_fast" json:"fail_fast"`

	// 优化选项
	SmartPagination bool `yaml:"smart_pagination" json:"smart_pagination"`

	// 慢查询
	SlowQueryThreshold time.Duration `yaml:"slow_query_threshold" json:"slow_query_threshold"`
}

// PoolConfig 连接池配置
type PoolConfig struct {
	MaxOpenConns    int           `yaml:"max_open_conns" json:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns" json:"max_idle_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime" json:"conn_max_lifetime"`
	ConnMaxIdleTime time.Duration `yaml:"conn_max_idle_time" json:"conn_max_idle_time"`
}

// DefaultConfig 默认配置
func DefaultConfig() Config {
	return Config{
		CrossShard: CrossShardConfig{
			Enabled:            true,
			MaxConcurrency:     8,
			ShardTimeout:       5 * time.Second,
			TotalTimeout:       30 * time.Second,
			AllowFullScan:      true,
			MaxShardScanCount:  100,
			MaxResultRows:      10000,
			RetryCount:         2,
			RetryDelay:         100 * time.Millisecond,
			FailFast:           false,
			SmartPagination:    true,
			SlowQueryThreshold: 1 * time.Second,
		},
		Pool: PoolConfig{
			MaxOpenConns:    100,
			MaxIdleConns:    20,
			ConnMaxLifetime: 30 * time.Minute,
			ConnMaxIdleTime: 10 * time.Minute,
		},
	}
}

// LoadConfig 从文件加载配置
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	config := DefaultConfig()

	// 根据文件扩展名解析
	if strings.HasSuffix(path, ".yaml") || strings.HasSuffix(path, ".yml") {
		err = yaml.Unmarshal(data, &config)
	} else {
		err = errors.New("unsupported config file format, use yaml")
	}

	if err != nil {
		return nil, err
	}

	return &config, nil
}

// Validate 验证配置
func (c *Config) Validate() error {
	if c.CrossShard.MaxConcurrency <= 0 {
		return errors.New("max_concurrency must be positive")
	}

	if c.CrossShard.MaxResultRows <= 0 {
		return errors.New("max_result_rows must be positive")
	}

	return nil
}
