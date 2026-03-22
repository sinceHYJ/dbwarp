# 分片策略扩展

dbwarp 支持多种分片策略，可以根据业务需求选择合适的分片方式。

## 内置分片策略

### 1. 枚举分片 (Enum Sharding)

**适用场景**：按地区、类型等固定枚举值分片

```go
strategy := &dbwarp.EnumShardingStrategy{
    EnumMap: map[string]string{
        "beijing":   "_bj",
        "shanghai":  "_sh",
        "guangzhou": "_gz",
    },
    DefaultSuffix: "_other",
}
```

**示例**：
- 分片键值为 "beijing" → 表名后缀 "_bj"
- 分片键值为 "shanghai" → 表名后缀 "_sh"
- 分片键值为 "hangzhou" → 表名后缀 "_other" (使用默认值)

### 2. 日期分片 (Date Sharding)

**适用场景**：按时间分表，适合日志、订单、流水等按时间增长的数据

```go
strategy := &dbwarp.DateShardingStrategy{
    Format: "200601", // 按月分表
    // Format: "20060102", // 按日分表
    // Format: "2006", // 按年分表
    ValueFormat: time.RFC3339, // 输入时间格式
}
```

**示例**：
- Format="200601", 值为 2026-03-22 → 后缀 "_202603"
- Format="20060102", 值为 2026-03-22 → 后缀 "_20260322"
- Format="2006", 值为 2026-03-22 → 后缀 "_2026"

### 3. 范围分片 (Range Sharding)

**适用场景**：按数值范围分表，适合按ID、金额等数值型数据分片

```go
strategy := &dbwarp.RangeShardingStrategy{
    Ranges: []int64{0, 10000, 50000, 100000},
    SuffixFunc: func(index int) string {
        return fmt.Sprintf("_range%d", index)
    },
}
```

**示例**（Ranges: [0, 10000, 50000, 100000]）：
- 值为 5000 → 后缀 "_range0" (在 [0, 10000) 区间)
- 值为 15000 → 后缀 "_range1" (在 [10000, 50000) 区间)
- 值为 60000 → 后缀 "_range2" (在 [50000, 100000) 区间)
- 值为 150000 → 后缀 "_range3" (>= 100000)

### 4. 哈希分片 (Hash Sharding)

**适用场景**：均匀分布数据，最常用的分片方式

```go
strategy := &dbwarp.HashShardingStrategy{
    Shards: 10, // 分片数量
}
```

**示例**（Shards: 10）：
- 值为 123 → 后缀 "_3" (123 % 10 = 3)
- 值为 456 → 后缀 "_6" (456 % 10 = 6)
- 字符串 "user_abc" → 后缀 "_X" (字符串哈希后取模)

## 使用示例

### 基本用法

```go
// 1. 枚举分片 - 订单表按地区
orderWarp := dbwarp.New().AddRule(&dbwarp.ShardingRule{
    ShardingKey: "region",
    ShardingStrategy: &dbwarp.EnumShardingStrategy{
        EnumMap: map[string]string{
            "beijing":  "_bj",
            "shanghai": "_sh",
        },
    },
}, Order{})

// 2. 日期分片 - 日志表按月
logWarp := dbwarp.New().AddRule(&dbwarp.ShardingRule{
    ShardingKey: "created_at",
    ShardingStrategy: &dbwarp.DateShardingStrategy{
        Format: "200601",
    },
}, LogRecord{})

// 3. 范围分片 - 用户表按ID
userWarp := dbwarp.New().AddRule(&dbwarp.ShardingRule{
    ShardingKey: "user_id",
    ShardingStrategy: &dbwarp.RangeShardingStrategy{
        Ranges: []int64{0, 10000, 50000, 100000},
    },
}, User{})
```

### 数据库分片 + 表分片

```go
// 数据库按用户ID哈希分4个库，每个库内按日期分表
rule := &dbwarp.ShardingRule{
    ShardingKey: "user_id",
    
    // 数据库分片：按哈希
    DatabaseNumberOfShards: 4,
    ShardingDatabaseStrategy: &dbwarp.HashShardingStrategy{
        Shards: 4,
    },
    
    // 表分片：按日期
    TableNumberOfShards: 12, // 12个月
    ShardingTableStrategy: &dbwarp.DateShardingStrategy{
        Format: "200601",
    },
    
    RouterCfgs: routerCfgs,
}

warp := dbwarp.New().AddRule(rule, Order{})
```

### 读写分离 + 分片

```go
rule := &dbwarp.ShardingRule{
    ShardingKey: "user_id",
    
    // 分片策略
    DatabaseNumberOfShards: 4,
    ShardingDatabaseStrategy: &dbwarp.HashShardingStrategy{
        Shards: 4,
    },
    
    // 读写分离配置
    RouterCfgs: []dbwarp.RouterCfg{
        {
            Name:     "shard_0",
            Sources:  []gorm.Dialector{mysql.Open("shard0_master_dsn")},
            Replicas: []gorm.Dialector{
                mysql.Open("shard0_replica1_dsn"),
                mysql.Open("shard0_replica2_dsn"),
            },
            ReplPolicy: dbwarp.RoundRobinPolicy{},
        },
        // ... 其他分片配置
    },
}

warp := dbwarp.New().AddRule(rule, User{})
```

## 自定义分片策略

您可以实现 `ShardingStrategy` 接口来创建自定义分片策略：

```go
type CustomShardingStrategy struct{}

func (s *CustomShardingStrategy) GetSuffix(value any) (string, error) {
    // 实现您的分片逻辑
    return "_custom", nil
}

func (s *CustomShardingStrategy) GetName() string {
    return "custom"
}

// 使用自定义策略
warp := dbwarp.New().AddRule(&dbwarp.ShardingRule{
    ShardingKey: "custom_key",
    ShardingStrategy: &CustomShardingStrategy{},
}, CustomTable{})
```

## 性能建议

1. **分片数量选择**：
   - 哈希分片：建议分片数为 2 的幂次方（8, 16, 32, 64...）
   - 范围分片：根据数据量合理划分范围边界
   - 日期分片：考虑数据增长速度和查询模式

2. **分片键选择**：
   - 选择区分度高的字段
   - 避免数据倾斜
   - 考虑查询场景（WHERE 条件中经常使用的字段）

3. **跨分片查询**：
   - 避免跨分片 JOIN
   - 使用 IN 子句时确保所有值在同一分片
   - 考虑使用冗余数据或聚合表

## 运行示例

```bash
cd examples/sharding-strategies
go run main.go
```

## 测试

```bash
go test -v ./... -run Strategy
```
