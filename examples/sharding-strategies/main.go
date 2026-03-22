package main

import (
	"fmt"
	"time"

	"github.com/sincehyj/dbwarp"
)

func main() {
	fmt.Println("=== 分片策略示例 ===")

	// 1. 枚举分片策略 - 按地区分表
	fmt.Println("\n1. 枚举分片策略（按地区分表）")
	enumStrategy := &dbwarp.EnumShardingStrategy{
		EnumMap: map[string]string{
			"beijing":   "_bj",
			"shanghai":  "_sh",
			"guangzhou": "_gz",
			"shenzhen":  "_sz",
		},
		DefaultSuffix: "_other",
	}

	cities := []string{"beijing", "shanghai", "hangzhou"}
	for _, city := range cities {
		suffix, _ := enumStrategy.GetSuffix(city)
		fmt.Printf("  城市: %s -> 表后缀: %s\n", city, suffix)
	}

	// 2. 日期分片策略 - 按月分表
	fmt.Println("\n2. 日期分片策略（按月分表）")
	dateStrategy := &dbwarp.DateShardingStrategy{
		Format: "200601", // 按月
	}

	dates := []string{
		"2026-01-15T10:00:00Z",
		"2026-02-20T10:00:00Z",
		"2026-03-22T10:00:00Z",
	}
	for _, dateStr := range dates {
		suffix, _ := dateStrategy.GetSuffix(dateStr)
		t, _ := time.Parse(time.RFC3339, dateStr)
		fmt.Printf("  日期: %s -> 表后缀: %s\n", t.Format("2006-01-02"), suffix)
	}

	// 3. 范围分片策略 - 按ID范围分表
	fmt.Println("\n3. 范围分片策略（按ID范围分表）")
	rangeStrategy := &dbwarp.RangeShardingStrategy{
		Ranges: []int64{0, 10000, 50000, 100000},
		SuffixFunc: func(index int) string {
				return fmt.Sprintf("_range%d", index)
			},
		}

	userIDs := []int64{500, 15000, 60000, 150000}
	for _, userID := range userIDs {
		suffix, _ := rangeStrategy.GetSuffix(userID)
		fmt.Printf("  用户ID: %d -> 表后缀: %s\n", userID, suffix)
	}

	// 4. 哈希分片策略 - 按哈希取模分表
	fmt.Println("\n4. 哈希分片策略（按哈希取模分表）")
	hashStrategy := &dbwarp.HashShardingStrategy{
		Shards: 8,
	}

	keys := []interface{}{12345, "user_abc", 67890, "user_xyz"}
	for _, key := range keys {
		suffix, _ := hashStrategy.GetSuffix(key)
		fmt.Printf("  分片键: %v -> 表后缀: %s\n", key, suffix)
	}

	// 5. 实际使用示例 - GORM 集成
	fmt.Println("\n5. GORM 集成示例代码:")
	fmt.Println(`
// 枚举分片 - 订单表按地区分片
orderWarp := dbwarp.New().AddRule(&dbwarp.ShardingRule{
    ShardingKey: "region",
    ShardingStrategy: &dbwarp.EnumShardingStrategy{
        EnumMap: map[string]string{
            "beijing":   "_bj",
            "shanghai":  "_sh",
        },
    },
}, Order{})

// 日期分片 - 日志表按月分片
logWarp := dbwarp.New().AddRule(&dbwarp.ShardingRule{
    ShardingKey: "created_at",
    ShardingStrategy: &dbwarp.DateShardingStrategy{
        Format: "200601",
    },
}, LogRecord{})

// 范围分片 - 用户表按ID范围分片
userWarp := dbwarp.New().AddRule(&dbwarp.ShardingRule{
    ShardingKey: "user_id",
    ShardingStrategy: &dbwarp.RangeShardingStrategy{
        Ranges: []int64{0, 10000, 50000, 100000},
    },
}, User{})
	`)
}
