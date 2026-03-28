package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/sincehyj/dbwarp"
)

func createDSN(host string, port int, user, password, dbname string) string {
	return fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		user, password, host, port, dbname,
	)
}

// Order 订单表 - 使用 Hash 分片策略
type Order struct {
	ID        int64     `gorm:"column:id;AUTO_INCREMENT;primary_key"`
	UserID    int64     `gorm:"column:user_id;index"`
	OrderNo   string    `gorm:"column:order_no;size:64"`
	Amount    float64   `gorm:"column:amount"`
	Status    int       `gorm:"column:status"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

func (Order) TableName() string {
	return "orders"
}

// RegionOrder 区域订单表 - 使用 Enum 分片策略
type RegionOrder struct {
	ID        int64     `gorm:"column:id;AUTO_INCREMENT;primary_key"`
	Region    string    `gorm:"column:region;size:32;index"`
	OrderNo   string    `gorm:"column:order_no;size:64"`
	Amount    float64   `gorm:"column:amount"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

func (RegionOrder) TableName() string {
	return "region_order"
}

// AccessLog 访问日志表 - 使用 Date 分片策略
type AccessLog struct {
	ID        int64     `gorm:"column:id;AUTO_INCREMENT;primary_key"`
	UserID    int64     `gorm:"column:user_id;index"`
	Action    string    `gorm:"column:action;size:64"`
	CreatedAt time.Time `gorm:"column:created_at;index"`
}

func (AccessLog) TableName() string {
	return "access_log"
}

// User 用户表 - 使用 Range 分片策略
type User struct {
	ID       int64  `gorm:"column:id;AUTO_INCREMENT;primary_key"`
	Name     string `gorm:"column:name;size:64"`
	Email    string `gorm:"column:email;size:128"`
	Region   string `gorm:"column:region;size:32"`
	IsActive bool   `gorm:"column:is_active"`
}

func (User) TableName() string {
	return "user"
}

var (
	TableOrder       = "orders"
	TableRegionOrder = "region_order"
	TableAccessLog   = "access_log"
	TableUser        = "user"
)

// 数据库配置
var masterConfigs = []struct {
	host     string
	port     int
	user     string
	password string
	dbname   string
}{
	{"127.0.0.1", 3307, "test_user", "test123", "test_db"},
	{"127.0.0.1", 3308, "test_user", "test123", "test_db"},
	{"127.0.0.1", 3309, "test_user", "test123", "test_db"},
	{"127.0.0.1", 3310, "test_user", "test123", "test_db"},
}

var replicaConfigs = []struct {
	host     string
	port     int
	user     string
	password string
	dbname   string
}{
	{"127.0.0.1", 3311, "test_user", "test123", "test_db"},
	{"127.0.0.1", 3312, "test_user", "test123", "test_db"},
	{"127.0.0.1", 3313, "test_user", "test123", "test_db"},
	{"127.0.0.1", 3314, "test_user", "test123", "test_db"},
}

func main() {
	fmt.Println("=== 分片策略完整示例 ===")

	// 连接默认数据库
	dsn := createDSN("127.0.0.1", 3315, "test_user", "test123", "test_db")
	customLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  logger.Info,
			IgnoreRecordNotFoundError: true,
			Colorful:                  true,
		},
	)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: customLogger})
	if err != nil {
		panic(fmt.Sprintf("连接数据库失败: %v", err))
	}

	if err := db.Exec(`SELECT 1`).Error; err != nil {
		panic(fmt.Sprintf("数据库连接测试失败: %v", err))
	}
	fmt.Println("数据库连接成功")

	// 创建分片表结构
	fmt.Println("\n>>> 创建分片表结构...")
	createShardingTables(db)
	fmt.Println("分片表创建完成")

	// 创建分片插件
	warp := dbwarp.New()

	// 添加各种分片规则
	setupHashShardingRule(warp)   // Hash 分片
	setupEnumShardingRule(warp)   // Enum 分片
	setupDateShardingRule(warp)   // Date 分片
	setupRangeShardingRule(warp)  // Range 分片

	// 注册插件
	if err := db.Use(warp); err != nil {
		panic(fmt.Sprintf("注册分片插件失败: %v", err))
	}
	fmt.Println("分片规则注册成功")

	// 执行测试
	fmt.Println("\n" + strings.Repeat("=", 60))
	testHashSharding(db)
	testEnumSharding(db)
	testDateSharding(db)
	testRangeSharding(db)

	fmt.Println("\n所有测试完成")
}

// setupHashShardingRule 配置 Hash 分片规则 - 订单表
func setupHashShardingRule(warp *dbwarp.Warp) {
	strategy := &dbwarp.HashShardingStrategy{Shards: 4}

	rule := &dbwarp.ShardingRule{
		ShardingKey:            "user_id",
		DatabaseNumberOfShards: 4,
		TableNumberOfShards:    1,
		ShardingDatabaseAlgorithm: strategy.GetSuffix,
		ShardingTableAlgorithm:    dbwarp.NoShardingAlgorithm(),
	}

	for i := 0; i < 4; i++ {
		rule.AddRouter(dbwarp.RouterCfg{
			Name:              fmt.Sprintf("db_%d", i),
			Sources:           []gorm.Dialector{mysql.Open(createDSN(masterConfigs[i].host, masterConfigs[i].port, masterConfigs[i].user, masterConfigs[i].password, masterConfigs[i].dbname))},
			Replicas:          []gorm.Dialector{mysql.Open(createDSN(replicaConfigs[i].host, replicaConfigs[i].port, replicaConfigs[i].user, replicaConfigs[i].password, replicaConfigs[i].dbname))},
			ReplPolicy:        dbwarp.RandomPolicy{},
			TraceResolverMode: true,
		})
	}

	warp.AddRule(rule, Order{})
	fmt.Println("[Hash分片] 订单表分片规则已配置")
}

// setupEnumShardingRule 配置 Enum 分片规则 - 区域订单表
func setupEnumShardingRule(warp *dbwarp.Warp) {
	strategy := &dbwarp.EnumShardingStrategy{
		EnumMap: map[string]string{
			"beijing":   "_0",
			"shanghai":  "_1",
			"guangzhou": "_2",
			"shenzhen":  "_3",
		},
		DefaultSuffix: "_0",
	}

	rule := &dbwarp.ShardingRule{
		ShardingKey:            "region",
		DatabaseNumberOfShards: 4,
		TableNumberOfShards:    1,
		ShardingDatabaseAlgorithm: strategy.GetSuffix,
		ShardingTableAlgorithm:    dbwarp.NoShardingAlgorithm(),
	}

	for i := 0; i < 4; i++ {
		rule.AddRouter(dbwarp.RouterCfg{
			Name:              fmt.Sprintf("db_%d", i),
			Sources:           []gorm.Dialector{mysql.Open(createDSN(masterConfigs[i].host, masterConfigs[i].port, masterConfigs[i].user, masterConfigs[i].password, masterConfigs[i].dbname))},
			Replicas:          []gorm.Dialector{mysql.Open(createDSN(replicaConfigs[i].host, replicaConfigs[i].port, replicaConfigs[i].user, replicaConfigs[i].password, replicaConfigs[i].dbname))},
			ReplPolicy:        dbwarp.RandomPolicy{},
			TraceResolverMode: true,
		})
	}

	warp.AddRule(rule, RegionOrder{})
	fmt.Println("[Enum分片] 区域订单表分片规则已配置")
}

// setupDateShardingRule 配置 Date 分片规则 - 访问日志表
func setupDateShardingRule(warp *dbwarp.Warp) {
	strategy := &dbwarp.DateShardingStrategy{
		Format: "200601", // 按月分表
	}

	rule := &dbwarp.ShardingRule{
		ShardingKey:            "created_at",
		DatabaseNumberOfShards: 4,
		TableNumberOfShards:    12, // 每个库12个月份表
		ShardingDatabaseAlgorithm: func(columnValue any) (string, error) {
			// 日期的月份决定表，日期的哈希决定库
			return "_0", nil // 简化：所有日志写入第一个库
		},
		ShardingTableAlgorithm: strategy.GetSuffix,
	}

	for i := 0; i < 4; i++ {
		rule.AddRouter(dbwarp.RouterCfg{
			Name:              fmt.Sprintf("db_%d", i),
			Sources:           []gorm.Dialector{mysql.Open(createDSN(masterConfigs[i].host, masterConfigs[i].port, masterConfigs[i].user, masterConfigs[i].password, masterConfigs[i].dbname))},
			Replicas:          []gorm.Dialector{mysql.Open(createDSN(replicaConfigs[i].host, replicaConfigs[i].port, replicaConfigs[i].user, replicaConfigs[i].password, replicaConfigs[i].dbname))},
			ReplPolicy:        dbwarp.RandomPolicy{},
			TraceResolverMode: true,
		})
	}

	warp.AddRule(rule, AccessLog{})
	fmt.Println("[Date分片] 访问日志表分片规则已配置")
}

// setupRangeShardingRule 配置 Range 分片规则 - 用户表
func setupRangeShardingRule(warp *dbwarp.Warp) {
	strategy := &dbwarp.RangeShardingStrategy{
		Ranges: []int64{0, 10000, 50000, 100000},
	}

	rule := &dbwarp.ShardingRule{
		ShardingKey:            "id",
		DatabaseNumberOfShards: 4,
		TableNumberOfShards:    1,
		ShardingDatabaseAlgorithm: strategy.GetSuffix,
		ShardingTableAlgorithm:    dbwarp.NoShardingAlgorithm(),
	}

	for i := 0; i < 4; i++ {
		rule.AddRouter(dbwarp.RouterCfg{
			Name:              fmt.Sprintf("db_%d", i),
			Sources:           []gorm.Dialector{mysql.Open(createDSN(masterConfigs[i].host, masterConfigs[i].port, masterConfigs[i].user, masterConfigs[i].password, masterConfigs[i].dbname))},
			Replicas:          []gorm.Dialector{mysql.Open(createDSN(replicaConfigs[i].host, replicaConfigs[i].port, replicaConfigs[i].user, replicaConfigs[i].password, replicaConfigs[i].dbname))},
			ReplPolicy:        dbwarp.RandomPolicy{},
			TraceResolverMode: true,
		})
	}

	warp.AddRule(rule, User{})
	fmt.Println("[Range分片] 用户表分片规则已配置")
}

// testHashSharding 测试 Hash 分片
func testHashSharding(db *gorm.DB) {
	fmt.Println("\n>>> 测试 Hash 分片策略 - 订单表")

	// 插入订单
	now := time.Now()
	orders := []Order{
		{UserID: 1001, OrderNo: "ORD-001", Amount: 99.9, Status: 1, CreatedAt: now},
		{UserID: 2002, OrderNo: "ORD-002", Amount: 199.9, Status: 1, CreatedAt: now},
		{UserID: 3003, OrderNo: "ORD-003", Amount: 299.9, Status: 1, CreatedAt: now},
		{UserID: 4004, OrderNo: "ORD-004", Amount: 399.9, Status: 1, CreatedAt: now},
	}

	for _, order := range orders {
		if err := db.Table(TableOrder).Create(&order).Error; err != nil {
			fmt.Printf("  插入失败 [user_id=%d]: %v\n", order.UserID, err)
		} else {
			fmt.Printf("  插入成功 [user_id=%d, order_no=%s]\n", order.UserID, order.OrderNo)
		}
	}

	// 查询订单
	fmt.Println("\n  查询订单:")
	for _, userID := range []int64{1001, 2002, 3003, 4004} {
		var orders []Order
		if err := db.Table(TableOrder).Where("user_id = ?", userID).Find(&orders).Error; err != nil {
			fmt.Printf("    查询失败 [user_id=%d]: %v\n", userID, err)
		} else {
			fmt.Printf("    查询成功 [user_id=%d]: %d 条记录\n", userID, len(orders))
		}
	}
}

// testEnumSharding 测试 Enum 分片
func testEnumSharding(db *gorm.DB) {
	fmt.Println("\n>>> 测试 Enum 分片策略 - 区域订单表")

	// 插入区域订单
	now := time.Now()
	orders := []RegionOrder{
		{Region: "beijing", OrderNo: "REG-BJ-001", Amount: 100.0, CreatedAt: now},
		{Region: "shanghai", OrderNo: "REG-SH-001", Amount: 200.0, CreatedAt: now},
		{Region: "guangzhou", OrderNo: "REG-GZ-001", Amount: 300.0, CreatedAt: now},
		{Region: "shenzhen", OrderNo: "REG-SZ-001", Amount: 400.0, CreatedAt: now},
		{Region: "hangzhou", OrderNo: "REG-HZ-001", Amount: 500.0, CreatedAt: now}, // 使用默认分片
	}

	for _, order := range orders {
		if err := db.Table(TableRegionOrder).Create(&order).Error; err != nil {
			fmt.Printf("  插入失败 [region=%s]: %v\n", order.Region, err)
		} else {
			fmt.Printf("  插入成功 [region=%s, order_no=%s]\n", order.Region, order.OrderNo)
		}
	}

	// 查询区域订单
	fmt.Println("\n  查询区域订单:")
	for _, region := range []string{"beijing", "shanghai", "hangzhou"} {
		var orders []RegionOrder
		if err := db.Table(TableRegionOrder).Where("region = ?", region).Find(&orders).Error; err != nil {
			fmt.Printf("    查询失败 [region=%s]: %v\n", region, err)
		} else {
			fmt.Printf("    查询成功 [region=%s]: %d 条记录\n", region, len(orders))
		}
	}
}

// testDateSharding 测试 Date 分片
func testDateSharding(db *gorm.DB) {
	fmt.Println("\n>>> 测试 Date 分片策略 - 访问日志表")

	// 插入不同月份的日志
	logs := []AccessLog{
		{UserID: 1001, Action: "login", CreatedAt: time.Date(2026, 1, 15, 10, 0, 0, 0, time.Local)},
		{UserID: 1002, Action: "logout", CreatedAt: time.Date(2026, 2, 20, 11, 0, 0, 0, time.Local)},
		{UserID: 1003, Action: "purchase", CreatedAt: time.Date(2026, 3, 25, 12, 0, 0, 0, time.Local)},
	}

	for _, logEntry := range logs {
		if err := db.Table(TableAccessLog).Create(&logEntry).Error; err != nil {
			fmt.Printf("  插入失败 [user_id=%d, month=%s]: %v\n", logEntry.UserID, logEntry.CreatedAt.Format("2006-01"), err)
		} else {
			fmt.Printf("  插入成功 [user_id=%d, month=%s, action=%s]\n", logEntry.UserID, logEntry.CreatedAt.Format("2006-01"), logEntry.Action)
		}
	}

	// 按月份查询（必须使用 = 操作符）
	fmt.Println("\n  查询访问日志:")
	queries := []time.Time{
		time.Date(2026, 1, 15, 10, 0, 0, 0, time.Local),
		time.Date(2026, 2, 20, 11, 0, 0, 0, time.Local),
	}
	for _, qTime := range queries {
		var logs []AccessLog
		// 使用 = 操作符查询指定月份的数据
		if err := db.Table(TableAccessLog).Where("created_at = ?", qTime).Find(&logs).Error; err != nil {
			fmt.Printf("    查询失败 [month=%s]: %v\n", qTime.Format("2006-01"), err)
		} else {
			fmt.Printf("    查询成功 [month=%s]: %d 条记录\n", qTime.Format("2006-01"), len(logs))
		}
	}
}

// testRangeSharding 测试 Range 分片
func testRangeSharding(db *gorm.DB) {
	fmt.Println("\n>>> 测试 Range 分片策略 - 用户表")

	// 插入不同 ID 范围的用户
	users := []User{
		{Name: "用户A", Email: "a@example.com", Region: "beijing", IsActive: true},  // ID ~ 5000
		{Name: "用户B", Email: "b@example.com", Region: "shanghai", IsActive: true}, // ID ~ 20000
		{Name: "用户C", Email: "c@example.com", Region: "guangzhou", IsActive: true}, // ID ~ 60000
	}

	for _, user := range users {
		if err := db.Table(TableUser).Create(&user).Error; err != nil {
			fmt.Printf("  插入失败 [name=%s]: %v\n", user.Name, err)
		} else {
			fmt.Printf("  插入成功 [id=%d, name=%s]\n", user.ID, user.Name)
		}
	}

	// 查询用户
	fmt.Println("\n  查询用户:")
	for _, id := range []int64{1, 5000, 15000, 60000} {
		var users []User
		if err := db.Table(TableUser).Where("id >= ?", id).Limit(1).Find(&users).Error; err != nil {
			fmt.Printf("    查询失败 [id>=%d]: %v\n", id, err)
		} else if len(users) > 0 {
			fmt.Printf("    查询成功 [id>=%d]: id=%d, name=%s\n", id, users[0].ID, users[0].Name)
		} else {
			fmt.Printf("    查询成功 [id>=%d]: 无记录\n", id)
		}
	}
}

// createShardingTables 在各分片库中创建表结构
func createShardingTables(db *gorm.DB) {
	// 定义需要创建的表结构
	tableDDLs := map[string]string{
		// orders 表 - Hash 分片
		"orders": `CREATE TABLE IF NOT EXISTS orders (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			user_id BIGINT NOT NULL,
			order_no VARCHAR(64) NOT NULL,
			amount DECIMAL(10,2),
			status INT DEFAULT 0,
			created_at DATETIME,
			INDEX idx_user_id (user_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

		// region_order 表 - Enum 分片
		"region_order": `CREATE TABLE IF NOT EXISTS region_order (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			region VARCHAR(32) NOT NULL,
			order_no VARCHAR(64) NOT NULL,
			amount DECIMAL(10,2),
			created_at DATETIME,
			INDEX idx_region (region)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

		// user 表 - Range 分片（不使用自增ID，改用 user_code 作为分片键）
		"user": `CREATE TABLE IF NOT EXISTS user (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			user_code BIGINT NOT NULL,
			name VARCHAR(64) NOT NULL,
			email VARCHAR(128),
			region VARCHAR(32),
			is_active TINYINT(1) DEFAULT 1,
			INDEX idx_user_code (user_code)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	}

	// access_log 月份表 DDL 模板
	accessLogMonthlyDDL := `CREATE TABLE IF NOT EXISTS access_log%s (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		user_id BIGINT NOT NULL,
		action VARCHAR(64) NOT NULL,
		created_at DATETIME,
		INDEX idx_user_id (user_id),
		INDEX idx_created_at (created_at)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	// 在每个主库中创建表
	for i := 0; i < 4; i++ {
		masterDSN := createDSN(masterConfigs[i].host, masterConfigs[i].port, masterConfigs[i].user, masterConfigs[i].password, masterConfigs[i].dbname)
		masterDB, err := gorm.Open(mysql.Open(masterDSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		if err != nil {
			fmt.Printf("  连接主库 %d 失败: %v\n", i, err)
			continue
		}

		dbName := fmt.Sprintf("db_%d", i)
		fmt.Printf("  在 %s (%s) 创建表...\n", dbName, masterConfigs[i].host)

		// 创建基础表
		for tableName, ddl := range tableDDLs {
			if err := masterDB.Exec(ddl).Error; err != nil {
				fmt.Printf("    创建表 %s 失败: %v\n", tableName, err)
			} else {
				fmt.Printf("    创建表 %s 成功\n", tableName)
			}
		}

		// 创建 12 个月的 access_log 分表
		for month := 1; month <= 12; month++ {
			tableName := fmt.Sprintf("access_log_%d%02d", 2026, month)
			ddl := fmt.Sprintf(accessLogMonthlyDDL, fmt.Sprintf("_%d%02d", 2026, month))
			if err := masterDB.Exec(ddl).Error; err != nil {
				fmt.Printf("    创建月表 %s 失败: %v\n", tableName, err)
			} else {
				fmt.Printf("    创建月表 %s 成功\n", tableName)
			}
		}

		sqlDB, _ := masterDB.DB()
		sqlDB.Close()
	}
}
