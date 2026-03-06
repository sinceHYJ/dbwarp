package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	// "gorm.io/hints"

	"dbwarp"

	"github.com/spf13/cast"
)

// createDSN helper function
func createDSN(host string, port int, user, password, dbname string) string {
	return fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local",
		user, password, host, port, dbname,
	)
}

type OrderInfo struct {
	ID     int64     `gorm:"column:id"`
	UserID int64     `gorm:"column:user_id"`
	Name   string    `gorm:"column:name"`
	Ctime  time.Time `gorm:"column:ctime"`
}

func (OrderInfo) TableName() string {
	return "order_info"
}

type Photo struct {
	ID       int64     `gorm:"column:id;AUTO_INCREMENT;primary_key"`
	Name     string    `gorm:"column:name"`
	Location string    `gorm:"column:location"`
	Url      string    `gorm:"column:url"`
	Ctime    time.Time `gorm:"column:ctime"`
}

func (Photo) TableName() string {
	return "photo"
}

var TableOrderInfo = "order_info"
var TablePhoto = "photo"

// TestMultiDBWithReplicas tests 4 sharded databases with replicas
// sharding key: user_id
func TestMultiDBWithReplicas() {
	// Define 4 master database configs
	masterConfigs := []struct {
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

	// Define 4 replica database configs (corresponding to 4 masters)
	replicaConfigs := []struct {
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

	// Use the first master as default connection
	dsn := createDSN(
		"127.0.0.1",
		3315,
		"test_user",
		"test123",
		"test_db",
	)

	// Custom logger: print SQL statements and execution time
	customLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             200 * time.Millisecond, // Slow query threshold
			LogLevel:                  logger.Info,            // Log level
			IgnoreRecordNotFoundError: true,                   // Ignore ErrRecordNotFound
			Colorful:                  true,                   // Enable colorful output
		},
	)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: customLogger,
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to database: %v", err))
	}

	// Test connection
	err = db.Exec(`SELECT 1`).Error
	if err != nil {
		panic(fmt.Sprintf("Database connection test failed: %v", err))
	}
	fmt.Println("Default database connected successfully")

	// Build router configs for 4 sharded databases
	rule := &dbwarp.ShardingRule{
		ShardingKey:            "user_id",
		DatabaseNumberOfShards: 4,
		TableNumberOfShards:    10,
		// Database sharding algorithm: user_id % 4
		ShardingDatabaseAlgorithm: func(columnValue any) (suffix string, err error) {
			dbIdx := cast.ToInt64(columnValue) % 4
			fmt.Printf("Database sharding: user_id=%v -> db_%d\n", columnValue, dbIdx)
			return fmt.Sprintf("_%d", dbIdx), nil
		},
		// Table sharding algorithm: user_id % 10
		ShardingTableAlgorithm: func(columnValue any) (suffix string, err error) {
			tableIdx := cast.ToInt64(columnValue) % 10
			fmt.Printf("Table sharding: user_id=%v -> table_%d\n", columnValue, tableIdx)
			return fmt.Sprintf("_%d", tableIdx), nil
		},
	}

	for i := 0; i < 4; i++ {
		cfg := dbwarp.RouterCfg{
			Name:              fmt.Sprintf("db_%d", i),
			Sources:           []gorm.Dialector{mysql.Open(createDSN(masterConfigs[i].host, masterConfigs[i].port, masterConfigs[i].user, masterConfigs[i].password, masterConfigs[i].dbname))},
			Replicas:          []gorm.Dialector{mysql.Open(createDSN(replicaConfigs[i].host, replicaConfigs[i].port, replicaConfigs[i].user, replicaConfigs[i].password, replicaConfigs[i].dbname))},
			ReplPolicy:        dbwarp.RandomPolicy{},
			TraceResolverMode: true,
		}
		rule.AddRouter(cfg)
	}

	// Register sharding rules
	warp := dbwarp.New().AddRule(rule, OrderInfo{})

	err = db.Use(warp)
	if err != nil {
		panic(fmt.Sprintf("Failed to register sharding plugin: %v", err))
	}
	fmt.Println("Sharding rules registered successfully")

	// Test querying order_info
	fmt.Println("\n===== Test querying order_info =====")

	// Test queries with different user_ids
	testUserIDs := []int64{1001, 2002, 3003, 4004, 5005}
	for _, userID := range testUserIDs {
		var orders []OrderInfo
		err = db.Table(TableOrderInfo).Where("user_id = ?", userID).Find(&orders).Error
		if err != nil {
			fmt.Printf("Query user_id=%d failed: %v\n", userID, err)
		} else {
			fmt.Printf("Query user_id=%d succeeded, %d records found\n", userID, len(orders))
		}
	}

	// Test writing
	fmt.Println("\n===== Test writing order_info =====")
	newOrder := OrderInfo{
		UserID: 10001,
		Name:   "test_order",
		Ctime:  time.Now(),
	}
	err = db.Table(TableOrderInfo).Create(&newOrder).Error
	if err != nil {
		fmt.Printf("Write failed: %v\n", err)
	} else {
		fmt.Printf("Write succeeded: user_id=%d\n", newOrder.UserID)
	}

	// Test photo table (no sharding)
	fmt.Println("\n===== Test photo table =====")

	// Insert photo
	newPhoto := Photo{
		Name:     "test_photo",
		Location: "/data/photos",
		Url:      "http://example.com/photo/test.jpg",
		Ctime:    time.Now(),
	}
	err = db.Table(TablePhoto).Create(&newPhoto).Error
	if err != nil {
		fmt.Printf("Insert photo failed: %v\n", err)
	} else {
		fmt.Printf("Insert photo succeeded: id=%d, name=%s\n", newPhoto.ID, newPhoto.Name)
	}

	// Query photo
	var photos []Photo
	err = db.Table(TablePhoto).Find(&photos).Error
	if err != nil {
		fmt.Printf("Query photo failed: %v\n", err)
	} else {
		fmt.Printf("Query photo succeeded, %d records found\n", len(photos))
		for _, p := range photos {
			fmt.Printf("  - id=%d, name=%s, location=%s, url=%s\n", p.ID, p.Name, p.Location, p.Url)
		}
	}

	fmt.Println("\nTest completed")
}

func main() {
	// Run multi-database with replicas test
	TestMultiDBWithReplicas()
}
