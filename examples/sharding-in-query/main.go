package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"dbwarp"

	"github.com/spf13/cast"
)

// createDSN helper function
func createDSN(host string, port int, user, password, dbname string) string {
	return fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local",
		user, password, host, port, dbname,
	)
}

// OrderInfo for table registration
type OrderInfo struct {
	ID     int64     `gorm:"column:id"`
	UserID int64     `gorm:"column:user_id"`
	Name   string    `gorm:"column:name"`
	Ctime  time.Time `gorm:"column:ctime"`
}

func (OrderInfo) TableName() string {
	return "order_info"
}

// Person for table registration
type Person struct {
	ID       int64      `gorm:"column:id"`
	Age      int        `gorm:"column:age"`
	Name     string     `gorm:"column:name"`
	Sex      string     `gorm:"column:sex"`
	Birthday *time.Time `gorm:"column:birthday"`
	IdNo     *string    `gorm:"column:id_no"`
}

func (Person) TableName() string {
	return "person"
}

// TestShardingINQuery tests IN query with sharding for order_info and person tables
func TestShardingINQuery() {
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

	// Define 4 replica database configs
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
	dsn := createDSN("127.0.0.1", 3307, "test_user", "test123", "test_db")

	// Custom logger
	customLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  logger.Info,
			IgnoreRecordNotFoundError: true,
			Colorful:                  true,
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

	// Build routers for 4 databases
	var routers []dbwarp.RouterCfg
	for i := 0; i < 4; i++ {
		cfg := dbwarp.RouterCfg{
			Name:              fmt.Sprintf("db_%d", i),
			Sources:           []gorm.Dialector{mysql.Open(createDSN(masterConfigs[i].host, masterConfigs[i].port, masterConfigs[i].user, masterConfigs[i].password, masterConfigs[i].dbname))},
			Replicas:          []gorm.Dialector{mysql.Open(createDSN(replicaConfigs[i].host, replicaConfigs[i].port, replicaConfigs[i].user, replicaConfigs[i].password, replicaConfigs[i].dbname))},
			ReplPolicy:        dbwarp.RandomPolicy{},
			TraceResolverMode: true,
		}
		routers = append(routers, cfg)
	}

	// OrderInfo sharding rule
	orderRule := &dbwarp.ShardingRule{
		ShardingKey:            "user_id",
		DatabaseNumberOfShards: 4,
		TableNumberOfShards:    10,
		ShardingDatabaseAlgorithm: func(columnValue any) (suffix string, err error) {
			dbIdx := cast.ToInt64(columnValue) % 4
			return fmt.Sprintf("_%d", dbIdx), nil
		},
		ShardingTableAlgorithm: func(columnValue any) (suffix string, err error) {
			tableIdx := cast.ToInt64(columnValue) % 10
			return fmt.Sprintf("_%d", tableIdx), nil
		},
	}
	for _, r := range routers {
		orderRule.AddRouter(r)
	}

	// Person sharding rule
	personRule := &dbwarp.ShardingRule{
		ShardingKey:            "id_no",
		DatabaseNumberOfShards: 4,
		TableNumberOfShards:    1,
		ShardingDatabaseAlgorithm: func(columnValue any) (suffix string, err error) {
			idNoStr := cast.ToString(columnValue)
			lastTwo := idNoStr
			if len(idNoStr) >= 2 {
				lastTwo = idNoStr[len(idNoStr)-2:]
			}
			dbIdx := cast.ToInt64(lastTwo) % 4
			return fmt.Sprintf("_%d", dbIdx), nil
		},
		ShardingTableAlgorithm: dbwarp.NoShardingAlgorithm(),
	}
	for _, r := range routers {
		personRule.AddRouter(r)
	}

	// Register sharding rules
	warp := dbwarp.New().
		AddRule(orderRule, OrderInfo{}).
		AddRule(personRule, Person{})

	err = db.Use(warp)
	if err != nil {
		panic(fmt.Sprintf("Failed to register sharding plugin: %v", err))
	}
	fmt.Println("Sharding rules registered successfully")

	// ===================== Test IN Query for order_info =====================
	fmt.Println("\n===== Test IN Query for order_info =====")

	// Single user_id query (baseline)
	fmt.Println("\n--- Single user_id query ---")
	var orders []OrderInfo
	err = db.Where("user_id = ?", 1001).Find(&orders).Error
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
	} else {
		fmt.Printf("Query user_id=1001 succeeded, %d records found\n", len(orders))
	}

	// IN query with multiple user_ids (across different shards)
	fmt.Println("\n--- IN query with multiple user_ids ---")
	userIDs := []int64{1001, 2002, 3003, 4004}
	var ordersIn []OrderInfo
	err = db.Where("user_id IN ?", userIDs).Find(&ordersIn).Error
	if err != nil {
		fmt.Printf("IN query failed: %v\n", err)
	} else {
		fmt.Printf("IN query user_ids=%v succeeded, %d records found\n", userIDs, len(ordersIn))
	}

	// IN query with user_ids in same shard
	fmt.Println("\n--- IN query with user_ids in same shard ---")
	sameShardUserIDs := []int64{1001, 1005, 1009} // All % 4 = 1, same shard
	var ordersSameShard []OrderInfo
	err = db.Where("user_id IN ?", sameShardUserIDs).Find(&ordersSameShard).Error
	if err != nil {
		fmt.Printf("IN query (same shard) failed: %v\n", err)
	} else {
		fmt.Printf("IN query user_ids=%v (same shard) succeeded, %d records found\n", sameShardUserIDs, len(ordersSameShard))
	}

	// ===================== Test IN Query for person =====================
	fmt.Println("\n===== Test IN Query for person =====")

	// Single id_no query (baseline)
	fmt.Println("\n--- Single id_no query ---")
	var persons []Person
	err = db.Where("id_no = ?", "1234567890120").Find(&persons).Error
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
	} else {
		fmt.Printf("Query id_no=1234567890120 succeeded, %d records found\n", len(persons))
	}

	// IN query with multiple id_nos (across different shards)
	fmt.Println("\n--- IN query with multiple id_nos ---")
	idNos := []string{"1234567890120", "1234567890121", "1234567890122", "1234567890123"}
	var personsIn []Person
	err = db.Where("id_no IN ?", idNos).Find(&personsIn).Error
	if err != nil {
		fmt.Printf("IN query failed: %v\n", err)
	} else {
		fmt.Printf("IN query id_nos=%v succeeded, %d records found\n", idNos, len(personsIn))
	}

	// IN query with id_nos in same shard
	fmt.Println("\n--- IN query with id_nos in same shard ---")
	sameShardIdNos := []string{"1234567890120", "1234567890124", "1234567890128"} // All % 4 = 0
	var personsSameShard []Person
	err = db.Where("id_no IN ?", sameShardIdNos).Find(&personsSameShard).Error
	if err != nil {
		fmt.Printf("IN query (same shard) failed: %v\n", err)
	} else {
		fmt.Printf("IN query id_nos=%v (same shard) succeeded, %d records found\n", sameShardIdNos, len(personsSameShard))
	}

	// ===================== Test Raw SQL IN Query =====================
	fmt.Println("\n===== Test Raw SQL IN Query =====")

	// Raw SQL IN query for order_info
	fmt.Println("\n--- Raw SQL IN query for order_info ---")
	var rawOrders []map[string]interface{}
	err = db.Raw("SELECT * FROM order_info WHERE user_id IN (?, ?, ?, ?)", 1001, 2002, 3003, 4004).Find(&rawOrders).Error
	if err != nil {
		fmt.Printf("Raw SQL IN query failed: %v\n", err)
	} else {
		fmt.Printf("Raw SQL IN query succeeded, %d records found\n", len(rawOrders))
	}

	// Raw SQL IN query for person
	fmt.Println("\n--- Raw SQL IN query for person ---")
	var rawPersons []map[string]interface{}
	err = db.Raw("SELECT * FROM person WHERE id_no IN (?, ?, ?, ?)", "1234567890120", "1234567890121", "1234567890122", "1234567890123").Find(&rawPersons).Error
	if err != nil {
		fmt.Printf("Raw SQL IN query failed: %v\n", err)
	} else {
		fmt.Printf("Raw SQL IN query succeeded, %d records found\n", len(rawPersons))
	}

	fmt.Println("\nTest completed")
}

func main() {
	TestShardingINQuery()
}
