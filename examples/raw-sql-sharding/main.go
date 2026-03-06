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
type OrderInfo struct{}

func (OrderInfo) TableName() string {
	return "order_info"
}

// Person for table registration
type Person struct{}

func (Person) TableName() string {
	return "person"
}

// TestRawSQLSharding tests raw SQL with sharding for order_info and person tables
func TestRawSQLSharding() {
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
	// Sharding key: user_id
	// 4 databases, 10 tables per database
	orderRule := &dbwarp.ShardingRule{
		ShardingKey:            "user_id",
		DatabaseNumberOfShards: 4,
		TableNumberOfShards:    10,
		ShardingDatabaseAlgorithm: func(columnValue any) (suffix string, err error) {
			dbIdx := cast.ToInt64(columnValue) % 4
			fmt.Printf("[order_info] Database sharding: user_id=%v -> db_%d\n", columnValue, dbIdx)
			return fmt.Sprintf("_%d", dbIdx), nil
		},
		ShardingTableAlgorithm: func(columnValue any) (suffix string, err error) {
			tableIdx := cast.ToInt64(columnValue) % 10
			fmt.Printf("[order_info] Table sharding: user_id=%v -> table_%d\n", columnValue, tableIdx)
			return fmt.Sprintf("_%d", tableIdx), nil
		},
	}
	for _, r := range routers {
		orderRule.AddRouter(r)
	}

	// Person sharding rule
	// Sharding key: id_no (身份证号)
	// 4 databases, 1 table per database (no table sharding)
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
			fmt.Printf("[person] Database sharding: id_no=%v -> db_%d\n", columnValue, dbIdx)
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

	// ===================== Test Raw SQL for order_info =====================
	fmt.Println("\n===== Test Raw SQL for order_info =====")

	// Insert using raw SQL
	userIDs := []int64{1001, 2002, 3003, 4004}
	for _, userID := range userIDs {
		sql := fmt.Sprintf("INSERT INTO order_info (user_id, name, ctime) VALUES (%d, 'order_%d', '%s')",
			userID, userID, time.Now().Format("2006-01-02 15:04:05"))
		err = db.Exec(sql).Error
		if err != nil {
			fmt.Printf("Insert order_info user_id=%d failed: %v\n", userID, err)
		} else {
			fmt.Printf("Insert order_info user_id=%d succeeded\n", userID)
		}
	}

	// Query using raw SQL
	fmt.Println("\n--- Query order_info ---")
	for _, userID := range userIDs {
		var results []map[string]interface{}
		sql := fmt.Sprintf("SELECT * FROM order_info WHERE user_id = %d", userID)
		err = db.Raw(sql).Find(&results).Error
		if err != nil {
			fmt.Printf("Query order_info user_id=%d failed: %v\n", userID, err)
		} else {
			fmt.Printf("Query order_info user_id=%d succeeded, %d records found\n", userID, len(results))
		}
	}

	// ===================== Test Raw SQL for person =====================
	fmt.Println("\n===== Test Raw SQL for person =====")

	// Insert using raw SQL
	idNos := []string{"1234567890120", "1234567890121", "1234567890122", "1234567890123"}
	names := []string{"张三", "李四", "王五", "赵六"}
	ages := []int{30, 25, 35, 28}
	sexes := []string{"男", "女", "男", "女"}

	for i, idNo := range idNos {
		sql := fmt.Sprintf("INSERT INTO person (id_no, name, age, sex, birthday) VALUES ('%s', '%s', %d, '%s', '%s')",
			idNo, names[i], ages[i], sexes[i], time.Now().AddDate(-ages[i], 0, 0).Format("2006-01-02"))
		err = db.Exec(sql).Error
		if err != nil {
			fmt.Printf("Insert person id_no=%s failed: %v\n", idNo, err)
		} else {
			fmt.Printf("Insert person id_no=%s succeeded\n", idNo)
		}
	}

	// Query using raw SQL
	fmt.Println("\n--- Query person ---")
	for _, idNo := range idNos {
		var results []map[string]interface{}
		sql := fmt.Sprintf("SELECT * FROM person WHERE id_no = '%s'", idNo)
		err = db.Raw(sql).Find(&results).Error
		if err != nil {
			fmt.Printf("Query person id_no=%s failed: %v\n", idNo, err)
		} else {
			fmt.Printf("Query person id_no=%s succeeded, %d records found\n", idNo, len(results))
		}
	}

	fmt.Println("\nTest completed")
}

func main() {
	TestRawSQLSharding()
}
