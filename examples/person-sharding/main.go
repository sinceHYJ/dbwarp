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

type Person struct {
	ID       int64      `gorm:"column:id;AUTO_INCREMENT;primary_key"`
	Age      int        `gorm:"column:age"`
	Name     string     `gorm:"column:name"`
	Sex      string     `gorm:"column:sex"`
	Birthday *time.Time `gorm:"column:birthday"`
	IdNo     *string    `gorm:"column:id_no;type:decimal(13,0)"` // 身份证号
}

func (Person) TableName() string {
	return "person"
}

var TablePerson = "person"

// TestPersonSharding tests 4 sharded databases with person table
// sharding key: id
func TestPersonSharding() {
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

	// Build sharding rule for person table
	// Sharding key: id_no (身份证号)
	// 4 databases, 1 table per database (no table sharding)
	rule := &dbwarp.ShardingRule{
		ShardingKey:            "id_no",
		DatabaseNumberOfShards: 4,
		TableNumberOfShards:    1, // No table sharding, each DB has only one person table
		// Database sharding algorithm: last 2 digits of id_no % 4
		ShardingDatabaseAlgorithm: func(columnValue any) (suffix string, err error) {
			idNoStr := cast.ToString(columnValue)
			// 取身份证号最后2位作为分库依据
			lastTwo := idNoStr
			if len(idNoStr) >= 2 {
				lastTwo = idNoStr[len(idNoStr)-2:]
			}
			dbIdx := cast.ToInt64(lastTwo) % 4
			fmt.Printf("Database sharding: id_no=%v -> db_%d\n", columnValue, dbIdx)
			return fmt.Sprintf("_%d", dbIdx), nil
		},
		// No table sharding - use NoShardingAlgorithm helper
		ShardingTableAlgorithm: dbwarp.NoShardingAlgorithm(),
	}

	// Add routers for 4 databases
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
	warp := dbwarp.New().AddRule(rule, Person{})

	err = db.Use(warp)
	if err != nil {
		panic(fmt.Sprintf("Failed to register sharding plugin: %v", err))
	}
	fmt.Println("Sharding rules registered successfully")

	// Test querying person
	fmt.Println("\n===== Test querying person =====")

	// Test queries with different id_no
	testIdNos := []string{"1234567890120", "1234567890121", "1234567890122", "1234567890123", "9876543210980"}
	for _, idNo := range testIdNos {
		var persons []Person
		err = db.Table(TablePerson).Where("id_no = ?", idNo).Find(&persons).Error
		if err != nil {
			fmt.Printf("Query id_no=%s failed: %v\n", idNo, err)
		} else {
			fmt.Printf("Query id_no=%s succeeded, %d records found\n", idNo, len(persons))
		}
	}

	// Test writing
	fmt.Println("\n===== Test writing person =====")

	birthday := time.Date(1990, 5, 15, 0, 0, 0, 0, time.Local)
	idNo := "1234567890125"
	newPerson := Person{
		Age:      30,
		Name:     "张三",
		Sex:      "男",
		Birthday: &birthday,
		IdNo:     &idNo,
	}
	err = db.Table(TablePerson).Create(&newPerson).Error
	if err != nil {
		fmt.Printf("Write failed: %v\n", err)
	} else {
		fmt.Printf("Write succeeded: id=%d, name=%s, age=%d, sex=%s, id_no=%s\n", newPerson.ID, newPerson.Name, newPerson.Age, newPerson.Sex, *newPerson.IdNo)
	}

	// Test with another person (will go to different shard)
	fmt.Println("\n===== Test writing another person =====")
	birthday2 := time.Date(1995, 8, 20, 0, 0, 0, 0, time.Local)
	idNo2 := "9876543210986"
	newPerson2 := Person{
		Age:      25,
		Name:     "李四",
		Sex:      "女",
		Birthday: &birthday2,
		IdNo:     &idNo2,
	}
	err = db.Table(TablePerson).Create(&newPerson2).Error
	if err != nil {
		fmt.Printf("Write failed: %v\n", err)
	} else {
		fmt.Printf("Write succeeded: id=%d, name=%s, age=%d, sex=%s, id_no=%s\n", newPerson2.ID, newPerson2.Name, newPerson2.Age, newPerson2.Sex, *newPerson2.IdNo)
	}

	fmt.Println("\nTest completed")
}

func main() {
	TestPersonSharding()
}
