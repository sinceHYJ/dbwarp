module example/sharding-strategies

go 1.20

require (
	github.com/sincehyj/dbwarp v0.0.0
	gorm.io/driver/mysql v1.5.4
	gorm.io/gorm v1.31.1
)

require (
	github.com/go-sql-driver/mysql v1.7.1 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/longbridgeapp/sqlparser v0.3.2 // indirect
	golang.org/x/text v0.20.0 // indirect
)

replace github.com/sincehyj/dbwarp => ../..
