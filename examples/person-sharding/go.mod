module dbwarp/examples/person-sharding

go 1.20

require (
	dbwarp v0.0.0
	github.com/spf13/cast v1.5.0
	gorm.io/driver/mysql v1.6.0
	gorm.io/gorm v1.31.1
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/go-sql-driver/mysql v1.8.1 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/longbridgeapp/sqlparser v0.3.2 // indirect
	golang.org/x/text v0.20.0 // indirect
)

replace dbwarp => ../../
