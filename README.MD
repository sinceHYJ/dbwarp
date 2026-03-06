# dbwarp

A GORM plugin for database sharding and read-write splitting. It enables horizontal table sharding and database sharding with simple configuration, while supporting automatic read-write separation in master-replica architectures.

This framework combines and integrates two popular GORM plugins: [gorm-dbresolver](https://github.com/go-gorm/dbresolver) (for read-write splitting) and [gorm-sharding](https://github.com/go-gorm/sharding) (for database/table sharding), providing a unified solution for both functionalities.

[![Go Reference](https://pkg.go.dev/badge/dbwarp.svg)](https://pkg.go.dev/dbwarp)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Features

- **Database Sharding** - Distribute data across multiple databases based on sharding key
- **Table Sharding** - Split tables horizontally with configurable number of shards
- **Read-Write Splitting** - Automatically route read operations to replicas, write operations to sources
- **Custom Sharding Algorithms** - Define your own sharding logic for tables and databases
- **Load Balancing** - Built-in random and round-robin policies for replica selection
- **SQL Parsing** - Automatically parse SQL and extract sharding key values
- **Seamless GORM Integration** - Works as a GORM plugin with minimal code changes
- **Prepared Statement Support** - Full support for GORM's prepared statement feature

## Installation

```bash
go get github.com/sinceHYJ/dbwarp
```


## Quick Start

### Basic Usage (Table Sharding Only)

```go
db, _ := gorm.Open(mysql.Open(dsn))
// Register sharding rules
warp := dbwarp.New().AddRule(&dbwarp.ShardingRule{
    ShardingKey:         "user_id",
    TableNumberOfShards: 10,
    ShardingTableAlgorithm: func(columnValue any) (string, error) {
        return fmt.Sprintf("_%d", cast.ToInt64(columnValue)%10), nil
    },
}, OrderInfo{})

db.Use(warp)
```

### Database Sharding with Read-Write Splitting

```go
rule := &dbwarp.ShardingRule{
    ShardingKey:              "user_id",
    DatabaseNumberOfShards:   4,
    TableNumberOfShards:      10,
    ShardingDatabaseAlgorithm: func(v any) (string, error) {
        return fmt.Sprintf("_%d", cast.ToInt64(v)%4), nil
    },
    ShardingTableAlgorithm: func(v any) (string, error) {
        return fmt.Sprintf("_%d", cast.ToInt64(v)%10), nil
    },
    RouterCfgs: routerCfgs,
}

cfg := dbwarp.RouterCfg{
    Name:     "name",
    Sources:  []gorm.Dialector{mysql.Open("dsn_source")},
    Replicas: []gorm.Dialector{mysql.Open("dsn_repl")},
    ReplPolicy: dbwarp.RandomPolicy{},
}
rule.AddRouter(cfg)

warp := dbwarp.New().AddRule(rule, OrderInfo{})
db.Use(warp)

```

## Limitations

- Requires sharding key in WHERE clause for queries (use `=` operator)
- INSERT statements must include the sharding key
- Cross-shard queries are not supported

## Project Status

> ⚠️ This project is in its early stages of development. Many features are still pending implementation. If you have specific requirements or feature requests, you are strongly encouraged to open an issue or start a discussion. Your feedback and contributions are highly appreciated!

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache-2.0 License - see the [LICENSE](LICENSE) file for details.
