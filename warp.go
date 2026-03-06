package dbwarp

import (
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"sync"
	"sync/atomic"

	"gorm.io/gorm"
)

type Warp struct {
	*gorm.DB

	// key: table, value: specific rule
	configs map[string]*WarpItem

	_rules []*ShardingRule

	mutex sync.RWMutex
}

type WarpItem struct {
	resolvers []*DBResolver // Multiple databases scenario, indexed by RouterCfg
	querys    sync.Map

	rule *ShardingRule
	w    *Warp
}

// ShardingRule defines a sharding rule based on a ShardingKey for database/table sharding
type ShardingRule struct {
	// ShardingKey specifies the table column you want to used for sharding the table rows.
	// For example, for a product order table, you may want to split the rows by `user_id`.
	ShardingKey string

	// TableNumberOfShards specifies the number of table shards to distribute data across.
	TableNumberOfShards uint
	// DatabaseNumberOfShards specifies the number of database shards to distribute data across.
	DatabaseNumberOfShards uint

	// ShardingTableAlgorithm is a custom function to determine the table suffix based on the sharding key value.
	// If not provided, a default algorithm using modulo will be used.
	ShardingTableAlgorithm func(columnValue any) (suffix string, err error)

	// ShardingDatabaseAlgorithm is a custom function to determine the database suffix based on the sharding key value.
	// If not provided and DatabaseNumberOfShards > 0, a default algorithm using modulo will be used.
	ShardingDatabaseAlgorithm func(columnValue any) (suffix string, err error)

	// RouterCfgs defines the sharding key, rules, and multiple databases
	RouterCfgs []RouterCfg

	// format specifies the sharding table suffix format.
	tableformat, dbFormat string
	_tables               []any
}

// RouterCfg defines a router configuration for each database, including master, replica, and routing rules
// Database sharding applies to specific tables as a whole unit
type RouterCfg struct {
	// Name is the database name
	Name string

	Sources      []gorm.Dialector
	Replicas     []gorm.Dialector
	ReplPolicy   Policy
	SourcePolicy Policy

	datas             []interface{}
	TraceResolverMode bool
}

// New creates a new Warp instance
func New() *Warp {
	return &Warp{
		_rules: make([]*ShardingRule, 0),
	}
}

// AddRule adds a sharding rule, supports method chaining
func (w *Warp) AddRule(rule *ShardingRule, tables ...any) *Warp {
	rule._tables = tables
	w._rules = append(w._rules, rule)
	return w
}

// AddRouter adds a router configuration to the sharding rule
func (r *ShardingRule) AddRouter(config RouterCfg, datas ...interface{}) *ShardingRule {
	config.datas = datas
	r.RouterCfgs = append(r.RouterCfgs, config)
	return r
}

func (w *Warp) Name() string {
	return "gorm:db_warp"
}

func (w *Warp) Initialize(db *gorm.DB) error {
	var err error
	w.DB = db
	w.registerCallbacks(db)
	if w.configs == nil {
		w.configs = make(map[string]*WarpItem)
	}
	for _, r := range w._rules {
		// Create independent DBResolver for each RouterCfg (database sharding scenario)
		var resolvers []*DBResolver
		for _, cfg := range r.RouterCfgs {
			dr := DBResolver{
				config: cfg,
				DB:     db,
			}
			if atomic.SwapInt32(&dr.once, 1) == 0 {
				if err = dr.compile(); err != nil {
					return err
				}
			}
			resolvers = append(resolvers, &dr)
		}

		warpItem := &WarpItem{
			rule: r,
			// _resolver: defaultResolver,
			resolvers: resolvers,
			w:         w,
		}

		if r.ShardingTableAlgorithm == nil {
			if r.tableformat, err = formatByNumOfShards(int(r.TableNumberOfShards)); err != nil {
				return err
			}
			r.ShardingTableAlgorithm = w.defaultShardingAlgorithm(r.tableformat, r.TableNumberOfShards)
		}

		if r.ShardingDatabaseAlgorithm == nil && r.DatabaseNumberOfShards > 0 {
			if r.dbFormat, err = formatByNumOfShards(int(r.DatabaseNumberOfShards)); err != nil {
				return err
			}
			r.ShardingDatabaseAlgorithm = w.defaultShardingAlgorithm(r.dbFormat, r.DatabaseNumberOfShards)
		}

		for _, table := range r._tables {
			if t, ok := table.(string); ok {
				w.configs[t] = warpItem
			} else {
				stmt := &gorm.Statement{DB: w.DB}
				if err := stmt.Parse(table); err == nil {
					w.configs[stmt.Table] = warpItem
				} else {
					return err
				}
			}
		}
	}

	return nil
}

// defaultShardingAlgorithm creates a default sharding algorithm
func (w *Warp) defaultShardingAlgorithm(format string, numOfShards uint) func(any) (string, error) {
	return func(value any) (suffix string, err error) {
		id := 0
		switch v := value.(type) {
		case int:
			id = v
		case int64:
			id = int(v)
		case string:
			id, err = strconv.Atoi(v)
			if err != nil {
				id = int(crc32.ChecksumIEEE([]byte(v)))
			}
		default:
			return "", fmt.Errorf("default algorithm only support integer and string column, " +
				"if you use other type, specify your own ShardingAlgorithm")
		}
		return fmt.Sprintf(format, id%int(numOfShards)), nil
	}
}

func formatByNumOfShards(numOfShards int) (format string, err error) {
	if numOfShards == 0 {
		return "", errors.New("specify NumberOfShards or ShardingAlgorithm")
	}
	if numOfShards < 10 {
		format = "_%01d"
	} else if numOfShards < 100 {
		format = "_%02d"
	} else if numOfShards < 1000 {
		format = "_%03d"
	} else if numOfShards < 10000 {
		format = "_%04d"
	}
	return
}

// NoShardingAlgorithm returns an empty sharding algorithm that performs no sharding.
// Use this when you want to disable sharding for a specific dimension (table or database).
func NoShardingAlgorithm() func(columnValue any) (suffix string, err error) {
	return func(columnValue any) (suffix string, err error) {
		return "", nil
	}
}

func isTransaction(connPool gorm.ConnPool) bool {
	_, ok := connPool.(gorm.TxCommitter)
	return ok
}
