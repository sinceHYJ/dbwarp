package dbwarp

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
)

type Operation string

const (
	Write Operation = "write"
	Read  Operation = "read"
)

type DBResolver struct {
	*gorm.DB

	config           RouterCfg
	resolvers        map[string]*resolver
	global           *resolver
	prepareStmtStore map[gorm.ConnPool]*gorm.PreparedStmtDB
	compileCallbacks []func(gorm.ConnPool) error
	once             int32
}

type resolver struct {
	sources           []gorm.ConnPool
	replicas          []gorm.ConnPool
	replPolicy        Policy
	sourcePolicy      Policy
	dbResolver        *DBResolver
	traceResolverMode bool
}

func (r *resolver) call(fc func(connPool gorm.ConnPool) error) error {
	for _, s := range r.sources {
		if err := fc(s); err != nil {
			return err
		}
	}

	for _, re := range r.replicas {
		if err := fc(re); err != nil {
			return err
		}
	}
	return nil
}

func (r *resolver) resolve(stmt *gorm.Statement, op Operation) (connPool gorm.ConnPool) {
	connPool = r.selectPool(op)

	if r.traceResolverMode && stmt != nil {
		if op == Read {
			markStmtResolverMode(stmt, ResolverModeReplica)
		} else {
			markStmtResolverMode(stmt, ResolverModeSource)
		}
	}

	if stmt != nil && stmt.DB != nil && stmt.DB.PrepareStmt {
		if preparedStmt, ok := r.dbResolver.prepareStmtStore[connPool]; ok {
			return &gorm.PreparedStmtDB{
				ConnPool: connPool,
				Mux:      preparedStmt.Mux,
				Stmts:    preparedStmt.Stmts,
			}
		}
	}

	return
}

// selectPool 根据操作类型选择连接池（不依赖 stmt）
func (r *resolver) selectPool(op Operation) gorm.ConnPool {
	if op == Read {
		if len(r.replicas) == 1 {
			return r.replicas[0]
		}
		return r.replPolicy.Resolve(r.replicas)
	}

	if len(r.sources) == 1 {
		return r.sources[0]
	}
	return r.sourcePolicy.Resolve(r.sources)
}

func (dr *DBResolver) resolve(stmt *gorm.Statement, op Operation) gorm.ConnPool {
	if r := dr.getResolver(stmt); r != nil {
		return r.resolve(stmt, op)
	}
	return stmt.ConnPool
}

func (dr *DBResolver) getResolver(stmt *gorm.Statement) *resolver {
	if len(dr.resolvers) > 0 {
		if u, ok := stmt.Clauses[usingName].Expression.(using); ok && u.Use != "" {
			if r, ok := dr.resolvers[u.Use]; ok {
				return r
			}
		}

		if stmt.Table != "" {
			if r, ok := dr.resolvers[stmt.Table]; ok {
				return r
			}
		}

		if stmt.Model != nil {
			if err := stmt.Parse(stmt.Model); err == nil {
				if r, ok := dr.resolvers[stmt.Table]; ok {
					return r
				}
			}
		}

		if stmt.Schema != nil {
			if r, ok := dr.resolvers[stmt.Schema.Table]; ok {
				return r
			}
		}

		if rawSQL := stmt.SQL.String(); rawSQL != "" {
			if r, ok := dr.resolvers[getTableFromRawSQL(rawSQL)]; ok {
				return r
			}
		}
	}

	return dr.global
}

func (dr *DBResolver) SetConnMaxIdleTime(d time.Duration) *DBResolver {
	dr.Call(func(connPool gorm.ConnPool) error {
		if conn, ok := connPool.(interface{ SetConnMaxIdleTime(time.Duration) }); ok {
			conn.SetConnMaxIdleTime(d)
		} else {
			dr.DB.Logger.Error(context.Background(), "SetConnMaxIdleTime not implemented for %#v, please use golang v1.15+", conn)
		}
		return nil
	})

	return dr
}

func (dr *DBResolver) SetConnMaxLifetime(d time.Duration) *DBResolver {
	dr.Call(func(connPool gorm.ConnPool) error {
		if conn, ok := connPool.(interface{ SetConnMaxLifetime(time.Duration) }); ok {
			conn.SetConnMaxLifetime(d)
		} else {
			dr.DB.Logger.Error(context.Background(), "SetConnMaxLifetime not implemented for %#v", conn)
		}
		return nil
	})

	return dr
}

func (dr *DBResolver) SetMaxIdleConns(n int) *DBResolver {
	dr.Call(func(connPool gorm.ConnPool) error {
		if conn, ok := connPool.(interface{ SetMaxIdleConns(int) }); ok {
			conn.SetMaxIdleConns(n)
		} else {
			dr.DB.Logger.Error(context.Background(), "SetMaxIdleConns not implemented for %#v", conn)
		}
		return nil
	})

	return dr
}

func (dr *DBResolver) SetMaxOpenConns(n int) *DBResolver {
	dr.Call(func(connPool gorm.ConnPool) error {

		if conn, ok := connPool.(interface{ SetMaxOpenConns(int) }); ok {
			conn.SetMaxOpenConns(n)
		} else {
			dr.DB.Logger.Error(context.Background(), "SetMaxOpenConns not implemented for %#v", conn)
		}
		return nil
	})

	return dr
}

func (dr *DBResolver) Call(fc func(connPool gorm.ConnPool) error) error {
	if dr.DB != nil {
		for _, r := range dr.resolvers {
			if err := r.call(fc); err != nil {
				return err
			}
		}

		if dr.global != nil {
			if err := dr.global.call(fc); err != nil {
				return err
			}
		}
	} else {
		dr.compileCallbacks = append(dr.compileCallbacks, fc)
	}

	return nil
}

func (dr *DBResolver) compile() error {
	if dr.prepareStmtStore == nil {
		dr.prepareStmtStore = map[gorm.ConnPool]*gorm.PreparedStmtDB{}
	}

	if dr.resolvers == nil {
		dr.resolvers = map[string]*resolver{}
	}

	// for _, config := range dr.configs {
	if dr.config.ReplPolicy == nil {
		dr.config.ReplPolicy = RandomPolicy{}
	}

	if dr.config.SourcePolicy == nil {
		dr.config.SourcePolicy = RandomPolicy{}
	}
	err := dr.compileConfig(dr.config)
	if err != nil {
		return err
	}
	// }
	return nil
}

func (dr *DBResolver) compileConfig(config RouterCfg) (err error) {
	var (
		connPool = dr.DB.Config.ConnPool
		r        = resolver{
			dbResolver:        dr,
			replPolicy:        config.ReplPolicy,
			sourcePolicy:      config.SourcePolicy,
			traceResolverMode: config.TraceResolverMode,
		}
	)

	if preparedStmtDB, ok := connPool.(*gorm.PreparedStmtDB); ok {
		connPool = preparedStmtDB.ConnPool
	}

	if len(config.Sources) == 0 {
		r.sources = []gorm.ConnPool{connPool}
		dr.prepareStmtStore[connPool] = gorm.NewPreparedStmtDB(connPool, dr.PrepareStmtMaxSize, dr.PrepareStmtTTL)
	} else if r.sources, err = dr.convertToConnPool(config.Sources); err != nil {
		return err
	}

	if len(config.Replicas) == 0 {
		r.replicas = r.sources
	} else if r.replicas, err = dr.convertToConnPool(config.Replicas); err != nil {
		return err
	}

	if len(config.datas) > 0 {
		for _, data := range config.datas {
			if t, ok := data.(string); ok {
				dr.resolvers[t] = &r
			} else {
				stmt := &gorm.Statement{DB: dr.DB}
				if err := stmt.Parse(data); err == nil {
					dr.resolvers[stmt.Table] = &r
				} else {
					return err
				}
			}
		}
	} else if dr.global == nil {
		dr.global = &r
	} else {
		return errors.New("conflicted global resolver")
	}

	for _, fc := range dr.compileCallbacks {
		if err = r.call(fc); err != nil {
			return err
		}
	}

	if config.TraceResolverMode {
		dr.Logger = NewResolverModeLogger(dr.Logger)
	}

	return nil
}

func (dr *DBResolver) convertToConnPool(dialectors []gorm.Dialector) (connPools []gorm.ConnPool, err error) {
	config := *dr.DB.Config
	for _, dialector := range dialectors {
		if db, err := gorm.Open(dialector, &config); err == nil {
			connPool := db.ConnPool
			if preparedStmtDB, ok := connPool.(*gorm.PreparedStmtDB); ok {
				connPool = preparedStmtDB.ConnPool
			}

			dr.prepareStmtStore[connPool] = gorm.NewPreparedStmtDB(db.ConnPool, dr.PrepareStmtMaxSize, dr.PrepareStmtTTL)

			connPools = append(connPools, connPool)
		} else {
			return nil, err
		}
	}

	return connPools, err
}
