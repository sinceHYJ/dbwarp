package dbwarp

import (
	"strings"

	"gorm.io/gorm"
)

func (w *Warp) registerCallbacks(db *gorm.DB) {
	w.Callback().Create().Before("*").Register("gorm:sharding", w.switchSource)
	w.Callback().Query().Before("*").Register("gorm:sharding", w.switchReplica)
	w.Callback().Update().Before("*").Register("gorm:sharding", w.switchSource)
	w.Callback().Delete().Before("*").Register("gorm:sharding", w.switchSource)
	w.Callback().Row().Before("*").Register("gorm:sharding", w.switchReplica)
	w.Callback().Raw().Before("*").Register("gorm:sharding", w.switchGuess)
}

func (w *Warp) switchSource(db *gorm.DB) {
	if !isTransaction(db.Statement.ConnPool) {
		_, ok := db.Get(ShardingIgnoreStoreKey)
		if ok {
			return
		}
		table := w.getTableName(db)
		if table == "" {
			return
		}
		warpRule, ok := w.configs[table]
		if !ok {
			return
		}

		w.mutex.Lock()
		if db.Statement.ConnPool != nil {
			db.Statement.ConnPool = &ConnPool{ConnPool: db.Statement.ConnPool, sharding: warpRule, op: Write, stmt: db.Statement}
		}
		w.mutex.Unlock()
	}
}

func (w *Warp) switchReplica(db *gorm.DB) {
	if !isTransaction(db.Statement.ConnPool) {
		_, ok := db.Get(ShardingIgnoreStoreKey)
		if ok {
			return
		}

		table := w.getTableName(db)
		if table == "" {
			return
		}
		warpRule, ok := w.configs[table]
		if !ok {
			return
		}

		// Determine operation type
		var op Operation = Read

		if rawSQL := db.Statement.SQL.String(); len(rawSQL) > 0 {
			w.switchGuess(db)
			return // switchGuess already created ConnPool, return directly
		} else {
			_, locking := db.Statement.Clauses["FOR"]
			if _, ok := db.Statement.Settings.Load(writeName); ok || locking {
				op = Write
			} else {
			}
		}

		w.mutex.Lock()
		if db.Statement.ConnPool != nil {
			db.Statement.ConnPool = &ConnPool{ConnPool: db.Statement.ConnPool, sharding: warpRule, op: op, stmt: db.Statement}
		}
		w.mutex.Unlock()
	}
}

func (w *Warp) switchGuess(db *gorm.DB) {
	if !isTransaction(db.Statement.ConnPool) {
		_, ok := db.Get(ShardingIgnoreStoreKey)
		if ok {
			return
		}

		table := w.getTableName(db)
		if table == "" {
			return
		}
		warpRule, ok := w.configs[table]
		if !ok {
			return
		}

		// Default operation type is write
		var op Operation = Write

		if _, ok := db.Statement.Settings.Load(writeName); ok {
			op = Write
		} else if _, ok := db.Statement.Settings.Load(readName); ok {
			op = Read
		} else if rawSQL := strings.TrimSpace(db.Statement.SQL.String()); len(rawSQL) > 10 && strings.EqualFold(rawSQL[:6], "select") && !strings.EqualFold(rawSQL[len(rawSQL)-10:], "for update") {
			op = Read
		}
		
		w.mutex.Lock()
		if db.Statement.ConnPool != nil {
			
			db.Statement.ConnPool = &ConnPool{ConnPool: db.Statement.ConnPool, sharding: warpRule, op: op, stmt: db.Statement}
		}
		w.mutex.Unlock()
	}
}

func (w *Warp) getTableName(db *gorm.DB) string {
	table := db.Statement.Table

	// If Table is empty, try to parse from Model
	if table == "" && db.Statement.Model != nil {
		stmt := &gorm.Statement{DB: w.DB}
		if err := stmt.Parse(db.Statement.Model); err == nil {
			table = stmt.Table
		}
	}
	// If using raw SQL, table may not be available
	if table == "" {
		table = getTableFromRawSQL(db.Statement.SQL.String())
		// TODO: add logging
	}

	return table
}
