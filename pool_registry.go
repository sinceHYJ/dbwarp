package dbwarp

import (
	"fmt"
	"sync"
	"sync/atomic"

	"gorm.io/gorm"
)

// PoolEntry represents a shared connection pool entry in the registry.
type PoolEntry struct {
	pool     gorm.ConnPool
	refCount int32
}

// PoolRegistry manages shared connection pools across all sharding rules.
type PoolRegistry struct {
	pools sync.Map
	mu    sync.Mutex
}

// NewPoolRegistry creates a new PoolRegistry instance.
func NewPoolRegistry() *PoolRegistry {
	return &PoolRegistry{}
}

// extractDSN extracts a normalized DSN string from a Dialector.
func extractDSN(dialector gorm.Dialector) string {
	if dsnGetter, ok := dialector.(interface{ GetDSN() string }); ok {
		return dsnGetter.GetDSN()
	}
	return fmt.Sprintf("%T:%v", dialector, dialector)
}

// closePool safely closes a connection pool if it implements io.Closer.
func closePool(pool gorm.ConnPool) {
	if closer, ok := pool.(interface{ Close() error }); ok {
		closer.Close()
	}
}

// GetOrCreatePool gets an existing pool or creates a new one.
func (r *PoolRegistry) GetOrCreatePool(dialector gorm.Dialector, config *gorm.Config) (gorm.ConnPool, error) {
	dsn := extractDSN(dialector)

	// Fast path: check if pool already exists
	if value, ok := r.pools.Load(dsn); ok {
		entry := value.(*PoolEntry)
		atomic.AddInt32(&entry.refCount, 1)
		return entry.pool, nil
	}

	// Slow path: create new pool with mutex protection
	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check after acquiring lock
	if value, ok := r.pools.Load(dsn); ok {
		entry := value.(*PoolEntry)
		atomic.AddInt32(&entry.refCount, 1)
		return entry.pool, nil
	}

	db, err := gorm.Open(dialector, config)
	if err != nil {
		return nil, err
	}

	connPool := db.ConnPool
	if preparedStmtDB, ok := connPool.(*gorm.PreparedStmtDB); ok {
		connPool = preparedStmtDB.ConnPool
	}

	entry := &PoolEntry{
		pool:     connPool,
		refCount: 1,
	}
	r.pools.Store(dsn, entry)

	return entry.pool, nil
}

// ReleasePool decrements the reference count for a pool and closes it if zero.
func (r *PoolRegistry) ReleasePool(dsn string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	value, ok := r.pools.Load(dsn)
	if !ok {
		return
	}

	entry := value.(*PoolEntry)
	newCount := atomic.AddInt32(&entry.refCount, -1)

	if newCount <= 0 {
		r.pools.Delete(dsn)
		closePool(entry.pool)
	}
}

// GetPoolStats returns statistics about the connection pools in the registry.
func (r *PoolRegistry) GetPoolStats() map[string]int32 {
	stats := make(map[string]int32)
	r.pools.Range(func(key, value interface{}) bool {
		dsn := key.(string)
		entry := value.(*PoolEntry)
		stats[dsn] = atomic.LoadInt32(&entry.refCount)
		return true
	})
	return stats
}

// GetPoolCount returns the number of unique connection pools in the registry.
func (r *PoolRegistry) GetPoolCount() int {
	count := 0
	r.pools.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}
