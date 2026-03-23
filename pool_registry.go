package dbwarp

import (
	"fmt"
	"sync"
	"sync/atomic"

	"gorm.io/gorm"
)

// PoolEntry represents a shared connection pool entry in the registry.
// It holds the actual connection pool, the dialector for potential recreation,
// and a reference count for lifecycle management.
type PoolEntry struct {
	pool      gorm.ConnPool  // The actual connection pool
	dialector gorm.Dialector // Dialector for recreating if needed
	refCount  int32          // Reference count for cleanup (atomic)
}

// PoolRegistry manages shared connection pools across all sharding rules.
// It uses sync.Map for thread-safe concurrent access without explicit locking.
// The key is the normalized DSN string, and the value is *PoolEntry.
type PoolRegistry struct {
	pools sync.Map // key: DSN string, value: *PoolEntry
}

// NewPoolRegistry creates a new PoolRegistry instance.
func NewPoolRegistry() *PoolRegistry {
	return &PoolRegistry{}
}

// extractDSN extracts a normalized DSN string from a Dialector.
// This ensures that different Dialector instances with the same connection
// parameters produce the same DSN string for pool sharing.
func extractDSN(dialector gorm.Dialector) string {
	// Try to get DSN using type assertion for common drivers
	// For mysql.Dialector, check if it has DSNConfig
	if dsnGetter, ok := dialector.(interface{ GetDSN() string }); ok {
		return dsnGetter.GetDSN()
	}

	// Fallback: use string representation
	// This works for most dialector implementations
	return fmt.Sprintf("%T:%v", dialector, dialector)
}

// GetOrCreatePool gets an existing pool from the registry or creates a new one.
// If a pool with the same DSN already exists, it increments the reference count
// and returns the existing pool. Otherwise, it creates a new pool and stores it.
//
// This method is thread-safe and handles race conditions using sync.Map's
// LoadOrStore operation.
func (r *PoolRegistry) GetOrCreatePool(dialector gorm.Dialector, config *gorm.Config) (gorm.ConnPool, error) {
	dsn := extractDSN(dialector)

	// Try to load existing entry
	if value, ok := r.pools.Load(dsn); ok {
		entry := value.(*PoolEntry)
		atomic.AddInt32(&entry.refCount, 1)
		return entry.pool, nil
	}

	// Create new pool
	db, err := gorm.Open(dialector, config)
	if err != nil {
		return nil, err
	}

	entry := &PoolEntry{
		pool:      db.ConnPool,
		dialector: dialector,
		refCount:  1,
	}

	// Store with LoadOrStore to handle race condition
	if actual, loaded := r.pools.LoadOrStore(dsn, entry); loaded {
		// Another goroutine already created the pool
		// Close our newly created pool and use existing one
		if closer, ok := db.ConnPool.(interface{ Close() error }); ok {
			closer.Close()
		}
		existingEntry := actual.(*PoolEntry)
		atomic.AddInt32(&existingEntry.refCount, 1)
		return existingEntry.pool, nil
	}

	return entry.pool, nil
}

// ReleasePool decrements the reference count for a pool and closes it if zero.
// When the reference count reaches zero, the pool is removed from the registry
// and closed to free resources.
//
// This method uses CompareAndDelete to ensure thread-safe removal.
func (r *PoolRegistry) ReleasePool(dsn string) {
	value, ok := r.pools.Load(dsn)
	if !ok {
		return
	}

	entry := value.(*PoolEntry)
	newCount := atomic.AddInt32(&entry.refCount, -1)

	if newCount <= 0 {
		// Try to delete and close the pool
		if r.pools.CompareAndDelete(dsn, entry) {
			// Successfully deleted, close the pool
			if closer, ok := entry.pool.(interface{ Close() error }); ok {
				closer.Close()
			}
		}
	}
}

// GetPoolStats returns statistics about the connection pools in the registry.
// This is useful for monitoring and debugging.
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
