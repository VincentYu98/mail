package cache

import "github.com/redis/go-redis/v9"

// Cache provides Redis caching operations for the mail module.
type Cache struct {
	rdb redis.Cmdable
}

// NewCache creates a Cache from a Redis client (supports both Client and ClusterClient).
func NewCache(rdb redis.Cmdable) *Cache {
	return &Cache{rdb: rdb}
}
