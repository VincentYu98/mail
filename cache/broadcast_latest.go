package cache

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

func broadcastLatestKey(serverID int32) string {
	return fmt.Sprintf("{broadcast:latest:%d}", serverID)
}

// GetBroadcastLatest returns the latest broadcast mailID for a server.
// Returns 0 if not set.
func (c *Cache) GetBroadcastLatest(ctx context.Context, serverID int32) (int64, error) {
	val, err := c.rdb.Get(ctx, broadcastLatestKey(serverID)).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(val, 10, 64)
}

// SetBroadcastLatest sets the latest broadcast mailID for a server.
func (c *Cache) SetBroadcastLatest(ctx context.Context, serverID int32, mailID int64) error {
	return c.rdb.Set(ctx, broadcastLatestKey(serverID), mailID, 0).Err()
}
