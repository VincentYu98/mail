package cache

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

func unreadKey(serverID int32) string {
	return fmt.Sprintf("{mail:unread:%d}", serverID)
}

func uidField(uid int64) string {
	return strconv.FormatInt(uid, 10)
}

// IncrUnread increments the unread count for a user by delta.
func (c *Cache) IncrUnread(ctx context.Context, serverID int32, uid int64, delta int64) error {
	return c.rdb.HIncrBy(ctx, unreadKey(serverID), uidField(uid), delta).Err()
}

// GetUnread returns the cached unread count. Returns 0 if not found.
func (c *Cache) GetUnread(ctx context.Context, serverID int32, uid int64) (int64, error) {
	val, err := c.rdb.HGet(ctx, unreadKey(serverID), uidField(uid)).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(val, 10, 64)
}

// SetUnread sets the unread count to an exact value (used for reconciliation).
func (c *Cache) SetUnread(ctx context.Context, serverID int32, uid int64, count int64) error {
	return c.rdb.HSet(ctx, unreadKey(serverID), uidField(uid), count).Err()
}
