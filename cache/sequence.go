package cache

import (
	"context"
	"fmt"
)

func seqKey(serverID int32) string {
	return fmt.Sprintf("{mail:seq:%d}", serverID)
}

// NextMailID atomically increments and returns the next mail ID for the given server.
func (c *Cache) NextMailID(ctx context.Context, serverID int32) (int64, error) {
	return c.rdb.Incr(ctx, seqKey(serverID)).Result()
}
