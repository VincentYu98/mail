package mail

import "context"

// RewardGranter grants rewards to a user. Must be idempotent for the given idempotencyKey.
type RewardGranter interface {
	Grant(ctx context.Context, serverID int32, uid int64, items []RewardItem, idempotencyKey string) error
}

// Locker provides distributed user-level locking.
type Locker interface {
	WithUserLock(ctx context.Context, serverID int32, uid int64, fn func(context.Context) error) error
}

// PushNotifier sends real-time push notifications (optional).
type PushNotifier interface {
	NotifyUser(ctx context.Context, serverID int32, uid int64, event string, payload any) error
	NotifyServer(ctx context.Context, serverID int32, event string, payload any) error
}

// TargetResolver determines whether a user matches a broadcast target (optional).
// Match must be stable: once a user does not match, they must never match later
// for the same broadcast (cursor safety). Dynamic rules must be materialized to uid_list.
type TargetResolver interface {
	Match(ctx context.Context, serverID int32, uid int64, target Target) (bool, error)
}
