package impl

import (
	"context"

	"github.com/vincentAlen/mail"
)

// matchTarget checks whether a user matches a broadcast target.
// Built-in scopes: "all" (always matches), "server" (matches if same serverId).
// Other scopes are delegated to the TargetResolver port.
func (s *serviceImpl) matchTarget(ctx context.Context, serverID int32, uid int64, target mail.Target) (bool, error) {
	switch target.Scope {
	case "all":
		return true, nil
	case "server":
		// target.Data should be a serverId; broadcast is already scoped by serverId,
		// so matching by server always returns true within the same serverId query.
		return true, nil
	default:
		if s.resolver != nil {
			return s.resolver.Match(ctx, serverID, uid, target)
		}
		// No resolver configured, skip unknown target scopes.
		return false, nil
	}
}
