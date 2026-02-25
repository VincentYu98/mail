package impl

import (
	"context"

	"github.com/vincentAlen/mail"
)

func (s *serviceImpl) GetUnreadCount(ctx context.Context, req mail.GetUnreadCountRequest) (mail.GetUnreadCountResponse, error) {
	if req.UID <= 0 {
		return mail.GetUnreadCountResponse{}, mail.NewError(mail.ErrInvalidParam, "uid is required")
	}

	now := nowMs()

	// Get cached unread count
	unread, err := s.cache.GetUnread(ctx, req.ServerID, req.UID)
	if err != nil {
		// Fallback to MongoDB
		unread, err = s.repo.CountUnread(ctx, req.ServerID, req.UID, now)
		if err != nil {
			return mail.GetUnreadCountResponse{}, mail.Errorf(mail.ErrInternal, "count unread: %v", err)
		}
		// Repair cache
		_ = s.cache.SetUnread(ctx, req.ServerID, req.UID, unread)
	}

	// Clamp to 0
	if unread < 0 {
		unread = 0
	}

	// Check for claimable
	hasClaimable, err := s.repo.HasClaimable(ctx, req.ServerID, req.UID, now)
	if err != nil {
		hasClaimable = false
	}

	return mail.GetUnreadCountResponse{
		UnreadCount:  unread,
		HasClaimable: hasClaimable,
	}, nil
}

// ReconcileUnread recalculates the unread count from MongoDB and updates Redis.
func (s *serviceImpl) ReconcileUnread(ctx context.Context, serverID int32, uid int64) error {
	now := nowMs()
	count, err := s.repo.CountUnread(ctx, serverID, uid, now)
	if err != nil {
		return err
	}
	return s.cache.SetUnread(ctx, serverID, uid, count)
}
