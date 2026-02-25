package impl

import (
	"context"

	"github.com/vincentAlen/mail"
)

func (s *serviceImpl) MarkRead(ctx context.Context, req mail.MarkReadRequest) (mail.MarkReadResponse, error) {
	if err := validateMarkRead(req); err != nil {
		return mail.MarkReadResponse{}, err
	}

	now := nowMs()

	var updatedIDs []int64
	var err error

	if req.All {
		updatedIDs, err = s.repo.MarkReadAll(ctx, req.ServerID, req.UID, now)
	} else {
		updatedIDs, err = s.repo.MarkRead(ctx, req.ServerID, req.UID, req.MailIDs, now)
	}
	if err != nil {
		return mail.MarkReadResponse{}, mail.Errorf(mail.ErrInternal, "mark read: %v", err)
	}

	// Decrement unread count
	if len(updatedIDs) > 0 {
		_ = s.cache.IncrUnread(ctx, req.ServerID, req.UID, -int64(len(updatedIDs)))
	}

	return mail.MarkReadResponse{UpdatedMailIDs: updatedIDs}, nil
}
