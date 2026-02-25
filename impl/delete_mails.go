package impl

import (
	"context"

	"github.com/vincentAlen/mail"
)

func (s *serviceImpl) DeleteMails(ctx context.Context, req mail.DeleteMailsRequest) (mail.DeleteMailsResponse, error) {
	if err := validateDeleteMails(req, s.config); err != nil {
		return mail.DeleteMailsResponse{}, err
	}

	now := nowMs()

	var deletedIDs []int64
	var unreadDeleted int64
	var err error

	if req.All {
		deletedIDs, unreadDeleted, err = s.repo.SoftDeleteAllMails(ctx, req.ServerID, req.UID, now)
	} else {
		deletedIDs, unreadDeleted, err = s.repo.SoftDeleteMails(ctx, req.ServerID, req.UID, req.MailIDs, now)
	}
	if err != nil {
		return mail.DeleteMailsResponse{}, mail.Errorf(mail.ErrInternal, "delete mails: %v", err)
	}

	// Decrement unread count for deleted unread mails
	if unreadDeleted > 0 {
		_ = s.cache.IncrUnread(ctx, req.ServerID, req.UID, -unreadDeleted)
	}

	return mail.DeleteMailsResponse{DeletedMailIDs: deletedIDs}, nil
}
