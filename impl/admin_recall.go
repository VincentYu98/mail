package impl

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/vincentAlen/mail"
)

func (a *adminServiceImpl) RecallBroadcast(ctx context.Context, req mail.RecallBroadcastRequest) error {
	if req.MailID <= 0 {
		return mail.NewError(mail.ErrInvalidParam, "mailId is required")
	}
	if req.Operator == "" {
		return mail.NewError(mail.ErrInvalidParam, "operator is required")
	}

	now := nowMs()

	// Mark the broadcast as recalled
	err := a.repo.RecallBroadcast(ctx, req.ServerID, req.MailID, req.Operator, now)
	if err == mongo.ErrNoDocuments {
		return mail.NewError(mail.ErrMailNotFound, "broadcast not found or already recalled")
	}
	if err != nil {
		return mail.Errorf(mail.ErrInternal, "recall broadcast: %v", err)
	}

	// Soft-delete already-delivered, unclaimed user mails from this broadcast
	_, _ = a.repo.SoftDeleteRecalledBroadcastMails(ctx, req.ServerID, req.MailID, now)

	return nil
}
