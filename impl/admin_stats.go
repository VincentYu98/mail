package impl

import (
	"context"

	"github.com/vincentAlen/mail"
)

func (a *adminServiceImpl) GetBroadcastStats(ctx context.Context, req mail.GetBroadcastStatsRequest) (mail.GetBroadcastStatsResponse, error) {
	if req.MailID <= 0 {
		return mail.GetBroadcastStatsResponse{}, mail.NewError(mail.ErrInvalidParam, "mailId is required")
	}

	// Verify the broadcast exists
	bc, err := a.repo.FindBroadcastMail(ctx, req.ServerID, req.MailID)
	if err != nil {
		return mail.GetBroadcastStatsResponse{}, mail.Errorf(mail.ErrInternal, "find broadcast: %v", err)
	}
	if bc == nil {
		return mail.GetBroadcastStatsResponse{}, mail.NewError(mail.ErrMailNotFound, "broadcast not found")
	}

	delivered, read, claimed, deleted, err := a.repo.CountBroadcastStats(ctx, req.ServerID, req.MailID)
	if err != nil {
		return mail.GetBroadcastStatsResponse{}, mail.Errorf(mail.ErrInternal, "count stats: %v", err)
	}

	return mail.GetBroadcastStatsResponse{
		TotalDelivered: delivered,
		TotalRead:      read,
		TotalClaimed:   claimed,
		TotalDeleted:   deleted,
	}, nil
}
