package impl

import (
	"context"
	"fmt"

	"github.com/vincentAlen/mail"
)

func (a *adminServiceImpl) BatchSendPersonal(ctx context.Context, req mail.BatchSendPersonalRequest) (mail.BatchSendPersonalResponse, error) {
	if req.RequestID == "" {
		return mail.BatchSendPersonalResponse{}, mail.NewError(mail.ErrInvalidParam, "requestId is required")
	}
	if len(req.UIDs) == 0 {
		return mail.BatchSendPersonalResponse{}, mail.NewError(mail.ErrInvalidParam, "uids is required")
	}
	if int32(len(req.UIDs)) > a.svc.config.MaxBatchSendSize {
		return mail.BatchSendPersonalResponse{}, mail.Errorf(mail.ErrInvalidParam, "too many uids, max %d", a.svc.config.MaxBatchSendSize)
	}

	var (
		results      []mail.BatchSendResult
		successCount int32
		failCount    int32
	)

	for _, uid := range req.UIDs {
		// Build per-uid dedup key: <requestId>:<uid>
		perReq := mail.SendPersonalRequest{
			ServerID:   req.ServerID,
			RequestID:  fmt.Sprintf("%s:%d", req.RequestID, uid),
			UID:        uid,
			Kind:       req.Kind,
			Source:     req.Source,
			TemplateID: req.TemplateID,
			Params:     req.Params,
			I18nParams: req.I18nParams,
			Title:      req.Title,
			Content:    req.Content,
			Rewards:    req.Rewards,
			SendAtMs:   req.SendAtMs,
			ExpireAtMs: req.ExpireAtMs,
		}

		resp, err := a.svc.SendPersonal(ctx, perReq)
		if err != nil {
			results = append(results, mail.BatchSendResult{
				UID: uid,
				Err: err.Error(),
			})
			failCount++
		} else {
			results = append(results, mail.BatchSendResult{
				UID:    uid,
				MailID: resp.MailID,
			})
			successCount++
		}
	}

	return mail.BatchSendPersonalResponse{
		Results:      results,
		SuccessCount: successCount,
		FailCount:    failCount,
	}, nil
}
