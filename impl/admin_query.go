package impl

import (
	"context"
	"fmt"

	"github.com/vincentAlen/mail"
)

func (a *adminServiceImpl) QueryByRequestId(ctx context.Context, req mail.QueryByRequestIdRequest) (mail.QueryByRequestIdResponse, error) {
	if req.RequestID == "" {
		return mail.QueryByRequestIdResponse{}, mail.NewError(mail.ErrInvalidParam, "requestId is required")
	}
	if req.Scope == "" {
		return mail.QueryByRequestIdResponse{}, mail.NewError(mail.ErrInvalidParam, "scope is required")
	}

	// Build dedupKey based on scope
	var dedupKey string
	switch req.Scope {
	case "send_personal", "send_broadcast":
		dedupKey = req.RequestID
	case "batch_send_personal":
		if req.UID <= 0 {
			return mail.QueryByRequestIdResponse{}, mail.NewError(mail.ErrInvalidParam, "uid is required for batch_send_personal scope")
		}
		dedupKey = fmt.Sprintf("%s:%d", req.RequestID, req.UID)
	default:
		return mail.QueryByRequestIdResponse{}, mail.Errorf(mail.ErrInvalidParam, "unknown scope: %s", req.Scope)
	}

	doc, err := a.repo.FindDedup(ctx, req.ServerID, req.Scope, dedupKey)
	if err != nil {
		return mail.QueryByRequestIdResponse{}, mail.Errorf(mail.ErrInternal, "query dedup: %v", err)
	}

	if doc == nil {
		return mail.QueryByRequestIdResponse{
			Found:  false,
			Status: "not_found",
		}, nil
	}

	resp := mail.QueryByRequestIdResponse{
		Found:  true,
		Scope:  doc.Scope,
		MailID: doc.ResultMailID,
	}

	resp.Status = doc.Status
	if resp.Status == "" {
		// Backward compatibility for legacy records without status field
		if doc.ResultMailID > 0 {
			resp.Status = "done"
		} else {
			resp.Status = "pending"
		}
	}
	if resp.Status == "done" {
		// Determine mail kind from scope
		switch doc.Scope {
		case "send_broadcast":
			resp.MailKind = mail.MailKindBroadcast
		default:
			resp.MailKind = mail.MailKindPersonal
		}
	}

	// For personal mails, try to get the actual kind from the mail doc
	if doc.ResultMailID > 0 && doc.Scope != "send_broadcast" {
		// Look up the user mail to get its kind
		if req.UID > 0 {
			docs, findErr := a.repo.FindUserMailsByIDs(ctx, req.ServerID, req.UID, []int64{doc.ResultMailID})
			if findErr == nil && len(docs) > 0 {
				resp.MailKind = mail.MailKind(docs[0].Kind)
			}
		}
	}

	return resp, nil
}
