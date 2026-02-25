package impl

import (
	"time"

	"github.com/vincentAlen/mail"
)

// timeNowMs returns current time in milliseconds. Extracted for testability.
var timeNowMs = func() int64 {
	return time.Now().UnixMilli()
}

func validateListInbox(req mail.ListInboxRequest, cfg mail.MailConfig) error {
	if req.UID <= 0 {
		return mail.NewError(mail.ErrInvalidParam, "uid is required")
	}
	if req.Limit < 0 {
		return mail.NewError(mail.ErrInvalidParam, "limit must be non-negative")
	}
	if req.Limit > cfg.MaxPageLimit {
		return mail.Errorf(mail.ErrInvalidParam, "limit exceeds max %d", cfg.MaxPageLimit)
	}
	return nil
}

func validateMarkRead(req mail.MarkReadRequest) error {
	if req.UID <= 0 {
		return mail.NewError(mail.ErrInvalidParam, "uid is required")
	}
	if !req.All && len(req.MailIDs) == 0 {
		return mail.NewError(mail.ErrInvalidParam, "mailIDs or all is required")
	}
	return nil
}

func validateClaimRewards(req mail.ClaimRewardsRequest, cfg mail.MailConfig) error {
	if req.UID <= 0 {
		return mail.NewError(mail.ErrInvalidParam, "uid is required")
	}
	if !req.All && len(req.MailIDs) == 0 {
		return mail.NewError(mail.ErrInvalidParam, "mailIDs or all is required")
	}
	if !req.All && int32(len(req.MailIDs)) > cfg.MaxClaimBatchSize {
		return mail.Errorf(mail.ErrInvalidParam, "too many mailIDs, max %d", cfg.MaxClaimBatchSize)
	}
	return nil
}

func validateDeleteMails(req mail.DeleteMailsRequest, cfg mail.MailConfig) error {
	if req.UID <= 0 {
		return mail.NewError(mail.ErrInvalidParam, "uid is required")
	}
	if !req.All && len(req.MailIDs) == 0 {
		return mail.NewError(mail.ErrInvalidParam, "mailIDs or all is required")
	}
	if !req.All && int32(len(req.MailIDs)) > cfg.MaxDeleteBatchSize {
		return mail.Errorf(mail.ErrInvalidParam, "too many mailIDs, max %d", cfg.MaxDeleteBatchSize)
	}
	return nil
}

func validateSendPersonal(req mail.SendPersonalRequest) error {
	if req.UID <= 0 {
		return mail.NewError(mail.ErrInvalidParam, "uid is required")
	}
	if req.RequestID == "" {
		return mail.NewError(mail.ErrInvalidParam, "requestId is required")
	}
	if req.SendAtMs > 0 {
		return mail.NewError(mail.ErrInvalidParam, "sendAtMs is not supported in V1, use 0 for immediate send")
	}
	return nil
}

func validateSendBroadcast(req mail.SendBroadcastRequest) error {
	if req.RequestID == "" {
		return mail.NewError(mail.ErrInvalidParam, "requestId is required")
	}
	if req.Target.Scope == "" {
		return mail.NewError(mail.ErrInvalidParam, "target.scope is required")
	}
	if req.StartAtMs > 0 {
		return mail.NewError(mail.ErrInvalidParam, "startAtMs is not supported in V1, use 0 for immediate activation")
	}
	return nil
}

// resolveExpireAt computes the expireAt timestamp.
func resolveExpireAt(sendAtMs, expireAtMs, defaultTTLMs int64) int64 {
	if expireAtMs > 0 {
		return expireAtMs
	}
	return sendAtMs + defaultTTLMs
}

// resolvePurgeAt computes the purgeAt timestamp.
func resolvePurgeAt(expireAtMs, purgeGraceMs int64) int64 {
	return expireAtMs + purgeGraceMs
}
