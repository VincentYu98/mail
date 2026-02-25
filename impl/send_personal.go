package impl

import (
	"context"

	"github.com/vincentAlen/mail"
	"github.com/vincentAlen/mail/repo"
)

func (s *serviceImpl) SendPersonal(ctx context.Context, req mail.SendPersonalRequest) (mail.SendResponse, error) {
	if err := validateSendPersonal(req); err != nil {
		return mail.SendResponse{}, err
	}

	now := nowMs()
	sendAt := now // V1: validation rejects SendAtMs > 0, always immediate send
	expireAt := resolveExpireAt(sendAt, req.ExpireAtMs, s.config.DefaultMailTTLMs)
	purgeAt := resolvePurgeAt(expireAt, s.config.PurgeGraceMs)

	// Dedup check
	scope := "send_personal"
	dedupKey := req.RequestID
	dedupID := repo.FormatDedupID(req.ServerID, scope, dedupKey)

	dedupDoc := repo.MailDedupDoc{
		ID:        dedupID,
		ServerID:  req.ServerID,
		Scope:     scope,
		DedupKey:  dedupKey,
		RequestID: req.RequestID,
		CreatedAt: now,
		PurgeAt:   purgeAt,
	}

	result, err := s.repo.TryInsertDedup(ctx, dedupDoc)
	if err != nil {
		return mail.SendResponse{}, mail.Errorf(mail.ErrInternal, "dedup: %v", err)
	}
	if result.Duplicate {
		return mail.SendResponse{MailID: result.ResultMailID}, nil
	}

	// Allocate mailId
	mailID, err := s.cache.NextMailID(ctx, req.ServerID)
	if err != nil {
		return mail.SendResponse{}, mail.Errorf(mail.ErrInternal, "alloc mailId: %v", err)
	}

	// Build user mail doc
	doc := repo.UserMailDoc{
		ServerID:   req.ServerID,
		UID:        req.UID,
		MailID:     mailID,
		Kind:       string(req.Kind),
		Source:     req.Source,
		TemplateID: req.TemplateID,
		Params:     req.Params,
		I18nParams: repo.MarshalI18nParams(req.I18nParams),
		Title:      req.Title,
		Content:    req.Content,
		Rewards:    repo.RewardsToDocs(req.Rewards),
		SendAt:     sendAt,
		ExpireAt:   expireAt,
		PurgeAt:    purgeAt,
	}

	if err := s.repo.InsertUserMail(ctx, doc); err != nil {
		return mail.SendResponse{}, mail.Errorf(mail.ErrInternal, "insert user mail: %v", err)
	}

	// Mark dedup as done with the assigned mailId
	_ = s.repo.CompleteDedupStatus(ctx, dedupID, mailID)

	// Update unread count
	_ = s.cache.IncrUnread(ctx, req.ServerID, req.UID, 1)

	// Optional push notification
	if s.push != nil {
		_ = s.push.NotifyUser(ctx, req.ServerID, req.UID, "new_mail", nil)
	}

	return mail.SendResponse{MailID: mailID}, nil
}
