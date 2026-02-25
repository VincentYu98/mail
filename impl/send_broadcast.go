package impl

import (
	"context"

	"github.com/vincentAlen/mail"
	"github.com/vincentAlen/mail/repo"
)

func (s *serviceImpl) SendBroadcast(ctx context.Context, req mail.SendBroadcastRequest) (mail.SendResponse, error) {
	if err := validateSendBroadcast(req); err != nil {
		return mail.SendResponse{}, err
	}

	now := nowMs()
	sendAt := req.StartAtMs
	if sendAt <= 0 {
		sendAt = now
	}
	expireAt := resolveExpireAt(sendAt, req.ExpireAtMs, s.config.DefaultMailTTLMs)
	purgeAt := resolvePurgeAt(expireAt, s.config.PurgeGraceMs)

	// Dedup check
	scope := "send_broadcast"
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

	// Build broadcast mail doc
	doc := repo.BroadcastMailDoc{
		ServerID:   req.ServerID,
		MailID:     mailID,
		Target:     repo.TargetToDoc(req.Target),
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

	if err := s.repo.InsertBroadcastMail(ctx, doc); err != nil {
		return mail.SendResponse{}, mail.Errorf(mail.ErrInternal, "insert broadcast: %v", err)
	}

	// Update dedup with assigned mailId
	_ = s.repo.UpdateDedupResultMailID(ctx, dedupID, mailID)

	// Update broadcast latest in cache
	_ = s.cache.SetBroadcastLatest(ctx, req.ServerID, mailID)

	// Optional push notification
	if s.push != nil {
		_ = s.push.NotifyServer(ctx, req.ServerID, "new_broadcast", nil)
	}

	return mail.SendResponse{MailID: mailID}, nil
}
