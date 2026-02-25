package impl

import (
	"context"

	"github.com/vincentAlen/mail"
	"github.com/vincentAlen/mail/repo"
)

func (s *serviceImpl) ListInbox(ctx context.Context, req mail.ListInboxRequest) (mail.ListInboxResponse, error) {
	if err := validateListInbox(req, s.config); err != nil {
		return mail.ListInboxResponse{}, err
	}

	now := nowMs()
	limit := req.Limit
	if limit <= 0 {
		limit = s.config.DefaultPageLimit
	}

	// Sync broadcasts before listing
	if err := s.syncBroadcasts(ctx, req.ServerID, req.UID, now); err != nil {
		// Log but don't fail the request — user can still see existing mails
		_ = err
	}

	// Query user_mails with pagination
	docs, err := s.repo.ListUserMails(ctx, req.ServerID, req.UID, req.BeforeMailID, limit+1, now)
	if err != nil {
		return mail.ListInboxResponse{}, mail.Errorf(mail.ErrInternal, "list mails: %v", err)
	}

	var nextCursor int64
	if int32(len(docs)) > limit {
		docs = docs[:limit]
		nextCursor = docs[len(docs)-1].MailID
	}

	mails := make([]mail.Mail, len(docs))
	for i, doc := range docs {
		mails[i] = repo.UserMailDocToMail(doc)
	}

	return mail.ListInboxResponse{
		Mails:      mails,
		NextCursor: nextCursor,
	}, nil
}

// syncBroadcasts performs the broadcast catch-up ("补齐") for a user.
func (s *serviceImpl) syncBroadcasts(ctx context.Context, serverID int32, uid int64, now int64) error {
	// Step 1: Get user's current cursor
	cursor, err := s.repo.GetBroadcastCursor(ctx, serverID, uid)
	if err != nil {
		return err
	}

	// Step 2: Fast path — check if there are new broadcasts
	latest, err := s.cache.GetBroadcastLatest(ctx, serverID)
	if err != nil {
		// Redis failure: fall through to MongoDB scan
		latest = 0
	}
	if latest > 0 && cursor >= latest {
		return nil // nothing new
	}

	// Step 3: Scan broadcast_mails after cursor
	broadcasts, err := s.repo.FindBroadcastsAfterCursor(ctx, serverID, cursor, s.config.BroadcastSyncBatchSize)
	if err != nil {
		return err
	}
	if len(broadcasts) == 0 {
		return nil
	}

	// Step 4: Filter by target and build user mail docs
	var toUpsert []repo.UserMailDoc
	var newCursor int64
	for _, bc := range broadcasts {
		// Skip expired broadcasts
		if bc.ExpireAt <= now {
			newCursor = bc.MailID
			continue
		}

		matched, err := s.matchTarget(ctx, serverID, uid, repo.DocToTarget(bc.Target))
		if err != nil {
			// Stop at failure point — don't advance cursor past this
			break
		}
		if matched {
			doc := repo.BroadcastDocToUserMailDoc(bc, uid)
			toUpsert = append(toUpsert, doc)
		}
		newCursor = bc.MailID
	}

	// Step 5: Bulk upsert matched broadcasts into user_mails
	if len(toUpsert) > 0 {
		inserted, err := s.repo.BulkUpsertUserMails(ctx, toUpsert)
		if err != nil {
			return err
		}
		// Update unread count for newly inserted mails
		if inserted > 0 {
			_ = s.cache.IncrUnread(ctx, serverID, uid, inserted)
		}
	}

	// Step 6: Advance cursor (only forward, $max semantic)
	if newCursor > cursor {
		_ = s.repo.UpdateBroadcastCursor(ctx, serverID, uid, newCursor, now)
	}

	return nil
}
