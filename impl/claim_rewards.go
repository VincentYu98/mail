package impl

import (
	"context"
	"fmt"

	"github.com/vincentAlen/mail"
	"github.com/vincentAlen/mail/repo"
)

func (s *serviceImpl) ClaimRewards(ctx context.Context, req mail.ClaimRewardsRequest) (mail.ClaimRewardsResponse, error) {
	if err := validateClaimRewards(req, s.config); err != nil {
		return mail.ClaimRewardsResponse{}, err
	}

	var resp mail.ClaimRewardsResponse

	// Must acquire user lock
	err := s.locker.WithUserLock(ctx, req.ServerID, req.UID, func(lockCtx context.Context) error {
		var innerErr error
		resp, innerErr = s.doClaimRewards(lockCtx, req)
		return innerErr
	})
	if err != nil {
		// Check if the lock itself failed
		if _, ok := err.(*mail.MailError); ok {
			return resp, err
		}
		return mail.ClaimRewardsResponse{}, mail.NewError(mail.ErrLockFailed, "failed to acquire user lock")
	}

	return resp, nil
}

func (s *serviceImpl) doClaimRewards(ctx context.Context, req mail.ClaimRewardsRequest) (mail.ClaimRewardsResponse, error) {
	now := nowMs()

	// Find claimable mails
	var mailIDs []int64
	if !req.All {
		mailIDs = req.MailIDs
	}
	claimable, err := s.repo.FindClaimableMails(ctx, req.ServerID, req.UID, mailIDs, now)
	if err != nil {
		return mail.ClaimRewardsResponse{}, mail.Errorf(mail.ErrInternal, "find claimable: %v", err)
	}

	if len(claimable) == 0 {
		return mail.ClaimRewardsResponse{}, nil
	}

	var (
		results    []mail.ClaimResult
		claimedIDs []int64
		failedIDs  []int64
		allRewards []mail.RewardItem
		newlyRead  int64
	)

	for _, doc := range claimable {
		rewards := repo.DocsToRewards(doc.Rewards)
		if len(rewards) == 0 {
			continue
		}

		// Build idempotency key
		idempotencyKey := fmt.Sprintf("%d:%d:%d", req.ServerID, req.UID, doc.MailID)

		// Grant rewards
		grantErr := s.granter.Grant(ctx, req.ServerID, req.UID, rewards, idempotencyKey)
		if grantErr != nil {
			results = append(results, mail.ClaimResult{
				MailID:  doc.MailID,
				Success: false,
				Code:    mail.ErrRewardGrantFail,
				ErrMsg:  grantErr.Error(),
			})
			failedIDs = append(failedIDs, doc.MailID)
			continue // Don't stop — continue with other mails
		}

		// Mark as claimed (also sets readAt)
		updated, markErr := s.repo.MarkClaimed(ctx, req.ServerID, req.UID, doc.MailID, now)
		if markErr != nil {
			results = append(results, mail.ClaimResult{
				MailID:  doc.MailID,
				Success: false,
				Code:    mail.ErrInternal,
				ErrMsg:  markErr.Error(),
			})
			failedIDs = append(failedIDs, doc.MailID)
			continue
		}

		results = append(results, mail.ClaimResult{
			MailID:  doc.MailID,
			Success: true,
			Code:    mail.ErrOK,
		})
		claimedIDs = append(claimedIDs, doc.MailID)
		allRewards = append(allRewards, rewards...)

		// Track newly read mails (was unread before claiming)
		if updated && doc.ReadAt == nil {
			newlyRead++
		}
	}

	// Update unread count for newly read mails
	if newlyRead > 0 {
		_ = s.cache.IncrUnread(ctx, req.ServerID, req.UID, -newlyRead)
	}

	return mail.ClaimRewardsResponse{
		Results:        results,
		ClaimedMailIDs: claimedIDs,
		FailedMailIDs:  failedIDs,
		Rewards:        aggregateRewards(allRewards),
	}, nil
}

// aggregateRewards merges duplicate itemIDs.
func aggregateRewards(items []mail.RewardItem) []mail.RewardItem {
	if len(items) == 0 {
		return nil
	}
	m := make(map[int32]int64)
	order := make([]int32, 0)
	for _, it := range items {
		if _, exists := m[it.ItemID]; !exists {
			order = append(order, it.ItemID)
		}
		m[it.ItemID] += it.Count
	}
	result := make([]mail.RewardItem, len(order))
	for i, id := range order {
		result[i] = mail.RewardItem{ItemID: id, Count: m[id]}
	}
	return result
}
