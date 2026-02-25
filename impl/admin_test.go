package impl

import (
	"context"
	"fmt"
	"testing"

	"github.com/vincentAlen/mail"
)

// ===================================================================
// RecallBroadcast
// ===================================================================

func TestRecallBroadcast(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()

		bc, err := env.svc.SendBroadcast(ctx, mail.SendBroadcastRequest{
			ServerID: env.serverID, RequestID: "rc-1",
			Target: mail.Target{Scope: "all"},
			Kind:   mail.MailKindBroadcast, Title: "To Recall",
		})
		must(t, err)

		err = env.admin.RecallBroadcast(ctx, mail.RecallBroadcastRequest{
			ServerID: env.serverID, MailID: bc.MailID,
			Operator: "admin1", Reason: "test",
		})
		must(t, err)

		// New user should NOT see the recalled broadcast
		list, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: 800,
		})
		must(t, err)
		assertEq(t, len(list.Mails), 0, "recalled broadcast not visible")
	})

	t.Run("NotFound", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()

		err := env.admin.RecallBroadcast(ctx, mail.RecallBroadcastRequest{
			ServerID: env.serverID, MailID: 99999,
			Operator: "admin1",
		})
		assertTrue(t, err != nil, "expected error for non-existent broadcast")
	})

	t.Run("AlreadyDelivered_UnclaimedSoftDeleted", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(801)

		bc, err := env.svc.SendBroadcast(ctx, mail.SendBroadcastRequest{
			ServerID: env.serverID, RequestID: "rc-clean",
			Target: mail.Target{Scope: "all"},
			Kind:   mail.MailKindBroadcast, Title: "Recall Cleanup",
		})
		must(t, err)

		// Deliver to user via ListInbox
		list, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, len(list.Mails), 1, "broadcast delivered")

		// Recall
		err = env.admin.RecallBroadcast(ctx, mail.RecallBroadcastRequest{
			ServerID: env.serverID, MailID: bc.MailID,
			Operator: "admin1", Reason: "oops",
		})
		must(t, err)

		// Already-delivered unclaimed mail should be soft-deleted
		list, err = env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, len(list.Mails), 0, "unclaimed mail cleaned up")
	})

	t.Run("AlreadyClaimed_NotRecalled", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(802)

		bc, err := env.svc.SendBroadcast(ctx, mail.SendBroadcastRequest{
			ServerID: env.serverID, RequestID: "rc-claimed",
			Target: mail.Target{Scope: "all"},
			Kind:   mail.MailKindBroadcast, Title: "Claimed BC",
			Rewards: []mail.RewardItem{{ItemID: 1, Count: 1}},
		})
		must(t, err)

		// Deliver via ListInbox
		_, err = env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)

		// Claim the reward
		_, err = env.svc.ClaimRewards(ctx, mail.ClaimRewardsRequest{
			ServerID: env.serverID, UID: uid, MailIDs: []int64{bc.MailID},
		})
		must(t, err)

		// Recall: already-claimed should NOT be soft-deleted
		err = env.admin.RecallBroadcast(ctx, mail.RecallBroadcastRequest{
			ServerID: env.serverID, MailID: bc.MailID,
			Operator: "admin1",
		})
		must(t, err)

		list, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, len(list.Mails), 1, "claimed mail still visible")
		assertTrue(t, list.Mails[0].ClaimedAtMs > 0, "still claimed")
	})

	t.Run("Validation", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()

		err := env.admin.RecallBroadcast(ctx, mail.RecallBroadcastRequest{
			ServerID: env.serverID, Operator: "admin1",
		})
		assertTrue(t, err != nil, "missing mailID")

		err = env.admin.RecallBroadcast(ctx, mail.RecallBroadcastRequest{
			ServerID: env.serverID, MailID: 1,
		})
		assertTrue(t, err != nil, "missing operator")
	})
}

// ===================================================================
// BatchSendPersonal
// ===================================================================

func TestBatchSendPersonal(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()

		uids := []int64{900, 901, 902}
		resp, err := env.admin.BatchSendPersonal(ctx, mail.BatchSendPersonalRequest{
			ServerID:  env.serverID,
			RequestID: "bs-1",
			UIDs:      uids,
			Kind:      mail.MailKindPersonal,
			Title:     "Batch Mail",
			Rewards:   []mail.RewardItem{{ItemID: 1, Count: 1}},
		})
		must(t, err)
		assertEq(t, resp.SuccessCount, int32(3), "all succeeded")
		assertEq(t, resp.FailCount, int32(0), "none failed")
		assertEq(t, len(resp.Results), 3, "result count")

		// Each user should see 1 mail
		for _, uid := range uids {
			list, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
				ServerID: env.serverID, UID: uid,
			})
			must(t, err)
			assertEq(t, len(list.Mails), 1, fmt.Sprintf("uid %d inbox", uid))
		}
	})

	t.Run("Idempotent", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()

		req := mail.BatchSendPersonalRequest{
			ServerID:  env.serverID,
			RequestID: "bs-idem",
			UIDs:      []int64{910, 911},
			Kind:      mail.MailKindPersonal,
			Title:     "Batch Idem",
		}

		resp1, err := env.admin.BatchSendPersonal(ctx, req)
		must(t, err)
		resp2, err := env.admin.BatchSendPersonal(ctx, req)
		must(t, err)

		for i := range resp1.Results {
			assertEq(t, resp1.Results[i].MailID, resp2.Results[i].MailID,
				fmt.Sprintf("uid %d idempotent", resp1.Results[i].UID))
		}
	})

	t.Run("PerUID_IndependentDedup", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()

		resp, err := env.admin.BatchSendPersonal(ctx, mail.BatchSendPersonalRequest{
			ServerID: env.serverID, RequestID: "bs-dedup",
			UIDs: []int64{920, 921},
			Kind: mail.MailKindPersonal, Title: "Dedup",
		})
		must(t, err)

		// Each UID gets a different mailID
		assertTrue(t, resp.Results[0].MailID != resp.Results[1].MailID,
			"different UIDs get different mailIDs")
	})

	t.Run("Validation_MissingRequestID", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		_, err := env.admin.BatchSendPersonal(ctx, mail.BatchSendPersonalRequest{
			ServerID: env.serverID, UIDs: []int64{1},
		})
		assertTrue(t, err != nil, "expected error for missing requestID")
	})

	t.Run("Validation_EmptyUIDs", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		_, err := env.admin.BatchSendPersonal(ctx, mail.BatchSendPersonalRequest{
			ServerID: env.serverID, RequestID: "bs-empty",
		})
		assertTrue(t, err != nil, "expected error for empty UIDs")
	})
}

// ===================================================================
// QueryByRequestId
// ===================================================================

func TestQueryByRequestId(t *testing.T) {
	t.Run("SendPersonal", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()

		send, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "qp-1", UID: 1000,
			Kind: mail.MailKindPersonal, Title: "Query Test",
		})
		must(t, err)

		query, err := env.admin.QueryByRequestId(ctx, mail.QueryByRequestIdRequest{
			ServerID:  env.serverID,
			Scope:     "send_personal",
			RequestID: "qp-1",
			UID:       1000,
		})
		must(t, err)
		assertTrue(t, query.Found, "found")
		assertEq(t, query.Status, "done", "status")
		assertEq(t, query.MailID, send.MailID, "mailID")
	})

	t.Run("SendBroadcast", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()

		send, err := env.svc.SendBroadcast(ctx, mail.SendBroadcastRequest{
			ServerID: env.serverID, RequestID: "qb-1",
			Target: mail.Target{Scope: "all"},
			Kind:   mail.MailKindBroadcast, Title: "Query BC",
		})
		must(t, err)

		query, err := env.admin.QueryByRequestId(ctx, mail.QueryByRequestIdRequest{
			ServerID:  env.serverID,
			Scope:     "send_broadcast",
			RequestID: "qb-1",
		})
		must(t, err)
		assertTrue(t, query.Found, "found")
		assertEq(t, query.Status, "done", "status")
		assertEq(t, query.MailID, send.MailID, "mailID")
		assertEq(t, query.MailKind, mail.MailKindBroadcast, "kind")
	})

	t.Run("BatchSendPersonal", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()

		resp, err := env.admin.BatchSendPersonal(ctx, mail.BatchSendPersonalRequest{
			ServerID: env.serverID, RequestID: "qbs-1",
			UIDs: []int64{1010, 1011},
			Kind: mail.MailKindPersonal, Title: "Query Batch",
		})
		must(t, err)

		// Query for first UID
		query, err := env.admin.QueryByRequestId(ctx, mail.QueryByRequestIdRequest{
			ServerID:  env.serverID,
			Scope:     "send_personal",
			RequestID: fmt.Sprintf("qbs-1:%d", int64(1010)),
			UID:       1010,
		})
		must(t, err)
		assertTrue(t, query.Found, "found")
		assertEq(t, query.MailID, resp.Results[0].MailID, "mailID for uid 1010")
	})

	t.Run("NotFound", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()

		query, err := env.admin.QueryByRequestId(ctx, mail.QueryByRequestIdRequest{
			ServerID:  env.serverID,
			Scope:     "send_personal",
			RequestID: "nonexistent",
		})
		must(t, err)
		assertFalse(t, query.Found, "not found")
		assertEq(t, query.Status, "not_found", "status")
	})

	t.Run("Validation_MissingScope", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		_, err := env.admin.QueryByRequestId(ctx, mail.QueryByRequestIdRequest{
			ServerID: env.serverID, RequestID: "x",
		})
		assertTrue(t, err != nil, "missing scope")
	})

	t.Run("Validation_MissingRequestID", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		_, err := env.admin.QueryByRequestId(ctx, mail.QueryByRequestIdRequest{
			ServerID: env.serverID, Scope: "send_personal",
		})
		assertTrue(t, err != nil, "missing requestID")
	})
}

// ===================================================================
// GetBroadcastStats
// ===================================================================

func TestGetBroadcastStats(t *testing.T) {
	t.Run("Comprehensive", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()

		// Send broadcast with rewards
		bc, err := env.svc.SendBroadcast(ctx, mail.SendBroadcastRequest{
			ServerID: env.serverID, RequestID: "stats-1",
			Target:  mail.Target{Scope: "all"},
			Kind:    mail.MailKindBroadcast, Title: "Stats",
			Rewards: []mail.RewardItem{{ItemID: 1, Count: 10}},
		})
		must(t, err)

		// Deliver to 3 users
		for _, uid := range []int64{1100, 1101, 1102} {
			_, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
				ServerID: env.serverID, UID: uid,
			})
			must(t, err)
		}

		// User 1100: read
		_, err = env.svc.MarkRead(ctx, mail.MarkReadRequest{
			ServerID: env.serverID, UID: 1100, MailIDs: []int64{bc.MailID},
		})
		must(t, err)

		// User 1101: claim (also marks read)
		_, err = env.svc.ClaimRewards(ctx, mail.ClaimRewardsRequest{
			ServerID: env.serverID, UID: 1101, MailIDs: []int64{bc.MailID},
		})
		must(t, err)

		// User 1102: delete
		_, err = env.svc.DeleteMails(ctx, mail.DeleteMailsRequest{
			ServerID: env.serverID, UID: 1102, MailIDs: []int64{bc.MailID},
		})
		must(t, err)

		stats, err := env.admin.GetBroadcastStats(ctx, mail.GetBroadcastStatsRequest{
			ServerID: env.serverID, MailID: bc.MailID,
		})
		must(t, err)
		assertEq(t, stats.TotalDelivered, int64(3), "delivered")
		assertEq(t, stats.TotalRead, int64(2), "read (direct + via claim)")
		assertEq(t, stats.TotalClaimed, int64(1), "claimed")
		assertEq(t, stats.TotalDeleted, int64(1), "deleted")
	})

	t.Run("NotFound", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		_, err := env.admin.GetBroadcastStats(ctx, mail.GetBroadcastStatsRequest{
			ServerID: env.serverID, MailID: 99999,
		})
		assertTrue(t, err != nil, "not found")
	})

	t.Run("NoDeliveries", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()

		bc, err := env.svc.SendBroadcast(ctx, mail.SendBroadcastRequest{
			ServerID: env.serverID, RequestID: "stats-empty",
			Target: mail.Target{Scope: "all"},
			Kind:   mail.MailKindBroadcast, Title: "No Delivery",
		})
		must(t, err)

		stats, err := env.admin.GetBroadcastStats(ctx, mail.GetBroadcastStatsRequest{
			ServerID: env.serverID, MailID: bc.MailID,
		})
		must(t, err)
		assertEq(t, stats.TotalDelivered, int64(0), "no deliveries")
	})

	t.Run("Validation_MissingMailID", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		_, err := env.admin.GetBroadcastStats(ctx, mail.GetBroadcastStatsRequest{
			ServerID: env.serverID,
		})
		assertTrue(t, err != nil, "missing mailID")
	})
}
