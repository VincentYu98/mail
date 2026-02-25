package impl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/vincentAlen/mail"
)

// ===================================================================
// SendPersonal
// ===================================================================

func TestSendPersonal(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()

		resp, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID:  env.serverID,
			RequestID: "sp-1",
			UID:       100,
			Kind:      mail.MailKindPersonal,
			Title:     "Hello",
			Content:   "World",
			Rewards:   []mail.RewardItem{{ItemID: 1, Count: 10}},
		})
		must(t, err)
		assertTrue(t, resp.MailID > 0, "mailID should be positive")
		assertEq(t, env.push.eventCount(), 1, "push event count")
	})

	t.Run("Idempotent", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()

		req := mail.SendPersonalRequest{
			ServerID:  env.serverID,
			RequestID: "sp-idem",
			UID:       101,
			Kind:      mail.MailKindPersonal,
			Title:     "Dup",
		}

		resp1, err := env.svc.SendPersonal(ctx, req)
		must(t, err)
		resp2, err := env.svc.SendPersonal(ctx, req)
		must(t, err)
		assertEq(t, resp1.MailID, resp2.MailID, "idempotent mailID")
	})

	t.Run("DifferentRequestID_DifferentMail", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()

		resp1, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "sp-diff-1", UID: 102,
			Kind: mail.MailKindPersonal, Title: "A",
		})
		must(t, err)
		resp2, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "sp-diff-2", UID: 102,
			Kind: mail.MailKindPersonal, Title: "B",
		})
		must(t, err)
		assertTrue(t, resp1.MailID != resp2.MailID, "different requestIDs => different mails")
	})

	t.Run("DedupRecovery_PendingStatus", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(110)

		// Directly insert a stale pending dedup record to simulate a failed previous attempt
		dedupID := fmt.Sprintf("%d:send_personal:dedup-recover", env.serverID)
		_, err := testDB.Collection("mail_dedup").InsertOne(ctx, map[string]any{
			"_id":          dedupID,
			"serverId":     env.serverID,
			"scope":        "send_personal",
			"dedupKey":     "dedup-recover",
			"requestId":    "dedup-recover",
			"resultMailId": int64(0),
			"status":       "pending",
			"createdAt":    time.Now().UnixMilli() - 60_000,
			"purgeAt":      time.Now().UnixMilli() + 86_400_000,
		})
		must(t, err)

		// SendPersonal with the same requestID should recover: delete stale pending and re-create
		resp, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID:  env.serverID,
			RequestID: "dedup-recover",
			UID:       uid,
			Kind:      mail.MailKindPersonal,
			Title:     "Recovered",
		})
		must(t, err)
		assertTrue(t, resp.MailID > 0, "mail should be created after dedup recovery")

		// Verify dedup is now "done" by calling again (idempotent)
		resp2, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID:  env.serverID,
			RequestID: "dedup-recover",
			UID:       uid,
			Kind:      mail.MailKindPersonal,
			Title:     "Recovered",
		})
		must(t, err)
		assertEq(t, resp.MailID, resp2.MailID, "idempotent after recovery")
	})

	t.Run("Validation_MissingUID", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		_, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "v1",
		})
		assertTrue(t, err != nil, "expected error for missing UID")
	})

	t.Run("Validation_MissingRequestID", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		_, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, UID: 1,
		})
		assertTrue(t, err != nil, "expected error for missing requestID")
	})

	t.Run("CustomExpireAt", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		now := time.Now().UnixMilli()

		resp, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "sp-exp", UID: 103,
			Kind: mail.MailKindPersonal, Title: "Custom Expire",
			ExpireAtMs: now + 3_600_000, // 1 hour
		})
		must(t, err)

		list, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: 103,
		})
		must(t, err)
		assertEq(t, len(list.Mails), 1, "visible mail count")
		assertEq(t, list.Mails[0].MailID, resp.MailID, "mail ID matches")
	})
}

// ===================================================================
// SendBroadcast
// ===================================================================

func TestSendBroadcast(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()

		resp, err := env.svc.SendBroadcast(ctx, mail.SendBroadcastRequest{
			ServerID:  env.serverID,
			RequestID: "bc-1",
			Target:    mail.Target{Scope: "all"},
			Kind:      mail.MailKindBroadcast,
			Title:     "Announcement",
			Rewards:   []mail.RewardItem{{ItemID: 2, Count: 5}},
		})
		must(t, err)
		assertTrue(t, resp.MailID > 0, "mailID should be positive")
	})

	t.Run("Idempotent", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()

		req := mail.SendBroadcastRequest{
			ServerID: env.serverID, RequestID: "bc-idem",
			Target: mail.Target{Scope: "all"},
			Kind:   mail.MailKindBroadcast, Title: "Dup",
		}
		resp1, err := env.svc.SendBroadcast(ctx, req)
		must(t, err)
		resp2, err := env.svc.SendBroadcast(ctx, req)
		must(t, err)
		assertEq(t, resp1.MailID, resp2.MailID, "idempotent mailID")
	})

	t.Run("Validation_MissingRequestID", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		_, err := env.svc.SendBroadcast(ctx, mail.SendBroadcastRequest{
			ServerID: env.serverID, Target: mail.Target{Scope: "all"},
		})
		assertTrue(t, err != nil, "expected error for missing requestID")
	})

	t.Run("Validation_MissingTarget", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		_, err := env.svc.SendBroadcast(ctx, mail.SendBroadcastRequest{
			ServerID: env.serverID, RequestID: "bc-val",
		})
		assertTrue(t, err != nil, "expected error for missing target")
	})

	t.Run("Validation_FutureStartAtMs", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		_, err := env.svc.SendBroadcast(ctx, mail.SendBroadcastRequest{
			ServerID:  env.serverID,
			RequestID: "v-future",
			Target:    mail.Target{Scope: "all"},
			StartAtMs: 1000,
		})
		assertTrue(t, err != nil, "expected error for StartAtMs > 0")
	})
}

// ===================================================================
// ListInbox
// ===================================================================

func TestListInbox(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()

		list, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: 200,
		})
		must(t, err)
		assertEq(t, len(list.Mails), 0, "empty inbox")
		assertEq(t, list.NextCursor, int64(0), "no next cursor")
	})

	t.Run("PersonalMails_OrderByMailIdDesc", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(201)

		for i := 1; i <= 3; i++ {
			_, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
				ServerID: env.serverID, RequestID: fmt.Sprintf("list-p-%d", i),
				UID: uid, Kind: mail.MailKindPersonal,
				Title: fmt.Sprintf("Mail %d", i),
			})
			must(t, err)
		}

		list, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, len(list.Mails), 3, "mail count")
		// ordered mailId desc
		assertTrue(t, list.Mails[0].MailID > list.Mails[1].MailID, "desc order [0]>[1]")
		assertTrue(t, list.Mails[1].MailID > list.Mails[2].MailID, "desc order [1]>[2]")
	})

	t.Run("BroadcastSync", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(202)

		bcResp, err := env.svc.SendBroadcast(ctx, mail.SendBroadcastRequest{
			ServerID: env.serverID, RequestID: "list-bc-1",
			Target: mail.Target{Scope: "all"},
			Kind:   mail.MailKindBroadcast, Title: "Global",
		})
		must(t, err)

		// ListInbox triggers broadcast sync
		list, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, len(list.Mails), 1, "broadcast synced")
		assertEq(t, list.Mails[0].MailID, bcResp.MailID, "broadcast mailID")
		assertEq(t, list.Mails[0].Kind, mail.MailKindBroadcast, "kind")
	})

	t.Run("BroadcastSync_FastPath", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(203)

		_, err := env.svc.SendBroadcast(ctx, mail.SendBroadcastRequest{
			ServerID: env.serverID, RequestID: "fast-bc-1",
			Target: mail.Target{Scope: "all"},
			Kind:   mail.MailKindBroadcast, Title: "Fast",
		})
		must(t, err)

		// First ListInbox: full sync
		list1, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, len(list1.Mails), 1, "first list")

		// Second ListInbox: fast path (no new broadcasts)
		list2, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, len(list2.Mails), 1, "second list (fast path)")
	})

	t.Run("BroadcastSync_MultipleUsers", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()

		_, err := env.svc.SendBroadcast(ctx, mail.SendBroadcastRequest{
			ServerID: env.serverID, RequestID: "multi-bc",
			Target: mail.Target{Scope: "all"},
			Kind:   mail.MailKindBroadcast, Title: "Multi",
		})
		must(t, err)

		// Two users both see it
		for _, uid := range []int64{210, 211} {
			list, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
				ServerID: env.serverID, UID: uid,
			})
			must(t, err)
			assertEq(t, len(list.Mails), 1, fmt.Sprintf("uid %d sees broadcast", uid))
		}
	})

	t.Run("BroadcastSync_SkipsFailedMatchTarget", func(t *testing.T) {
		resolver := &mockResolver{failScopes: map[string]bool{"fail_scope": true}}
		env := newTestEnvWithResolver(t, resolver)
		ctx := context.Background()
		uid := int64(207)

		// Broadcast 1: scope="all" (succeeds via built-in)
		_, err := env.svc.SendBroadcast(ctx, mail.SendBroadcastRequest{
			ServerID: env.serverID, RequestID: "skip-bc-1",
			Target: mail.Target{Scope: "all"},
			Kind:   mail.MailKindBroadcast, Title: "BC1",
		})
		must(t, err)

		// Broadcast 2: scope="fail_scope" (matchTarget will error)
		_, err = env.svc.SendBroadcast(ctx, mail.SendBroadcastRequest{
			ServerID: env.serverID, RequestID: "skip-bc-2",
			Target: mail.Target{Scope: "fail_scope"},
			Kind:   mail.MailKindBroadcast, Title: "BC2-Poison",
		})
		must(t, err)

		// Broadcast 3: scope="all" (succeeds)
		_, err = env.svc.SendBroadcast(ctx, mail.SendBroadcastRequest{
			ServerID: env.serverID, RequestID: "skip-bc-3",
			Target: mail.Target{Scope: "all"},
			Kind:   mail.MailKindBroadcast, Title: "BC3",
		})
		must(t, err)

		// ListInbox should deliver bc1 and bc3, skipping bc2
		list, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, len(list.Mails), 2, "skip failed broadcast, deliver bc1 and bc3")

		// Second ListInbox should not re-process (cursor advanced past all 3)
		list2, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, len(list2.Mails), 2, "cursor advanced, no reprocessing")
	})

	t.Run("Pagination", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(204)

		for i := 1; i <= 5; i++ {
			_, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
				ServerID: env.serverID, RequestID: fmt.Sprintf("page-%d", i),
				UID: uid, Kind: mail.MailKindPersonal, Title: fmt.Sprintf("M%d", i),
			})
			must(t, err)
		}

		// Page 1: limit 2
		page1, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: uid, Limit: 2,
		})
		must(t, err)
		assertEq(t, len(page1.Mails), 2, "page1 count")
		assertTrue(t, page1.NextCursor > 0, "page1 has next cursor")

		// Page 2
		page2, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: uid, Limit: 2,
			BeforeMailID: page1.NextCursor,
		})
		must(t, err)
		assertEq(t, len(page2.Mails), 2, "page2 count")
		assertTrue(t, page2.NextCursor > 0, "page2 has next cursor")

		// Page 3 (last)
		page3, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: uid, Limit: 2,
			BeforeMailID: page2.NextCursor,
		})
		must(t, err)
		assertEq(t, len(page3.Mails), 1, "page3 count")
		assertEq(t, page3.NextCursor, int64(0), "no more pages")

		// No duplicates across pages
		seen := make(map[int64]bool)
		allMails := append(page1.Mails, append(page2.Mails, page3.Mails...)...)
		for _, m := range allMails {
			assertFalse(t, seen[m.MailID], fmt.Sprintf("dup mailID %d", m.MailID))
			seen[m.MailID] = true
		}
		assertEq(t, len(seen), 5, "total unique mails")
	})

	t.Run("ExpiredFiltered", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(205)
		now := time.Now().UnixMilli()

		// Expired mail (sendAt=now, expireAt=past → already expired)
		_, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "exp-1", UID: uid,
			Kind: mail.MailKindPersonal, Title: "Expired",
			ExpireAtMs: now - 1_000,
		})
		must(t, err)

		// Active mail
		_, err = env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "exp-2", UID: uid,
			Kind: mail.MailKindPersonal, Title: "Active",
		})
		must(t, err)

		list, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, len(list.Mails), 1, "only active mail visible")
		assertEq(t, list.Mails[0].Title, "Active", "correct mail")
	})

	t.Run("FutureSendAtMs_Rejected", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		_, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "future-1", UID: 206,
			Kind: mail.MailKindPersonal, Title: "Future",
			SendAtMs: 1000, // any positive value
		})
		assertTrue(t, err != nil, "expected error for SendAtMs > 0")
	})

	t.Run("Validation_MissingUID", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		_, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID,
		})
		assertTrue(t, err != nil, "expected error for missing UID")
	})
}

// ===================================================================
// MarkRead
// ===================================================================

func TestMarkRead(t *testing.T) {
	t.Run("ByIDs", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(300)

		r1, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "mr-1", UID: uid,
			Kind: mail.MailKindPersonal, Title: "M1",
		})
		must(t, err)
		r2, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "mr-2", UID: uid,
			Kind: mail.MailKindPersonal, Title: "M2",
		})
		must(t, err)

		// Mark only first as read
		readResp, err := env.svc.MarkRead(ctx, mail.MarkReadRequest{
			ServerID: env.serverID, UID: uid, MailIDs: []int64{r1.MailID},
		})
		must(t, err)
		assertEq(t, len(readResp.UpdatedMailIDs), 1, "updated count")
		assertEq(t, readResp.UpdatedMailIDs[0], r1.MailID, "updated mailID")

		// Verify in inbox
		list, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		for _, m := range list.Mails {
			if m.MailID == r1.MailID {
				assertTrue(t, m.ReadAtMs > 0, "mail 1 should be read")
			}
			if m.MailID == r2.MailID {
				assertEq(t, m.ReadAtMs, int64(0), "mail 2 should be unread")
			}
		}
	})

	t.Run("All", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(301)

		for i := 1; i <= 3; i++ {
			_, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
				ServerID: env.serverID, RequestID: fmt.Sprintf("mra-%d", i),
				UID: uid, Kind: mail.MailKindPersonal, Title: fmt.Sprintf("M%d", i),
			})
			must(t, err)
		}

		readResp, err := env.svc.MarkRead(ctx, mail.MarkReadRequest{
			ServerID: env.serverID, UID: uid, All: true,
		})
		must(t, err)
		assertEq(t, len(readResp.UpdatedMailIDs), 3, "all marked read")
	})

	t.Run("Idempotent", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(302)

		resp, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "mri-1", UID: uid,
			Kind: mail.MailKindPersonal, Title: "M",
		})
		must(t, err)

		r1, err := env.svc.MarkRead(ctx, mail.MarkReadRequest{
			ServerID: env.serverID, UID: uid, MailIDs: []int64{resp.MailID},
		})
		must(t, err)
		assertEq(t, len(r1.UpdatedMailIDs), 1, "first mark")

		r2, err := env.svc.MarkRead(ctx, mail.MarkReadRequest{
			ServerID: env.serverID, UID: uid, MailIDs: []int64{resp.MailID},
		})
		must(t, err)
		assertEq(t, len(r2.UpdatedMailIDs), 0, "second mark is no-op")
	})

	t.Run("UnreadCountDecremented", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(303)

		for i := 1; i <= 3; i++ {
			_, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
				ServerID: env.serverID, RequestID: fmt.Sprintf("mruc-%d", i),
				UID: uid, Kind: mail.MailKindPersonal, Title: fmt.Sprintf("M%d", i),
			})
			must(t, err)
		}

		unread, err := env.svc.GetUnreadCount(ctx, mail.GetUnreadCountRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, unread.UnreadCount, int64(3), "before mark read")

		_, err = env.svc.MarkRead(ctx, mail.MarkReadRequest{
			ServerID: env.serverID, UID: uid, All: true,
		})
		must(t, err)

		unread, err = env.svc.GetUnreadCount(ctx, mail.GetUnreadCountRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, unread.UnreadCount, int64(0), "after mark all read")
	})

	t.Run("Validation_MissingMailIDsAndAll", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		_, err := env.svc.MarkRead(ctx, mail.MarkReadRequest{
			ServerID: env.serverID, UID: 1,
		})
		assertTrue(t, err != nil, "expected error when neither mailIDs nor all is set")
	})
}

// ===================================================================
// ClaimRewards
// ===================================================================

func TestClaimRewards(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(400)

		resp, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "cr-1", UID: uid,
			Kind: mail.MailKindPersonal, Title: "Reward",
			Rewards: []mail.RewardItem{{ItemID: 100, Count: 50}},
		})
		must(t, err)

		claim, err := env.svc.ClaimRewards(ctx, mail.ClaimRewardsRequest{
			ServerID: env.serverID, UID: uid, MailIDs: []int64{resp.MailID},
		})
		must(t, err)
		assertEq(t, len(claim.ClaimedMailIDs), 1, "claimed count")
		assertEq(t, len(claim.FailedMailIDs), 0, "failed count")
		assertEq(t, len(claim.Rewards), 1, "reward types")
		assertEq(t, claim.Rewards[0].ItemID, int32(100), "reward itemID")
		assertEq(t, claim.Rewards[0].Count, int64(50), "reward count")
		assertEq(t, env.granter.callCount(), 1, "grant calls")

		// Verify marked as claimed in inbox
		list, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, len(list.Mails), 1, "still in inbox")
		assertTrue(t, list.Mails[0].ClaimedAtMs > 0, "claimed")
		assertTrue(t, list.Mails[0].ReadAtMs > 0, "also marked read")
	})

	t.Run("ClaimAll", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(401)

		for i := 1; i <= 3; i++ {
			_, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
				ServerID: env.serverID, RequestID: fmt.Sprintf("cra-%d", i), UID: uid,
				Kind: mail.MailKindPersonal, Title: fmt.Sprintf("R%d", i),
				Rewards: []mail.RewardItem{{ItemID: int32(i), Count: int64(i * 10)}},
			})
			must(t, err)
		}

		claim, err := env.svc.ClaimRewards(ctx, mail.ClaimRewardsRequest{
			ServerID: env.serverID, UID: uid, All: true,
		})
		must(t, err)
		assertEq(t, len(claim.ClaimedMailIDs), 3, "all claimed")
		assertEq(t, len(claim.Rewards), 3, "reward types")
	})

	t.Run("PartialFailure", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(402)

		var mailIDs []int64
		for i := 1; i <= 3; i++ {
			resp, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
				ServerID: env.serverID, RequestID: fmt.Sprintf("crp-%d", i), UID: uid,
				Kind: mail.MailKindPersonal, Title: fmt.Sprintf("R%d", i),
				Rewards: []mail.RewardItem{{ItemID: int32(i), Count: 10}},
			})
			must(t, err)
			mailIDs = append(mailIDs, resp.MailID)
		}

		// Make granter fail for the second mail
		failKey := fmt.Sprintf("%d:%d:%d", env.serverID, uid, mailIDs[1])
		env.granter.mu.Lock()
		env.granter.failKeys = map[string]error{failKey: fmt.Errorf("grant failed")}
		env.granter.mu.Unlock()

		claim, err := env.svc.ClaimRewards(ctx, mail.ClaimRewardsRequest{
			ServerID: env.serverID, UID: uid, MailIDs: mailIDs,
		})
		must(t, err)
		assertEq(t, len(claim.ClaimedMailIDs), 2, "2 succeeded")
		assertEq(t, len(claim.FailedMailIDs), 1, "1 failed")
		assertEq(t, claim.FailedMailIDs[0], mailIDs[1], "failed is the second mail")

		// Check results detail
		for _, r := range claim.Results {
			if r.MailID == mailIDs[1] {
				assertFalse(t, r.Success, "second mail failed")
				assertEq(t, r.Code, mail.ErrRewardGrantFail, "failure code")
			} else {
				assertTrue(t, r.Success, fmt.Sprintf("mail %d succeeded", r.MailID))
			}
		}
	})

	t.Run("AlreadyClaimed", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(403)

		resp, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "crac-1", UID: uid,
			Kind: mail.MailKindPersonal, Title: "Already",
			Rewards: []mail.RewardItem{{ItemID: 1, Count: 1}},
		})
		must(t, err)

		// Claim once
		c1, err := env.svc.ClaimRewards(ctx, mail.ClaimRewardsRequest{
			ServerID: env.serverID, UID: uid, MailIDs: []int64{resp.MailID},
		})
		must(t, err)
		assertEq(t, len(c1.ClaimedMailIDs), 1, "first claim")

		// Claim again — nothing claimable
		c2, err := env.svc.ClaimRewards(ctx, mail.ClaimRewardsRequest{
			ServerID: env.serverID, UID: uid, MailIDs: []int64{resp.MailID},
		})
		must(t, err)
		assertEq(t, len(c2.ClaimedMailIDs), 0, "second claim no-op")
	})

	t.Run("NoRewards", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(404)

		resp, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "crnr-1", UID: uid,
			Kind: mail.MailKindNotify, Title: "No Rewards",
		})
		must(t, err)

		claim, err := env.svc.ClaimRewards(ctx, mail.ClaimRewardsRequest{
			ServerID: env.serverID, UID: uid, MailIDs: []int64{resp.MailID},
		})
		must(t, err)
		assertEq(t, len(claim.ClaimedMailIDs), 0, "nothing to claim")
		assertEq(t, env.granter.callCount(), 0, "granter not called")
	})

	t.Run("ExpiredNotClaimable", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(405)
		now := time.Now().UnixMilli()

		resp, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "crexp-1", UID: uid,
			Kind: mail.MailKindPersonal, Title: "Expired",
			Rewards:    []mail.RewardItem{{ItemID: 1, Count: 1}},
			ExpireAtMs: now - 1_000, // sendAt=now, expireAt=past → already expired
		})
		must(t, err)

		claim, err := env.svc.ClaimRewards(ctx, mail.ClaimRewardsRequest{
			ServerID: env.serverID, UID: uid, MailIDs: []int64{resp.MailID},
		})
		must(t, err)
		assertEq(t, len(claim.ClaimedMailIDs), 0, "expired not claimable")
	})

	t.Run("AggregateRewards_MergesDuplicateItems", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(406)

		// Two mails with the same reward itemID
		for i := 1; i <= 2; i++ {
			_, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
				ServerID: env.serverID, RequestID: fmt.Sprintf("crag-%d", i), UID: uid,
				Kind: mail.MailKindPersonal, Title: fmt.Sprintf("R%d", i),
				Rewards: []mail.RewardItem{{ItemID: 100, Count: int64(i * 10)}},
			})
			must(t, err)
		}

		claim, err := env.svc.ClaimRewards(ctx, mail.ClaimRewardsRequest{
			ServerID: env.serverID, UID: uid, All: true,
		})
		must(t, err)
		assertEq(t, len(claim.ClaimedMailIDs), 2, "both claimed")
		// Aggregated: itemID 100 → 10+20=30
		assertEq(t, len(claim.Rewards), 1, "merged into 1 reward type")
		assertEq(t, claim.Rewards[0].ItemID, int32(100), "itemID")
		assertEq(t, claim.Rewards[0].Count, int64(30), "merged count")
	})

	t.Run("UnreadCountUpdated", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(407)

		_, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "cruc-1", UID: uid,
			Kind: mail.MailKindPersonal, Title: "R",
			Rewards: []mail.RewardItem{{ItemID: 1, Count: 1}},
		})
		must(t, err)

		unread, err := env.svc.GetUnreadCount(ctx, mail.GetUnreadCountRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, unread.UnreadCount, int64(1), "before claim")

		_, err = env.svc.ClaimRewards(ctx, mail.ClaimRewardsRequest{
			ServerID: env.serverID, UID: uid, All: true,
		})
		must(t, err)

		unread, err = env.svc.GetUnreadCount(ctx, mail.GetUnreadCountRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, unread.UnreadCount, int64(0), "after claim (also marks read)")
	})
}

// ===================================================================
// DeleteMails
// ===================================================================

func TestDeleteMails(t *testing.T) {
	t.Run("ByIDs", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(500)

		r1, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "del-1", UID: uid,
			Kind: mail.MailKindPersonal, Title: "D1",
		})
		must(t, err)
		_, err = env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "del-2", UID: uid,
			Kind: mail.MailKindPersonal, Title: "D2",
		})
		must(t, err)

		delResp, err := env.svc.DeleteMails(ctx, mail.DeleteMailsRequest{
			ServerID: env.serverID, UID: uid, MailIDs: []int64{r1.MailID},
		})
		must(t, err)
		assertEq(t, len(delResp.DeletedMailIDs), 1, "deleted 1")

		list, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, len(list.Mails), 1, "one mail left")
	})

	t.Run("All", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(501)

		for i := 1; i <= 3; i++ {
			_, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
				ServerID: env.serverID, RequestID: fmt.Sprintf("dela-%d", i),
				UID: uid, Kind: mail.MailKindPersonal, Title: fmt.Sprintf("D%d", i),
			})
			must(t, err)
		}

		delResp, err := env.svc.DeleteMails(ctx, mail.DeleteMailsRequest{
			ServerID: env.serverID, UID: uid, All: true,
		})
		must(t, err)
		assertEq(t, len(delResp.DeletedMailIDs), 3, "all deleted")

		list, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, len(list.Mails), 0, "inbox empty")
	})

	t.Run("UnreadCountDecremented", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(502)

		_, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "deluc-1", UID: uid,
			Kind: mail.MailKindPersonal, Title: "Unread",
		})
		must(t, err)

		unread, err := env.svc.GetUnreadCount(ctx, mail.GetUnreadCountRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, unread.UnreadCount, int64(1), "before delete")

		_, err = env.svc.DeleteMails(ctx, mail.DeleteMailsRequest{
			ServerID: env.serverID, UID: uid, All: true,
		})
		must(t, err)

		unread, err = env.svc.GetUnreadCount(ctx, mail.GetUnreadCountRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, unread.UnreadCount, int64(0), "after delete")
	})

	t.Run("DeleteIdempotent", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(503)

		resp, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "deli-1", UID: uid,
			Kind: mail.MailKindPersonal, Title: "Del",
		})
		must(t, err)

		d1, err := env.svc.DeleteMails(ctx, mail.DeleteMailsRequest{
			ServerID: env.serverID, UID: uid, MailIDs: []int64{resp.MailID},
		})
		must(t, err)
		assertEq(t, len(d1.DeletedMailIDs), 1, "first delete")

		d2, err := env.svc.DeleteMails(ctx, mail.DeleteMailsRequest{
			ServerID: env.serverID, UID: uid, MailIDs: []int64{resp.MailID},
		})
		must(t, err)
		assertEq(t, len(d2.DeletedMailIDs), 0, "second delete no-op")
	})
}

// ===================================================================
// GetUnreadCount
// ===================================================================

func TestGetUnreadCount(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(600)

		// No mails → 0
		unread, err := env.svc.GetUnreadCount(ctx, mail.GetUnreadCountRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, unread.UnreadCount, int64(0), "empty")

		// Send 3
		for i := 1; i <= 3; i++ {
			_, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
				ServerID: env.serverID, RequestID: fmt.Sprintf("uc-%d", i),
				UID: uid, Kind: mail.MailKindPersonal, Title: fmt.Sprintf("U%d", i),
			})
			must(t, err)
		}

		unread, err = env.svc.GetUnreadCount(ctx, mail.GetUnreadCountRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, unread.UnreadCount, int64(3), "3 unread")
	})

	t.Run("HasClaimable", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(601)

		unread, err := env.svc.GetUnreadCount(ctx, mail.GetUnreadCountRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertFalse(t, unread.HasClaimable, "no mail => no claimable")

		// Notify (no rewards)
		_, err = env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "hc-0", UID: uid,
			Kind: mail.MailKindNotify, Title: "Notify",
		})
		must(t, err)

		unread, err = env.svc.GetUnreadCount(ctx, mail.GetUnreadCountRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertFalse(t, unread.HasClaimable, "notify without rewards => no claimable")

		// Mail with rewards
		_, err = env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
			ServerID: env.serverID, RequestID: "hc-1", UID: uid,
			Kind: mail.MailKindPersonal, Title: "With Reward",
			Rewards: []mail.RewardItem{{ItemID: 1, Count: 1}},
		})
		must(t, err)

		unread, err = env.svc.GetUnreadCount(ctx, mail.GetUnreadCountRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertTrue(t, unread.HasClaimable, "has claimable")
	})

	t.Run("BroadcastSyncIncrementsUnread", func(t *testing.T) {
		env := newTestEnv(t)
		ctx := context.Background()
		uid := int64(602)

		_, err := env.svc.SendBroadcast(ctx, mail.SendBroadcastRequest{
			ServerID: env.serverID, RequestID: "ucbc-1",
			Target: mail.Target{Scope: "all"},
			Kind:   mail.MailKindBroadcast, Title: "BC",
		})
		must(t, err)

		// Before ListInbox: broadcast not synced, unread=0
		unread, err := env.svc.GetUnreadCount(ctx, mail.GetUnreadCountRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, unread.UnreadCount, int64(0), "before sync")

		// ListInbox triggers sync
		_, err = env.svc.ListInbox(ctx, mail.ListInboxRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)

		unread, err = env.svc.GetUnreadCount(ctx, mail.GetUnreadCountRequest{
			ServerID: env.serverID, UID: uid,
		})
		must(t, err)
		assertEq(t, unread.UnreadCount, int64(1), "after sync")
	})
}

// ===================================================================
// Full Lifecycle: send → list → read → claim → delete
// ===================================================================

func TestFullLifecycle(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	uid := int64(700)

	// 1. Send personal mail with rewards
	personal, err := env.svc.SendPersonal(ctx, mail.SendPersonalRequest{
		ServerID: env.serverID, RequestID: "lc-1", UID: uid,
		Kind: mail.MailKindPersonal, Title: "Lifecycle",
		Rewards: []mail.RewardItem{{ItemID: 10, Count: 100}},
	})
	must(t, err)

	// 2. Send broadcast (no rewards)
	broadcast, err := env.svc.SendBroadcast(ctx, mail.SendBroadcastRequest{
		ServerID: env.serverID, RequestID: "lc-bc",
		Target: mail.Target{Scope: "all"},
		Kind:   mail.MailKindBroadcast, Title: "Broadcast",
	})
	must(t, err)

	// 3. Unread = 1 (personal only; broadcast not synced yet)
	unread, err := env.svc.GetUnreadCount(ctx, mail.GetUnreadCountRequest{
		ServerID: env.serverID, UID: uid,
	})
	must(t, err)
	assertEq(t, unread.UnreadCount, int64(1), "step3: unread=1")
	assertTrue(t, unread.HasClaimable, "step3: has claimable")

	// 4. ListInbox triggers broadcast sync → 2 mails visible
	list, err := env.svc.ListInbox(ctx, mail.ListInboxRequest{
		ServerID: env.serverID, UID: uid,
	})
	must(t, err)
	assertEq(t, len(list.Mails), 2, "step4: 2 mails")

	// 5. Unread now = 2 (broadcast synced +1)
	unread, err = env.svc.GetUnreadCount(ctx, mail.GetUnreadCountRequest{
		ServerID: env.serverID, UID: uid,
	})
	must(t, err)
	assertEq(t, unread.UnreadCount, int64(2), "step5: unread=2")

	// 6. Mark broadcast as read
	_, err = env.svc.MarkRead(ctx, mail.MarkReadRequest{
		ServerID: env.serverID, UID: uid, MailIDs: []int64{broadcast.MailID},
	})
	must(t, err)

	unread, err = env.svc.GetUnreadCount(ctx, mail.GetUnreadCountRequest{
		ServerID: env.serverID, UID: uid,
	})
	must(t, err)
	assertEq(t, unread.UnreadCount, int64(1), "step6: unread=1")

	// 7. Claim personal mail rewards (also marks read)
	claim, err := env.svc.ClaimRewards(ctx, mail.ClaimRewardsRequest{
		ServerID: env.serverID, UID: uid, MailIDs: []int64{personal.MailID},
	})
	must(t, err)
	assertEq(t, len(claim.ClaimedMailIDs), 1, "step7: claimed")
	assertEq(t, claim.Rewards[0].ItemID, int32(10), "step7: reward itemID")
	assertEq(t, claim.Rewards[0].Count, int64(100), "step7: reward count")

	// 8. Unread = 0
	unread, err = env.svc.GetUnreadCount(ctx, mail.GetUnreadCountRequest{
		ServerID: env.serverID, UID: uid,
	})
	must(t, err)
	assertEq(t, unread.UnreadCount, int64(0), "step8: unread=0")
	assertFalse(t, unread.HasClaimable, "step8: nothing claimable")

	// 9. Delete all
	del, err := env.svc.DeleteMails(ctx, mail.DeleteMailsRequest{
		ServerID: env.serverID, UID: uid, All: true,
	})
	must(t, err)
	assertEq(t, len(del.DeletedMailIDs), 2, "step9: deleted 2")

	// 10. Inbox empty
	list, err = env.svc.ListInbox(ctx, mail.ListInboxRequest{
		ServerID: env.serverID, UID: uid,
	})
	must(t, err)
	assertEq(t, len(list.Mails), 0, "step10: inbox empty")
}
