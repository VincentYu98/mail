package repo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// DedupResult represents the outcome of a dedup check.
type DedupResult struct {
	Duplicate    bool
	ResultMailID int64
}

// TryInsertDedup attempts to insert a dedup record with status "pending".
// On duplicate key:
//   - If existing record is "done" (or legacy with resultMailId > 0): returns Duplicate=true.
//   - If existing record is pending but the mail is already created, repair dedup and return duplicate.
//   - Otherwise treat it as stale pending, delete it, and re-insert for caller retry.
func (r *Repository) TryInsertDedup(ctx context.Context, doc MailDedupDoc) (DedupResult, error) {
	doc.Status = DedupStatusPending
	_, err := r.mailDedup.InsertOne(ctx, doc)
	if err == nil {
		return DedupResult{Duplicate: false}, nil
	}
	if !mongo.IsDuplicateKeyError(err) {
		return DedupResult{}, fmt.Errorf("dedup insert: %w", err)
	}

	// Duplicate found — check existing status
	var existing MailDedupDoc
	if findErr := r.mailDedup.FindOne(ctx, bson.M{"_id": doc.ID}).Decode(&existing); findErr != nil {
		return DedupResult{}, fmt.Errorf("dedup find existing: %w", findErr)
	}

	// Completed record (explicit done, or legacy record with resultMailId > 0)
	if existing.Status == DedupStatusDone || (existing.Status == "" && existing.ResultMailID > 0) {
		return DedupResult{Duplicate: true, ResultMailID: existing.ResultMailID}, nil
	}

	// Pending record might already have an inserted mail (e.g. dedup completion failed).
	recoveredMailID, recovered, recoverErr := r.recoverPendingDedupMailID(ctx, existing, doc)
	if recoverErr != nil {
		return DedupResult{}, fmt.Errorf("dedup recover pending: %w", recoverErr)
	}
	if recovered {
		_ = r.CompleteDedupStatus(ctx, doc.ID, recoveredMailID)
		return DedupResult{Duplicate: true, ResultMailID: recoveredMailID}, nil
	}

	// Stale pending record — delete and re-insert
	_, _ = r.mailDedup.DeleteOne(ctx, bson.M{"_id": doc.ID, "status": bson.M{"$ne": DedupStatusDone}})
	_, reinsertErr := r.mailDedup.InsertOne(ctx, doc)
	if reinsertErr != nil {
		if mongo.IsDuplicateKeyError(reinsertErr) {
			// Concurrent request completed — retry once
			return r.TryInsertDedup(ctx, doc)
		}
		return DedupResult{}, fmt.Errorf("dedup reinsert: %w", reinsertErr)
	}
	return DedupResult{Duplicate: false}, nil
}

func (r *Repository) recoverPendingDedupMailID(ctx context.Context, existing MailDedupDoc, incoming MailDedupDoc) (int64, bool, error) {
	requestID := existing.RequestID
	if requestID == "" {
		requestID = incoming.RequestID
	}
	if requestID == "" {
		requestID = existing.DedupKey
	}
	if requestID == "" {
		return 0, false, nil
	}

	switch existing.Scope {
	case "send_personal":
		return r.FindUserMailIDByRequestID(ctx, existing.ServerID, requestID)
	case "send_broadcast":
		return r.FindBroadcastMailIDByRequestID(ctx, existing.ServerID, requestID)
	default:
		return 0, false, nil
	}
}

// CompleteDedupStatus marks a dedup record as done and sets the resultMailId.
func (r *Repository) CompleteDedupStatus(ctx context.Context, dedupID string, mailID int64) error {
	_, err := r.mailDedup.UpdateByID(ctx, dedupID, bson.M{
		"$set": bson.M{"status": DedupStatusDone, "resultMailId": mailID},
	})
	return err
}

// FindDedup looks up a dedup record by its composite ID.
func (r *Repository) FindDedup(ctx context.Context, serverID int32, scope, dedupKey string) (*MailDedupDoc, error) {
	id := FormatDedupID(serverID, scope, dedupKey)
	var doc MailDedupDoc
	err := r.mailDedup.FindOne(ctx, bson.M{"_id": id}).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &doc, nil
}

// FormatDedupID builds the composite _id for mail_dedup.
func FormatDedupID(serverID int32, scope, dedupKey string) string {
	return fmt.Sprintf("%d:%s:%s", serverID, scope, dedupKey)
}
