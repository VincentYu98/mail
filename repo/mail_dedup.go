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

// TryInsertDedup attempts to insert a dedup record. On duplicate key, returns the existing record.
func (r *Repository) TryInsertDedup(ctx context.Context, doc MailDedupDoc) (DedupResult, error) {
	_, err := r.mailDedup.InsertOne(ctx, doc)
	if err == nil {
		return DedupResult{Duplicate: false}, nil
	}
	if mongo.IsDuplicateKeyError(err) {
		var existing MailDedupDoc
		if findErr := r.mailDedup.FindOne(ctx, bson.M{"_id": doc.ID}).Decode(&existing); findErr != nil {
			return DedupResult{}, fmt.Errorf("dedup find existing: %w", findErr)
		}
		return DedupResult{Duplicate: true, ResultMailID: existing.ResultMailID}, nil
	}
	return DedupResult{}, fmt.Errorf("dedup insert: %w", err)
}

// UpdateDedupResultMailID updates the resultMailId for a dedup record after mail creation.
func (r *Repository) UpdateDedupResultMailID(ctx context.Context, dedupID string, mailID int64) error {
	_, err := r.mailDedup.UpdateByID(ctx, dedupID, bson.M{
		"$set": bson.M{"resultMailId": mailID},
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
