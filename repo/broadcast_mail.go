package repo

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// InsertBroadcastMail inserts a broadcast mail document.
func (r *Repository) InsertBroadcastMail(ctx context.Context, doc BroadcastMailDoc) error {
	_, err := r.broadcastMails.InsertOne(ctx, doc)
	return err
}

// FindBroadcastsAfterCursor returns broadcasts with mailId > cursor, ordered by mailId asc, up to limit.
// Skips recalled broadcasts.
func (r *Repository) FindBroadcastsAfterCursor(ctx context.Context, serverID int32, cursor int64, limit int32) ([]BroadcastMailDoc, error) {
	filter := bson.M{
		"serverId":   serverID,
		"mailId":     bson.M{"$gt": cursor},
		"recalledAt": nil,
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "mailId", Value: 1}}).
		SetLimit(int64(limit))

	cur, err := r.broadcastMails.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var docs []BroadcastMailDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	return docs, nil
}

// RecallBroadcast marks a broadcast as recalled.
func (r *Repository) RecallBroadcast(ctx context.Context, serverID int32, mailID int64, operator string, nowMs int64) error {
	filter := bson.M{
		"serverId":   serverID,
		"mailId":     mailID,
		"recalledAt": nil,
	}
	update := bson.M{
		"$set": bson.M{
			"recalledAt": nowMs,
			"recalledBy": operator,
		},
	}
	res, err := r.broadcastMails.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return mongo.ErrNoDocuments
	}
	return nil
}

// FindBroadcastMail finds a single broadcast mail by serverID and mailID.
func (r *Repository) FindBroadcastMail(ctx context.Context, serverID int32, mailID int64) (*BroadcastMailDoc, error) {
	filter := bson.M{"serverId": serverID, "mailId": mailID}
	var doc BroadcastMailDoc
	err := r.broadcastMails.FindOne(ctx, filter).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &doc, nil
}
