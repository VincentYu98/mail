package repo

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// GetBroadcastCursor returns the user's broadcast sync cursor. Returns 0 if not found.
func (r *Repository) GetBroadcastCursor(ctx context.Context, serverID int32, uid int64) (int64, error) {
	filter := bson.M{"serverId": serverID, "uid": uid}
	var doc MailboxMetaDoc
	err := r.mailboxMeta.FindOne(ctx, filter).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return doc.BroadcastCursor, nil
}

// UpdateBroadcastCursor advances the cursor using $max (never goes backward).
func (r *Repository) UpdateBroadcastCursor(ctx context.Context, serverID int32, uid int64, cursor int64, nowMs int64) error {
	filter := bson.M{"serverId": serverID, "uid": uid}
	update := bson.M{
		"$max": bson.M{"broadcastCursor": cursor},
		"$set": bson.M{"updatedAt": nowMs},
		"$setOnInsert": bson.M{
			"serverId": serverID,
			"uid":      uid,
		},
	}
	opts := options.UpdateOne().SetUpsert(true)
	_, err := r.mailboxMeta.UpdateOne(ctx, filter, update, opts)
	return err
}
