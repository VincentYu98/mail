package repo

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// InsertUserMail inserts a single user mail document.
func (r *Repository) InsertUserMail(ctx context.Context, doc UserMailDoc) error {
	_, err := r.userMails.InsertOne(ctx, doc)
	return err
}

// BulkUpsertUserMails inserts multiple user mail documents using ordered:false bulk write.
// Uses $setOnInsert so duplicate key conflicts are no-ops. Returns the count of inserted documents.
func (r *Repository) BulkUpsertUserMails(ctx context.Context, docs []UserMailDoc) (int64, error) {
	if len(docs) == 0 {
		return 0, nil
	}

	models := make([]mongo.WriteModel, len(docs))
	for i, doc := range docs {
		filter := bson.M{
			"serverId": doc.ServerID,
			"uid":      doc.UID,
			"mailId":   doc.MailID,
		}
		models[i] = mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(bson.M{"$setOnInsert": doc}).
			SetUpsert(true)
	}

	opts := options.BulkWrite().SetOrdered(false)
	res, err := r.userMails.BulkWrite(ctx, models, opts)
	if err != nil {
		// With ordered:false, partial success is possible. Check for bulk write exceptions.
		if bwe, ok := err.(mongo.BulkWriteException); ok {
			// Count only non-duplicate-key errors as real failures
			realErrors := 0
			for _, we := range bwe.WriteErrors {
				if !mongo.IsDuplicateKeyError(we) {
					realErrors++
				}
			}
			if realErrors == 0 {
				// All errors were duplicate key — treat as success
				if res != nil {
					return res.UpsertedCount, nil
				}
				return 0, nil
			}
		}
		return 0, err
	}
	return res.UpsertedCount, nil
}

// ListUserMails returns mails for the inbox page query.
// Filters: deletedAt == null, sendAt <= nowMs, expireAt > nowMs
// If beforeMailID > 0, only returns mails with mailId < beforeMailID.
// Ordered by mailId desc, limited to `limit`.
func (r *Repository) ListUserMails(ctx context.Context, serverID int32, uid int64, beforeMailID int64, limit int32, nowMs int64) ([]UserMailDoc, error) {
	filter := bson.M{
		"serverId":  serverID,
		"uid":       uid,
		"deletedAt": nil,
		"sendAt":    bson.M{"$lte": nowMs},
		"expireAt":  bson.M{"$gt": nowMs},
	}
	if beforeMailID > 0 {
		filter["mailId"] = bson.M{"$lt": beforeMailID}
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "mailId", Value: -1}}).
		SetLimit(int64(limit))

	cur, err := r.userMails.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var docs []UserMailDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	return docs, nil
}

// FindUserMailsByIDs returns user mails for the given mailIDs.
func (r *Repository) FindUserMailsByIDs(ctx context.Context, serverID int32, uid int64, mailIDs []int64) ([]UserMailDoc, error) {
	filter := bson.M{
		"serverId": serverID,
		"uid":      uid,
		"mailId":   bson.M{"$in": mailIDs},
	}
	cur, err := r.userMails.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var docs []UserMailDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	return docs, nil
}

// FindUserMailIDByRequestID returns the latest mailId for a send_personal request.
func (r *Repository) FindUserMailIDByRequestID(ctx context.Context, serverID int32, requestID string) (int64, bool, error) {
	filter := bson.M{
		"serverId":  serverID,
		"requestId": requestID,
	}
	opts := options.FindOne().
		SetProjection(bson.M{"mailId": 1}).
		SetSort(bson.D{{Key: "mailId", Value: -1}})

	var row struct {
		MailID int64 `bson:"mailId"`
	}
	err := r.userMails.FindOne(ctx, filter, opts).Decode(&row)
	if err == mongo.ErrNoDocuments {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return row.MailID, true, nil
}

// MarkRead sets readAt for the given mails where readAt is currently null.
// Returns the list of mailIDs that were actually updated.
func (r *Repository) MarkRead(ctx context.Context, serverID int32, uid int64, mailIDs []int64, nowMs int64) ([]int64, error) {
	filter := bson.M{
		"serverId": serverID,
		"uid":      uid,
		"mailId":   bson.M{"$in": mailIDs},
		"readAt":   nil,
	}
	update := bson.M{"$set": bson.M{"readAt": nowMs}}

	res, err := r.userMails.UpdateMany(ctx, filter, update)
	if err != nil {
		return nil, err
	}

	if res.ModifiedCount == 0 {
		return nil, nil
	}

	// Re-query to get the exact IDs that were updated
	return r.findMailIDsWithReadAt(ctx, serverID, uid, mailIDs, nowMs)
}

// MarkReadAll sets readAt for all unread, non-deleted, active mails.
func (r *Repository) MarkReadAll(ctx context.Context, serverID int32, uid int64, nowMs int64) ([]int64, error) {
	filter := bson.M{
		"serverId":  serverID,
		"uid":       uid,
		"readAt":    nil,
		"deletedAt": nil,
		"sendAt":    bson.M{"$lte": nowMs},
		"expireAt":  bson.M{"$gt": nowMs},
	}
	update := bson.M{"$set": bson.M{"readAt": nowMs}}

	res, err := r.userMails.UpdateMany(ctx, filter, update)
	if err != nil {
		return nil, err
	}
	if res.ModifiedCount == 0 {
		return nil, nil
	}

	// Find the IDs that were marked
	readFilter := bson.M{
		"serverId":  serverID,
		"uid":       uid,
		"readAt":    nowMs,
		"deletedAt": nil,
	}
	return r.findMailIDsByFilter(ctx, readFilter)
}

func (r *Repository) findMailIDsWithReadAt(ctx context.Context, serverID int32, uid int64, candidateIDs []int64, readAtMs int64) ([]int64, error) {
	filter := bson.M{
		"serverId": serverID,
		"uid":      uid,
		"mailId":   bson.M{"$in": candidateIDs},
		"readAt":   readAtMs,
	}
	return r.findMailIDsByFilter(ctx, filter)
}

func (r *Repository) findMailIDsByFilter(ctx context.Context, filter bson.M) ([]int64, error) {
	opts := options.Find().SetProjection(bson.M{"mailId": 1})
	cur, err := r.userMails.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var results []struct {
		MailID int64 `bson:"mailId"`
	}
	if err := cur.All(ctx, &results); err != nil {
		return nil, err
	}

	ids := make([]int64, len(results))
	for i, r := range results {
		ids[i] = r.MailID
	}
	return ids, nil
}

// MarkClaimed sets claimedAt and readAt for a single mail. Returns true if updated.
func (r *Repository) MarkClaimed(ctx context.Context, serverID int32, uid int64, mailID int64, nowMs int64) (bool, error) {
	filter := bson.M{
		"serverId":  serverID,
		"uid":       uid,
		"mailId":    mailID,
		"claimedAt": nil,
	}
	update := bson.M{
		"$set": bson.M{
			"claimedAt": nowMs,
			"readAt":    nowMs,
		},
	}
	res, err := r.userMails.UpdateOne(ctx, filter, update)
	if err != nil {
		return false, err
	}
	return res.ModifiedCount > 0, nil
}

// SoftDeleteMails sets deletedAt for given mails. Returns the IDs actually deleted and how many were unread.
func (r *Repository) SoftDeleteMails(ctx context.Context, serverID int32, uid int64, mailIDs []int64, nowMs int64) (deletedIDs []int64, unreadDeleted int64, err error) {
	// First count how many unread mails will be deleted
	unreadFilter := bson.M{
		"serverId":  serverID,
		"uid":       uid,
		"mailId":    bson.M{"$in": mailIDs},
		"deletedAt": nil,
		"readAt":    nil,
	}
	unreadCount, err := r.userMails.CountDocuments(ctx, unreadFilter)
	if err != nil {
		return nil, 0, err
	}

	// Perform soft delete
	filter := bson.M{
		"serverId":  serverID,
		"uid":       uid,
		"mailId":    bson.M{"$in": mailIDs},
		"deletedAt": nil,
	}
	update := bson.M{"$set": bson.M{"deletedAt": nowMs}}
	res, err := r.userMails.UpdateMany(ctx, filter, update)
	if err != nil {
		return nil, 0, err
	}
	if res.ModifiedCount == 0 {
		return nil, 0, nil
	}

	// Find which IDs were actually deleted
	deletedFilter := bson.M{
		"serverId":  serverID,
		"uid":       uid,
		"mailId":    bson.M{"$in": mailIDs},
		"deletedAt": nowMs,
	}
	ids, err := r.findMailIDsByFilter(ctx, deletedFilter)
	if err != nil {
		return nil, 0, err
	}
	return ids, unreadCount, nil
}

// SoftDeleteAllMails soft-deletes all non-deleted, active mails.
func (r *Repository) SoftDeleteAllMails(ctx context.Context, serverID int32, uid int64, nowMs int64) (deletedIDs []int64, unreadDeleted int64, err error) {
	unreadFilter := bson.M{
		"serverId":  serverID,
		"uid":       uid,
		"deletedAt": nil,
		"readAt":    nil,
		"sendAt":    bson.M{"$lte": nowMs},
		"expireAt":  bson.M{"$gt": nowMs},
	}
	unreadCount, err := r.userMails.CountDocuments(ctx, unreadFilter)
	if err != nil {
		return nil, 0, err
	}

	filter := bson.M{
		"serverId":  serverID,
		"uid":       uid,
		"deletedAt": nil,
	}
	update := bson.M{"$set": bson.M{"deletedAt": nowMs}}
	res, err := r.userMails.UpdateMany(ctx, filter, update)
	if err != nil {
		return nil, 0, err
	}
	if res.ModifiedCount == 0 {
		return nil, 0, nil
	}

	deletedFilter := bson.M{
		"serverId":  serverID,
		"uid":       uid,
		"deletedAt": nowMs,
	}
	ids, err := r.findMailIDsByFilter(ctx, deletedFilter)
	if err != nil {
		return nil, 0, err
	}
	return ids, unreadCount, nil
}

// CountUnread counts unread, non-deleted, active mails for reconciliation.
func (r *Repository) CountUnread(ctx context.Context, serverID int32, uid int64, nowMs int64) (int64, error) {
	filter := bson.M{
		"serverId":  serverID,
		"uid":       uid,
		"readAt":    nil,
		"deletedAt": nil,
		"sendAt":    bson.M{"$lte": nowMs},
		"expireAt":  bson.M{"$gt": nowMs},
	}
	return r.userMails.CountDocuments(ctx, filter)
}

// HasClaimable checks whether the user has any claimable mails (has rewards, not claimed, not expired).
func (r *Repository) HasClaimable(ctx context.Context, serverID int32, uid int64, nowMs int64) (bool, error) {
	filter := bson.M{
		"serverId":  serverID,
		"uid":       uid,
		"claimedAt": nil,
		"deletedAt": nil,
		"sendAt":    bson.M{"$lte": nowMs},
		"expireAt":  bson.M{"$gt": nowMs},
		"rewards":   bson.M{"$exists": true, "$ne": bson.A{}},
	}
	count, err := r.userMails.CountDocuments(ctx, filter, options.Count().SetLimit(1))
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// FindClaimableMails returns mails that can be claimed (has rewards, not claimed, active, not deleted).
func (r *Repository) FindClaimableMails(ctx context.Context, serverID int32, uid int64, mailIDs []int64, nowMs int64) ([]UserMailDoc, error) {
	filter := bson.M{
		"serverId":  serverID,
		"uid":       uid,
		"claimedAt": nil,
		"deletedAt": nil,
		"sendAt":    bson.M{"$lte": nowMs},
		"expireAt":  bson.M{"$gt": nowMs},
		"rewards":   bson.M{"$exists": true, "$ne": bson.A{}},
	}
	if len(mailIDs) > 0 {
		filter["mailId"] = bson.M{"$in": mailIDs}
	}

	cur, err := r.userMails.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var docs []UserMailDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	return docs, nil
}

// SoftDeleteRecalledBroadcastMails soft-deletes user mails originating from a recalled broadcast
// that haven't been claimed yet.
func (r *Repository) SoftDeleteRecalledBroadcastMails(ctx context.Context, serverID int32, broadcastMailID int64, nowMs int64) (int64, error) {
	filter := bson.M{
		"serverId":    serverID,
		"origin.type": "broadcast",
		"origin.id":   broadcastMailID,
		"claimedAt":   nil,
		"deletedAt":   nil,
	}
	update := bson.M{"$set": bson.M{"deletedAt": nowMs}}
	res, err := r.userMails.UpdateMany(ctx, filter, update)
	if err != nil {
		return 0, err
	}
	return res.ModifiedCount, nil
}

// CountBroadcastStats aggregates delivery statistics for a broadcast mail.
func (r *Repository) CountBroadcastStats(ctx context.Context, serverID int32, broadcastMailID int64) (delivered, read, claimed, deleted int64, err error) {
	pipeline := bson.A{
		bson.M{"$match": bson.M{
			"serverId":    serverID,
			"origin.type": "broadcast",
			"origin.id":   broadcastMailID,
		}},
		bson.M{"$group": bson.M{
			"_id":       nil,
			"delivered": bson.M{"$sum": 1},
			"read":      bson.M{"$sum": bson.M{"$cond": bson.A{bson.M{"$ne": bson.A{"$readAt", nil}}, 1, 0}}},
			"claimed":   bson.M{"$sum": bson.M{"$cond": bson.A{bson.M{"$ne": bson.A{"$claimedAt", nil}}, 1, 0}}},
			"deleted":   bson.M{"$sum": bson.M{"$cond": bson.A{bson.M{"$ne": bson.A{"$deletedAt", nil}}, 1, 0}}},
		}},
	}

	cur, err := r.userMails.Aggregate(ctx, pipeline)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	defer cur.Close(ctx)

	var results []struct {
		Delivered int64 `bson:"delivered"`
		Read      int64 `bson:"read"`
		Claimed   int64 `bson:"claimed"`
		Deleted   int64 `bson:"deleted"`
	}
	if err := cur.All(ctx, &results); err != nil {
		return 0, 0, 0, 0, err
	}
	if len(results) == 0 {
		return 0, 0, 0, 0, nil
	}
	r0 := results[0]
	return r0.Delivered, r0.Read, r0.Claimed, r0.Deleted, nil
}
