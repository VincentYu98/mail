package repo

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// EnsureIndexes creates all required indexes. Safe to call repeatedly.
func (r *Repository) EnsureIndexes(ctx context.Context) error {
	if err := r.ensureUserMailIndexes(ctx); err != nil {
		return err
	}
	if err := r.ensureBroadcastMailIndexes(ctx); err != nil {
		return err
	}
	if err := r.ensureMailboxMetaIndexes(ctx); err != nil {
		return err
	}
	return r.ensureMailDedupIndexes(ctx)
}

func (r *Repository) ensureUserMailIndexes(ctx context.Context) error {
	models := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "serverId", Value: 1},
				{Key: "uid", Value: 1},
				{Key: "mailId", Value: 1},
			},
			Options: options.Index().SetUnique(true).SetName("uniq_server_uid_mail"),
		},
		{
			Keys: bson.D{
				{Key: "serverId", Value: 1},
				{Key: "uid", Value: 1},
				{Key: "deletedAt", Value: 1},
				{Key: "mailId", Value: -1},
			},
			Options: options.Index().SetName("idx_inbox_page"),
		},
		{
			Keys: bson.D{
				{Key: "serverId", Value: 1},
				{Key: "requestId", Value: 1},
				{Key: "mailId", Value: -1},
			},
			Options: options.Index().SetName("idx_request_lookup"),
		},
		{
			Keys:    bson.D{{Key: "purgeAt", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(0).SetName("ttl_purge"),
		},
	}
	_, err := r.userMails.Indexes().CreateMany(ctx, models)
	return err
}

func (r *Repository) ensureBroadcastMailIndexes(ctx context.Context) error {
	models := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "serverId", Value: 1},
				{Key: "mailId", Value: 1},
			},
			Options: options.Index().SetUnique(true).SetName("uniq_server_mail"),
		},
		{
			Keys: bson.D{
				{Key: "serverId", Value: 1},
				{Key: "mailId", Value: 1},
			},
			Options: options.Index().SetName("idx_cursor_scan"),
		},
		{
			Keys: bson.D{
				{Key: "serverId", Value: 1},
				{Key: "requestId", Value: 1},
				{Key: "mailId", Value: -1},
			},
			Options: options.Index().SetName("idx_request_lookup"),
		},
		{
			Keys:    bson.D{{Key: "purgeAt", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(0).SetName("ttl_purge"),
		},
	}
	_, err := r.broadcastMails.Indexes().CreateMany(ctx, models)
	return err
}

func (r *Repository) ensureMailboxMetaIndexes(ctx context.Context) error {
	models := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "serverId", Value: 1},
				{Key: "uid", Value: 1},
			},
			Options: options.Index().SetUnique(true).SetName("uniq_server_uid"),
		},
	}
	_, err := r.mailboxMeta.Indexes().CreateMany(ctx, models)
	return err
}

func (r *Repository) ensureMailDedupIndexes(ctx context.Context) error {
	models := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "purgeAt", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(0).SetName("ttl_purge"),
		},
	}
	_, err := r.mailDedup.Indexes().CreateMany(ctx, models)
	return err
}

// convenience: milliseconds to time.Time for TTL index
func msToTime(ms int64) time.Time {
	return time.UnixMilli(ms)
}
