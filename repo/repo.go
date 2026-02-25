package repo

import "go.mongodb.org/mongo-driver/v2/mongo"

const (
	CollUserMails     = "user_mails"
	CollBroadcastMails = "broadcast_mails"
	CollMailboxMeta   = "mailbox_meta"
	CollMailDedup     = "mail_dedup"
)

// Repository aggregates all MongoDB collection handles.
type Repository struct {
	userMails      *mongo.Collection
	broadcastMails *mongo.Collection
	mailboxMeta    *mongo.Collection
	mailDedup      *mongo.Collection
}

// NewRepository creates a Repository from a MongoDB database handle.
func NewRepository(db *mongo.Database) *Repository {
	return &Repository{
		userMails:      db.Collection(CollUserMails),
		broadcastMails: db.Collection(CollBroadcastMails),
		mailboxMeta:    db.Collection(CollMailboxMeta),
		mailDedup:      db.Collection(CollMailDedup),
	}
}
