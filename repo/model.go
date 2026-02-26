package repo

import "go.mongodb.org/mongo-driver/v2/bson"

// UserMailDoc is the BSON document for the user_mails collection.
type UserMailDoc struct {
	ServerID   int32         `bson:"serverId"`
	UID        int64         `bson:"uid"`
	MailID     int64         `bson:"mailId"`
	RequestID  string        `bson:"requestId,omitempty"`
	Kind       string        `bson:"kind"`
	Source     int8          `bson:"source"`
	TemplateID int32         `bson:"templateId"`
	Params     []string      `bson:"params,omitempty"`
	I18nParams bson.RawValue `bson:"i18nParams,omitempty"`
	Title      string        `bson:"title,omitempty"`
	Content    string        `bson:"content,omitempty"`
	Rewards    []RewardDoc   `bson:"rewards,omitempty"`
	SendAt     int64         `bson:"sendAt"`
	ExpireAt   int64         `bson:"expireAt"`
	ReadAt     *int64        `bson:"readAt"`    // nil = unread
	ClaimedAt  *int64        `bson:"claimedAt"` // nil = unclaimed
	DeletedAt  *int64        `bson:"deletedAt"` // nil = not deleted
	PurgeAt    int64         `bson:"purgeAt"`
	Origin     *OriginDoc    `bson:"origin,omitempty"`
}

// RewardDoc is the BSON sub-document for a reward item.
type RewardDoc struct {
	ItemID int32 `bson:"itemId"`
	Count  int64 `bson:"count"`
}

// OriginDoc tracks the provenance of a user mail.
type OriginDoc struct {
	Type string `bson:"type"`
	ID   int64  `bson:"id"`
}

// BroadcastMailDoc is the BSON document for the broadcast_mails collection.
type BroadcastMailDoc struct {
	ServerID   int32         `bson:"serverId"`
	MailID     int64         `bson:"mailId"`
	RequestID  string        `bson:"requestId,omitempty"`
	Target     TargetDoc     `bson:"target"`
	Kind       string        `bson:"kind"`
	Source     int8          `bson:"source"`
	TemplateID int32         `bson:"templateId"`
	Params     []string      `bson:"params,omitempty"`
	I18nParams bson.RawValue `bson:"i18nParams,omitempty"`
	Title      string        `bson:"title,omitempty"`
	Content    string        `bson:"content,omitempty"`
	Rewards    []RewardDoc   `bson:"rewards,omitempty"`
	SendAt     int64         `bson:"sendAt"`
	ExpireAt   int64         `bson:"expireAt"`
	PurgeAt    int64         `bson:"purgeAt"`
	RecalledAt *int64        `bson:"recalledAt"`
	RecalledBy string        `bson:"recalledBy,omitempty"`
}

// TargetDoc is the BSON sub-document for broadcast targeting.
type TargetDoc struct {
	Scope string        `bson:"scope"`
	Data  bson.RawValue `bson:"data,omitempty"`
}

// MailboxMetaDoc is the BSON document for the mailbox_meta collection.
type MailboxMetaDoc struct {
	ServerID        int32 `bson:"serverId"`
	UID             int64 `bson:"uid"`
	BroadcastCursor int64 `bson:"broadcastCursor"`
	UpdatedAt       int64 `bson:"updatedAt"`
}

const (
	DedupStatusPending = "pending"
	DedupStatusDone    = "done"
)

// MailDedupDoc is the BSON document for the mail_dedup collection.
type MailDedupDoc struct {
	ID           string `bson:"_id"` // <serverId>:<scope>:<dedupKey>
	ServerID     int32  `bson:"serverId"`
	Scope        string `bson:"scope"`
	DedupKey     string `bson:"dedupKey"`
	RequestID    string `bson:"requestId"`
	ResultMailID int64  `bson:"resultMailId"`
	Status       string `bson:"status"`
	CreatedAt    int64  `bson:"createdAt"`
	PurgeAt      int64  `bson:"purgeAt"`
}
