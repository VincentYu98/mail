package mail

// MailKind represents the type of mail.
type MailKind string

const (
	MailKindPersonal  MailKind = "personal"
	MailKindBroadcast MailKind = "broadcast"
	MailKindNotify    MailKind = "notify"
)

// RewardItem represents a single reward attachment.
type RewardItem struct {
	ItemID int32
	Count  int64
}

// Mail represents a mail entry in a user's inbox.
type Mail struct {
	ServerID    int32
	UID         int64
	MailID      int64
	Kind        MailKind
	Source      int8
	TemplateID  int32
	Params      []string
	I18nParams  any
	Title       string
	Content     string
	Rewards     []RewardItem
	SendAtMs    int64
	ExpireAtMs  int64
	ReadAtMs    int64 // 0 = unread
	ClaimedAtMs int64 // 0 = unclaimed
	DeletedAtMs int64 // 0 = not deleted
}

// Target defines the audience for a broadcast mail.
type Target struct {
	Scope string // all / server / uid_list / segment
	Data  any
}

// Origin records the provenance of a user mail (e.g. broadcast parent).
type Origin struct {
	Type string // "broadcast"
	ID   int64  // parent broadcast mailId
}
