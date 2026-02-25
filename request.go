package mail

// --- Player API ---

type ListInboxRequest struct {
	ServerID     int32
	UID          int64
	BeforeMailID int64 // pagination cursor; 0 = start from latest
	Limit        int32 // default 50, max 100
}

type MarkReadRequest struct {
	ServerID int32
	UID      int64
	MailIDs  []int64
	All      bool
}

type ClaimRewardsRequest struct {
	ServerID int32
	UID      int64
	MailIDs  []int64
	All      bool
}

type DeleteMailsRequest struct {
	ServerID int32
	UID      int64
	MailIDs  []int64
	All      bool
}

type GetUnreadCountRequest struct {
	ServerID int32
	UID      int64
}

// --- Business / Internal API ---

type SendPersonalRequest struct {
	ServerID   int32
	RequestID  string
	UID        int64
	Kind       MailKind
	Source     int8
	TemplateID int32
	Params     []string
	I18nParams any
	Title      string
	Content    string
	Rewards    []RewardItem
	SendAtMs   int64 // 0 = now
	ExpireAtMs int64 // 0 = default 15 days
}

type SendBroadcastRequest struct {
	ServerID   int32
	RequestID  string
	Target     Target
	Kind       MailKind
	Source     int8
	TemplateID int32
	Params     []string
	I18nParams any
	Title      string
	Content    string
	Rewards    []RewardItem
	StartAtMs  int64 // 0 = immediate
	ExpireAtMs int64
}

// --- Admin / GM API ---

type RecallBroadcastRequest struct {
	ServerID int32
	MailID   int64
	Operator string
	Reason   string
}

type BatchSendPersonalRequest struct {
	ServerID   int32
	RequestID  string
	UIDs       []int64
	Kind       MailKind
	Source     int8
	TemplateID int32
	Params     []string
	I18nParams any
	Title      string
	Content    string
	Rewards    []RewardItem
	SendAtMs   int64
	ExpireAtMs int64
}

type QueryByRequestIdRequest struct {
	ServerID  int32
	Scope     string // send_personal / send_broadcast / batch_send_personal
	RequestID string
	UID       int64 // required for batch_send_personal
}

type GetBroadcastStatsRequest struct {
	ServerID int32
	MailID   int64
}
