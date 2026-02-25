package mail

// --- Player API ---

type ListInboxResponse struct {
	Mails      []Mail
	NextCursor int64 // next page cursor; 0 = no more
}

type MarkReadResponse struct {
	UpdatedMailIDs []int64
}

type ClaimResult struct {
	MailID  int64
	Success bool
	Code    ErrCode
	ErrMsg  string
}

type ClaimRewardsResponse struct {
	Results        []ClaimResult
	ClaimedMailIDs []int64
	FailedMailIDs  []int64
	Rewards        []RewardItem // aggregated rewards from successful claims
}

type DeleteMailsResponse struct {
	DeletedMailIDs []int64
}

type GetUnreadCountResponse struct {
	UnreadCount  int64
	HasClaimable bool
}

// --- Business / Internal API ---

type SendResponse struct {
	MailID int64
}

// --- Admin / GM API ---

type BatchSendResult struct {
	UID    int64
	MailID int64
	Err    string
}

type BatchSendPersonalResponse struct {
	Results      []BatchSendResult
	SuccessCount int32
	FailCount    int32
}

type QueryByRequestIdResponse struct {
	Found    bool
	Status   string // not_found / pending / done
	Scope    string
	MailID   int64
	MailKind MailKind
}

type GetBroadcastStatsResponse struct {
	TotalDelivered int64
	TotalRead      int64
	TotalClaimed   int64
	TotalDeleted   int64
}
