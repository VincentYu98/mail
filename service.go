package mail

import "context"

// Service is the primary mail API for players and game services.
type Service interface {
	// --- Player API ---
	ListInbox(ctx context.Context, req ListInboxRequest) (ListInboxResponse, error)
	MarkRead(ctx context.Context, req MarkReadRequest) (MarkReadResponse, error)
	ClaimRewards(ctx context.Context, req ClaimRewardsRequest) (ClaimRewardsResponse, error)
	DeleteMails(ctx context.Context, req DeleteMailsRequest) (DeleteMailsResponse, error)
	GetUnreadCount(ctx context.Context, req GetUnreadCountRequest) (GetUnreadCountResponse, error)

	// --- Business / Internal API ---
	SendPersonal(ctx context.Context, req SendPersonalRequest) (SendResponse, error)
	SendBroadcast(ctx context.Context, req SendBroadcastRequest) (SendResponse, error)
}

// AdminService provides GM/OPS operations.
type AdminService interface {
	RecallBroadcast(ctx context.Context, req RecallBroadcastRequest) error
	BatchSendPersonal(ctx context.Context, req BatchSendPersonalRequest) (BatchSendPersonalResponse, error)
	QueryByRequestId(ctx context.Context, req QueryByRequestIdRequest) (QueryByRequestIdResponse, error)
	GetBroadcastStats(ctx context.Context, req GetBroadcastStatsRequest) (GetBroadcastStatsResponse, error)
}
