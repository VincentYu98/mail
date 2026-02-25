package mail

// MailConfig holds all tunable parameters for the mail module.
type MailConfig struct {
	DefaultPageLimit        int32
	MaxPageLimit            int32
	BroadcastSyncBatchSize  int32
	DefaultMailTTLMs        int64
	PurgeGraceMs            int64
	MaxVisibleMails         int32
	UserLockTTLMs           int64
	MaxRewardsPerMail       int32
	MaxBatchSendSize        int32
	MaxClaimBatchSize       int32
	MaxDeleteBatchSize      int32
	UnreadReconcileInterval int32
}

// DefaultConfig returns sensible defaults for production use.
func DefaultConfig() MailConfig {
	return MailConfig{
		DefaultPageLimit:        50,
		MaxPageLimit:            100,
		BroadcastSyncBatchSize:  200,
		DefaultMailTTLMs:        1_296_000_000, // 15 days
		PurgeGraceMs:            604_800_000,   // 7 days
		MaxVisibleMails:         200,
		UserLockTTLMs:           5000,
		MaxRewardsPerMail:       20,
		MaxBatchSendSize:        500,
		MaxClaimBatchSize:       50,
		MaxDeleteBatchSize:      50,
		UnreadReconcileInterval: 10,
	}
}
