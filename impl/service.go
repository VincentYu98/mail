package impl

import (
	"github.com/vincentAlen/mail"
	"github.com/vincentAlen/mail/cache"
	"github.com/vincentAlen/mail/repo"
)

// serviceImpl implements mail.Service.
type serviceImpl struct {
	config   mail.MailConfig
	repo     *repo.Repository
	cache    *cache.Cache
	granter  mail.RewardGranter
	locker   mail.Locker
	push     mail.PushNotifier
	resolver mail.TargetResolver

	// listInboxCounter tracks per-user ListInbox calls for reconciliation.
	// In production this would use an atomic map or per-request counter.
	// For simplicity, reconciliation is triggered when the caller signals it.
}

// Option configures optional dependencies.
type Option func(*serviceImpl)

// WithPushNotifier sets the push notifier.
func WithPushNotifier(p mail.PushNotifier) Option {
	return func(s *serviceImpl) { s.push = p }
}

// WithTargetResolver sets a custom target resolver for broadcast matching.
func WithTargetResolver(r mail.TargetResolver) Option {
	return func(s *serviceImpl) { s.resolver = r }
}

// NewService creates a new mail.Service implementation.
func NewService(
	config mail.MailConfig,
	repository *repo.Repository,
	redisCache *cache.Cache,
	granter mail.RewardGranter,
	locker mail.Locker,
	opts ...Option,
) mail.Service {
	s := &serviceImpl{
		config:  config,
		repo:    repository,
		cache:   redisCache,
		granter: granter,
		locker:  locker,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// nowMs returns the current time in milliseconds.
func nowMs() int64 {
	return timeNowMs()
}
