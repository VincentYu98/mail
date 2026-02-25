package impl

import (
	"github.com/vincentAlen/mail"
	"github.com/vincentAlen/mail/repo"
)

// adminServiceImpl implements mail.AdminService.
type adminServiceImpl struct {
	svc  *serviceImpl
	repo *repo.Repository
}

// NewAdminService creates an AdminService that shares the same repo and cache as the Service.
func NewAdminService(svc mail.Service) mail.AdminService {
	s := svc.(*serviceImpl)
	return &adminServiceImpl{
		svc:  s,
		repo: s.repo,
	}
}
