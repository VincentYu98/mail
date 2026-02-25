package mail

import "fmt"

// ErrCode represents a business error code.
type ErrCode int32

const (
	ErrOK              ErrCode = 0
	ErrInvalidParam    ErrCode = 1001
	ErrMailNotFound    ErrCode = 1002
	ErrMailExpired     ErrCode = 1003
	ErrAlreadyClaimed  ErrCode = 1004
	ErrNoRewards       ErrCode = 1005
	ErrLockFailed      ErrCode = 1006
	ErrRewardGrantFail ErrCode = 1007
	ErrDuplicate       ErrCode = 1008
	ErrMailNotActive   ErrCode = 1011
	ErrInternal        ErrCode = 9999
)

// MailError is a structured business error.
type MailError struct {
	Code    ErrCode
	Message string
}

func (e *MailError) Error() string {
	return fmt.Sprintf("mail error %d: %s", e.Code, e.Message)
}

func NewError(code ErrCode, msg string) *MailError {
	return &MailError{Code: code, Message: msg}
}

func Errorf(code ErrCode, format string, args ...any) *MailError {
	return &MailError{Code: code, Message: fmt.Sprintf(format, args...)}
}
