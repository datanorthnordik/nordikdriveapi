package auth

import "nordik-drive-api/internal/logs"

type AuthServicePort interface {
	CreateUser(user Auth) (*Auth, error)
	GetUser(email string) (*Auth, error)
	GetUserByID(id int) (*Auth, error)
	GetAllUsers() ([]Auth, error)
	SendOTP(email string) (*Auth, string, error)
	ResetPassword(email, code, newPassword string) (*Auth, error)
}

type LogServicePort interface {
	Log(entry logs.SystemLog, payload any) error
}

var _ AuthServicePort = (*AuthService)(nil)
var _ LogServicePort = (*logs.LogService)(nil)
