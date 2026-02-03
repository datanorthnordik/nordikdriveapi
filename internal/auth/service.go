package auth

import (
	"errors"
	"fmt"
	"log"
	"net/smtp"
	"nordik-drive-api/config"
	"nordik-drive-api/internal/util"
	"strings"
	"time"

	"gorm.io/gorm"
)

type AuthService struct {
	DB  *gorm.DB
	CFG *config.Config
}

var sendMail = smtp.SendMail

func (s *AuthService) CreateUser(user Auth) (*Auth, error) {
	if user.Role == "" {
		user.Role = "User"
	}

	if err := s.DB.Create(&user).Error; err != nil {
		// check if it's a unique constraint violation
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "unique constraint") {
			return nil, errors.New("An account with this email or phone number already exists. Please log in or use different details.")
		}
		return nil, err
	}

	return &user, nil
}

// func (s *AuthService) CreateAccess(access Access) (*Access, error) {
// 	if err := s.DB.Create(&access).Error; err != nil {
// 		return nil, err
// 	}

// 	return &access, nil
// }

func (s *AuthService) GetUser(email string) (*Auth, error) {

	var user Auth
	result := s.DB.Where("email = ?", email).First(&user)
	if result.Error != nil {
		return nil, result.Error
	}
	return &user, nil
}

func (s *AuthService) GetUserByID(id int) (*Auth, error) {
	var user Auth
	result := s.DB.Where("id = ?", id).First(&user)
	if result.Error != nil {
		return nil, result.Error
	}
	return &user, nil
}

// func (s *AuthService) GetUserByPhone(phone string) (*Auth, error) {
// 	var user Auth
// 	result := s.DB.Where("phonenumber = ?", phone).First(&user)
// 	if result.Error != nil {
// 		return nil, result.Error
// 	}
// 	return &user, nil
// }

func (s *AuthService) GetAllUsers() ([]Auth, error) {
	var users []Auth
	result := s.DB.Where("role = ?", "User").Find(&users)
	if result.Error != nil {
		return nil, result.Error
	}
	return users, nil
}

type AccessWithUser struct {
	CommunityName string    `json:"community_name"`
	Filename      *string   `json:"filename,omitempty"`
	UserID        int       `json:"user_id"`
	FirstName     string    `json:"firstname" gorm:"column:firstname"`
	LastName      string    `json:"lastname" gorm:"column:lastname"`
	Email         string    `json:"email"`
	CreatedAt     time.Time `json:"created_at"`
}

func (s *AuthService) SendOTP(email string) (*Auth, string, error) {
	// Check if user exists
	var user Auth
	if err := s.DB.Where("email = ?", email).First(&user).Error; err != nil {
		return nil, "", errors.New("user not found")
	}

	// Generate 6-digit OTP
	otp := fmt.Sprintf("%06d", util.RandomInt(100000, 999999))

	// Save to DB
	record := OTP{
		Email: email,
		Code:  otp,
	}
	if err := s.DB.Create(&record).Error; err != nil {
		return nil, "", err
	}

	from := s.CFG.GmailUser
	password := s.CFG.GmailPass
	to := []string{user.Email}
	smtpHost := "smtp.gmail.com"
	smtpPort := "587"

	subject := "OTP to change password"
	body := fmt.Sprintf(
		"Hi there,\n\n"+
			"Your OTP to change the password is: %s\n\n"+
			"This code will expire in 10 minutes.\n\n"+
			"Thank you.",
		otp,
	)

	// 2. Format the email message.
	// The email header and body must be formatted correctly with \r\n
	// to ensure it's a valid email message.
	message := []byte(fmt.Sprintf(
		"To: %s\r\n"+
			"Subject: %s\r\n"+
			"\r\n"+
			"%s",
		user.Email,
		subject,
		body,
	))

	// 3. Authenticate with the SMTP server.
	// This is the required step to prove your application has permission.
	auth := smtp.PlainAuth("", from, password, smtpHost)

	// 4. Send the email.
	// This function connects to the server, authenticates, and sends the message.
	err := sendMail(smtpHost+":"+smtpPort, auth, from, to, message)
	if err != nil {
		log.Printf("Error sending email to %s: %v\n", user.Email, err)
		return nil, "", errors.New("failed to send OTP email")
	}

	return &user, otp, nil
}

// Verify OTP and reset password
func (s *AuthService) ResetPassword(email, code, newPassword string) (*Auth, error) {
	// Get latest OTP for email
	var otp OTP
	if err := s.DB.Where("email = ? AND code = ?", email, code).
		Order("created_at desc").First(&otp).Error; err != nil {
		return nil, errors.New("invalid OTP")
	}

	var user Auth
	if err := s.DB.Where("email = ?", email).First(&user).Error; err != nil {
		return nil, errors.New("user not found")
	}

	// Check if OTP is older than 10 minutes
	if time.Since(otp.CreatedAt) > 10*time.Minute {
		return nil, errors.New("OTP expired")
	}

	// Update user password
	hashed, err := util.HashPassword(newPassword)
	if err != nil {
		return nil, err
	}
	if err := s.DB.Model(&Auth{}).Where("email = ?", email).
		Update("password", hashed).Error; err != nil {
		return nil, err
	}

	return &user, nil
}

// func (s *AuthService) GetAccessRequests(userID int) ([]AccessWithUser, error) {
// 	var userRoles []UserRole
// 	if err := s.DB.Where("user_id = ?", userID).Find(&userRoles).Error; err != nil {
// 		return nil, err
// 	}

// 	if len(userRoles) == 0 {
// 		return nil, nil
// 	}

// 	isAdmin := false
// 	managedCommunities := []string{}

// 	for _, ur := range userRoles {
// 		if ur.Role == "Admin" {
// 			isAdmin = true
// 			break
// 		} else if ur.Role == "Manager" && ur.CommunityName != nil {
// 			managedCommunities = append(managedCommunities, *ur.CommunityName)
// 		}
// 	}

// 	var accessRequests []AccessWithUser
// 	query := s.DB.Table("access").
// 		Select("access.community_name, access.filename, access.user_id, users.firstname, users.lastname, users.email, access.created_at").
// 		Joins("JOIN users ON users.id = access.user_id").
// 		Where("access.status = ?", "pending")

// 	if !isAdmin && len(managedCommunities) > 0 {
// 		// Filter by manager communities
// 		query = query.Where("access.community_name IN ?", managedCommunities)
// 	} else if !isAdmin {
// 		// Normal user: no access
// 		return nil, nil
// 	}

// 	if err := query.Find(&accessRequests).Error; err != nil {
// 		return nil, err
// 	}

// 	return accessRequests, nil
// }

// func (s *AuthService) GetUserAccess(userID int) ([]FileAccessResponse, error) {
// 	// Step 1: Load user roles
// 	var roles []UserRole
// 	if err := s.DB.Where("user_id = ?", userID).Find(&roles).Error; err != nil {
// 		return nil, err
// 	}

// 	isAdmin := false
// 	for _, r := range roles {
// 		if r.Role == "Admin" {
// 			isAdmin = true
// 			break
// 		}
// 	}

// 	// Step 2: If Admin → return files with "all" communities
// 	if isAdmin {
// 		var files []file.File
// 		if err := s.DB.Find(&files).Error; err != nil {
// 			return nil, err
// 		}

// 		res := make([]FileAccessResponse, 0, len(files))
// 		for _, f := range files {
// 			res = append(res, FileAccessResponse{
// 				Filename:    f.Filename,
// 				Communities: []string{"all"},
// 			})
// 		}
// 		return res, nil
// 	}

// 	// Step 3: Non-admin → return from Access table grouped by filename
// 	var access []Access
// 	if err := s.DB.Where("user_id = ?", userID).Find(&access).Error; err != nil {
// 		return nil, err
// 	}

// 	grouped := map[string][]string{}
// 	for _, a := range access {
// 		if a.Filename != nil {
// 			grouped[*a.Filename] = append(grouped[*a.Filename], a.CommunityName)
// 		}
// 	}

// 	res := []FileAccessResponse{}
// 	for filename, communities := range grouped {
// 		res = append(res, FileAccessResponse{
// 			Filename:    filename,
// 			Communities: communities,
// 		})
// 	}

// 	return res, nil
// }

// func (s *AuthService) ProcessRequests(requests []RequestAction) error {
// 	for _, r := range requests {
// 		if r.Status == "approved" {
// 			// Delete existing pending row
// 			if err := s.DB.Where("community_name = ? AND user_id = ? AND status = ?", r.CommunityName, r.UserID, "pending").
// 				Delete(&Access{}).Error; err != nil {
// 				return err
// 			}

// 			// Insert approved row
// 			newAccess := Access{
// 				Filename:      &r.Filename,
// 				CommunityName: r.CommunityName,
// 				UserID:        r.UserID,
// 				Status:        "approved",
// 			}
// 			if err := s.DB.Create(&newAccess).Error; err != nil {
// 				return err
// 			}

// 			// Add entry into user_roles if not exists
// 			var existing UserRole
// 			if err := s.DB.Where("user_id = ? AND community_name = ? AND role = ?", r.UserID, r.CommunityName, r.Role).First(&existing).Error; err != nil {
// 				if err == gorm.ErrRecordNotFound {
// 					newUserRole := UserRole{
// 						UserID:        r.UserID,
// 						CommunityName: &r.CommunityName,
// 						Role:          r.Role,
// 					}
// 					_ = s.DB.Create(&newUserRole).Error
// 				}
// 			}

// 		} else if r.Status == "rejected" {
// 			if err := s.DB.Model(&Access{}).
// 				Where("filename = ? AND community_name = ? AND user_id = ?", r.Filename, r.CommunityName, r.UserID).
// 				Update("status", "rejected").Error; err != nil {
// 				return err
// 			}
// 		}
// 	}

// 	return nil
// }
