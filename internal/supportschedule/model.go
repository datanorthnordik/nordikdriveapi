// Package supportschedule manages the local support-call rota, availability,
// booking workflow, and completed support time. It intentionally keeps Zoom
// and SageHR behind provider interfaces until those integrations are enabled.
package supportschedule

import (
	"time"

	"gorm.io/datatypes"
)

const (
	RoleAdmin   = "Admin"
	RoleManager = "Manager"

	AssignmentStatusScheduled  = "scheduled"
	AssignmentStatusUncovered  = "uncovered"
	AssignmentStatusReassigned = "reassigned"

	CallStatusAwaitingApproval = "awaiting_staff_approval"
	CallStatusScheduled        = "scheduled"
	CallStatusDeclined         = "declined"
	CallStatusCancelled        = "cancelled"
	CallStatusNeedsReschedule  = "needs_reschedule"
	CallStatusCompleted        = "completed"

	MeetingStatusPendingProvider = "pending_provider"
	MeetingStatusReady           = "ready"

	DefaultTimeZone       = "America/Toronto"
	DefaultStartTime      = "08:30"
	DefaultEndTime        = "16:30"
	DefaultHorizonDays    = 14
	DefaultDurationMinute = 30
)

// SupportScheduleSettings is a singleton configurable by managers. JSON is
// used for durations so PostgreSQL and existing SQLite test databases share
// the exact same model shape.
type SupportScheduleSettings struct {
	ID                     uint           `gorm:"primaryKey" json:"id"`
	TimeZone               string         `gorm:"not null;default:'America/Toronto'" json:"time_zone"`
	WorkdayStart           string         `gorm:"not null;default:'08:30'" json:"workday_start"`
	WorkdayEnd             string         `gorm:"not null;default:'16:30'" json:"workday_end"`
	AllowedDurationsJSON   datatypes.JSON `gorm:"type:jsonb;not null" json:"-"`
	DefaultDurationMinutes int            `gorm:"not null;default:30" json:"default_duration_minutes"`
	BookingHorizonDays     int            `gorm:"not null;default:14" json:"booking_horizon_days"`
	UpdatedByID            *uint          `json:"updated_by_id,omitempty"`
	CreatedAt              time.Time      `json:"created_at"`
	UpdatedAt              time.Time      `json:"updated_at"`
}

func (SupportScheduleSettings) TableName() string { return "support_schedule_settings" }

type SupportUser struct {
	ID        uint   `gorm:"primaryKey" json:"id"`
	FirstName string `gorm:"column:firstname" json:"firstname"`
	LastName  string `gorm:"column:lastname" json:"lastname"`
	Email     string `gorm:"column:email" json:"email"`
	Role      string `gorm:"column:role" json:"role"`
}

func (SupportUser) TableName() string { return "users" }

type SupportTeamMember struct {
	ID        uint        `gorm:"primaryKey" json:"id"`
	UserID    uint        `gorm:"uniqueIndex;not null" json:"user_id"`
	IsActive  bool        `gorm:"not null;default:true;index" json:"is_active"`
	AddedByID *uint       `json:"added_by_id,omitempty"`
	CreatedAt time.Time   `json:"created_at"`
	UpdatedAt time.Time   `json:"updated_at"`
	User      SupportUser `gorm:"foreignKey:UserID;references:ID" json:"user"`
}

func (SupportTeamMember) TableName() string { return "support_team_members" }

type SupportDailyAssignment struct {
	ID             uint         `gorm:"primaryKey" json:"id"`
	ScheduleDate   string       `gorm:"type:varchar(10);uniqueIndex;not null" json:"schedule_date"`
	AssignedUserID *uint        `gorm:"index" json:"assigned_user_id,omitempty"`
	Status         string       `gorm:"type:varchar(24);not null;index" json:"status"`
	Reason         string       `gorm:"type:text;not null;default:''" json:"reason"`
	AssignedByID   *uint        `json:"assigned_by_id,omitempty"`
	CreatedAt      time.Time    `json:"created_at"`
	UpdatedAt      time.Time    `json:"updated_at"`
	AssignedUser   *SupportUser `gorm:"foreignKey:AssignedUserID;references:ID" json:"assigned_user,omitempty"`
}

func (SupportDailyAssignment) TableName() string { return "support_daily_assignments" }

type SupportStaffUnavailability struct {
	ID          uint         `gorm:"primaryKey" json:"id"`
	UserID      *uint        `gorm:"index" json:"user_id,omitempty"`
	AllTeam     bool         `gorm:"not null;default:false;index" json:"all_team"`
	StartsAt    time.Time    `gorm:"not null;index" json:"starts_at"`
	EndsAt      time.Time    `gorm:"not null;index" json:"ends_at"`
	Reason      string       `gorm:"type:text;not null" json:"reason"`
	CreatedByID uint         `gorm:"not null" json:"created_by_id"`
	CreatedAt   time.Time    `json:"created_at"`
	UpdatedAt   time.Time    `json:"updated_at"`
	User        *SupportUser `gorm:"foreignKey:UserID;references:ID" json:"user,omitempty"`
}

func (SupportStaffUnavailability) TableName() string { return "support_staff_unavailabilities" }

type SupportCallRequest struct {
	ID                 uint         `gorm:"primaryKey" json:"id"`
	CreatedByID        uint         `gorm:"not null;index" json:"created_by_id"`
	RequestedStaffID   *uint        `gorm:"index" json:"requested_staff_id,omitempty"`
	AssignedUserID     *uint        `gorm:"index" json:"assigned_user_id,omitempty"`
	ScheduleDate       string       `gorm:"type:varchar(10);not null;index" json:"schedule_date"`
	ScheduledStart     time.Time    `gorm:"not null;index" json:"scheduled_start"`
	ScheduledEnd       time.Time    `gorm:"not null" json:"scheduled_end"`
	DurationMinutes    int          `gorm:"not null" json:"duration_minutes"`
	Status             string       `gorm:"type:varchar(32);not null;index" json:"status"`
	Subject            string       `gorm:"type:varchar(160);not null" json:"subject"`
	Message            string       `gorm:"type:text;not null;default:''" json:"message"`
	MeetingProvider    string       `gorm:"type:varchar(32);not null;default:'zoom'" json:"meeting_provider"`
	MeetingStatus      string       `gorm:"type:varchar(32);not null;default:'pending_provider'" json:"meeting_status"`
	MeetingURL         string       `gorm:"type:text;not null;default:''" json:"meeting_url"`
	ExternalMeetingID  string       `gorm:"type:text;not null;default:''" json:"external_meeting_id"`
	ApprovalNote       string       `gorm:"type:text;not null;default:''" json:"approval_note"`
	ReassignmentReason string       `gorm:"type:text;not null;default:''" json:"reassignment_reason"`
	ActualStart        *time.Time   `json:"actual_start,omitempty"`
	ActualEnd          *time.Time   `json:"actual_end,omitempty"`
	ActualMinutes      int          `gorm:"not null;default:0" json:"actual_minutes"`
	CompletedByID      *uint        `json:"completed_by_id,omitempty"`
	CompletedAt        *time.Time   `json:"completed_at,omitempty"`
	CreatedAt          time.Time    `json:"created_at"`
	UpdatedAt          time.Time    `json:"updated_at"`
	CreatedBy          SupportUser  `gorm:"foreignKey:CreatedByID;references:ID" json:"created_by"`
	RequestedStaff     *SupportUser `gorm:"foreignKey:RequestedStaffID;references:ID" json:"requested_staff,omitempty"`
	AssignedUser       *SupportUser `gorm:"foreignKey:AssignedUserID;references:ID" json:"assigned_user,omitempty"`
}

func (SupportCallRequest) TableName() string { return "support_call_requests" }

type SettingsResponse struct {
	TimeZone              string `json:"time_zone"`
	WorkdayStart          string `json:"workday_start"`
	WorkdayEnd            string `json:"workday_end"`
	AllowedDurations      []int  `json:"allowed_durations"`
	DefaultDurationMinute int    `json:"default_duration_minutes"`
	BookingHorizonDays    int    `json:"booking_horizon_days"`
}

type UpdateSettingsInput struct {
	TimeZone              string `json:"time_zone"`
	WorkdayStart          string `json:"workday_start"`
	WorkdayEnd            string `json:"workday_end"`
	AllowedDurations      []int  `json:"allowed_durations"`
	DefaultDurationMinute int    `json:"default_duration_minutes"`
}

type AvailabilitySlot struct {
	StartAt time.Time `json:"start_at"`
	EndAt   time.Time `json:"end_at"`
}

type AvailabilityResponse struct {
	Date          string             `json:"date"`
	Duration      int                `json:"duration_minutes"`
	AssignedStaff *SupportUser       `json:"assigned_staff,omitempty"`
	Slots         []AvailabilitySlot `json:"slots"`
}

type CreateCallInput struct {
	ScheduledStart   string `json:"scheduled_start"`
	DurationMinutes  int    `json:"duration_minutes"`
	Subject          string `json:"subject"`
	Message          string `json:"message"`
	RequestedStaffID *uint  `json:"requested_staff_id"`
}

type CreateUnavailabilityInput struct {
	UserID   *uint  `json:"user_id"`
	AllTeam  bool   `json:"all_team"`
	StartsAt string `json:"starts_at"`
	EndsAt   string `json:"ends_at"`
	Reason   string `json:"reason"`
}

type CompleteCallInput struct {
	ActualStart string `json:"actual_start"`
	ActualEnd   string `json:"actual_end"`
}

type ReassignInput struct {
	UserID uint   `json:"user_id"`
	Reason string `json:"reason"`
}

type SpecificApprovalInput struct {
	Approved bool   `json:"approved"`
	Note     string `json:"note"`
}

type TeamMemberInput struct {
	IsActive bool `json:"is_active"`
}

type SupportStaffOption struct {
	UserID    uint   `json:"user_id"`
	FirstName string `json:"firstname"`
	LastName  string `json:"lastname"`
}
