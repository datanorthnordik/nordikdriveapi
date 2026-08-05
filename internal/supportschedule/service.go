package supportschedule

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
)

var (
	ErrForbidden           = errors.New("support schedule access is forbidden")
	ErrNotFound            = errors.New("support schedule record not found")
	ErrInvalidInput        = errors.New("invalid support schedule input")
	ErrUnavailable         = errors.New("requested support time is unavailable")
	ErrNoSupportStaff      = errors.New("no support team member is available")
	ErrInvalidStatusChange = errors.New("invalid support call status change")
)

type SupportScheduleMailer interface {
	Send(to []string, subject, body string) error
}

type Service struct {
	DB                     *gorm.DB
	Mailer                 SupportScheduleMailer
	Clock                  func() time.Time
	MeetingProvider        MeetingProvider
	HRAvailabilityProvider HRAvailabilityProvider
}

func (s *Service) now() time.Time {
	if s != nil && s.Clock != nil {
		return s.Clock()
	}
	return time.Now()
}

func (s *Service) location(settings *SupportScheduleSettings) (*time.Location, error) {
	name := DefaultTimeZone
	if settings != nil && strings.TrimSpace(settings.TimeZone) != "" {
		name = strings.TrimSpace(settings.TimeZone)
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid time zone", ErrInvalidInput)
	}
	return loc, nil
}

func defaultSettings() SupportScheduleSettings {
	durations, _ := json.Marshal([]int{30, 60})
	return SupportScheduleSettings{
		ID:                     1,
		TimeZone:               DefaultTimeZone,
		WorkdayStart:           DefaultStartTime,
		WorkdayEnd:             DefaultEndTime,
		AllowedDurationsJSON:   durations,
		DefaultDurationMinutes: DefaultDurationMinute,
		BookingHorizonDays:     DefaultHorizonDays,
	}
}

func (s *Service) settings() (*SupportScheduleSettings, error) {
	if s == nil || s.DB == nil {
		return nil, errors.New("db not initialized")
	}
	var settings SupportScheduleSettings
	err := s.DB.First(&settings, 1).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		settings = defaultSettings()
		if err := s.DB.Create(&settings).Error; err != nil {
			return nil, err
		}
		return &settings, nil
	}
	if err != nil {
		return nil, err
	}
	return &settings, nil
}

func durationList(settings *SupportScheduleSettings) []int {
	if settings == nil {
		return []int{30, 60}
	}
	var durations []int
	if err := json.Unmarshal(settings.AllowedDurationsJSON, &durations); err != nil || len(durations) == 0 {
		return []int{30, 60}
	}
	return normalizeDurations(durations)
}

func normalizeDurations(input []int) []int {
	seen := map[int]struct{}{}
	result := make([]int, 0, len(input))
	for _, duration := range input {
		if duration < 15 || duration > 240 || duration%15 != 0 {
			continue
		}
		if _, exists := seen[duration]; exists {
			continue
		}
		seen[duration] = struct{}{}
		result = append(result, duration)
	}
	sort.Ints(result)
	return result
}

func settingsResponse(settings *SupportScheduleSettings) *SettingsResponse {
	return &SettingsResponse{
		TimeZone:              settings.TimeZone,
		WorkdayStart:          settings.WorkdayStart,
		WorkdayEnd:            settings.WorkdayEnd,
		AllowedDurations:      durationList(settings),
		DefaultDurationMinute: settings.DefaultDurationMinutes,
		BookingHorizonDays:    settings.BookingHorizonDays,
	}
}

func (s *Service) GetSettings() (*SettingsResponse, error) {
	settings, err := s.settings()
	if err != nil {
		return nil, err
	}
	return settingsResponse(settings), nil
}

func (s *Service) UpdateSettings(actorID uint, input UpdateSettingsInput) (*SettingsResponse, error) {
	if err := s.requireManager(actorID); err != nil {
		return nil, err
	}
	settings, err := s.settings()
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(input.TimeZone) != "" {
		settings.TimeZone = strings.TrimSpace(input.TimeZone)
	}
	if _, err := s.location(settings); err != nil {
		return nil, err
	}
	if strings.TrimSpace(input.WorkdayStart) != "" {
		settings.WorkdayStart = strings.TrimSpace(input.WorkdayStart)
	}
	if strings.TrimSpace(input.WorkdayEnd) != "" {
		settings.WorkdayEnd = strings.TrimSpace(input.WorkdayEnd)
	}
	if _, _, err := serviceWindow(settings, s.now()); err != nil {
		return nil, err
	}
	if input.AllowedDurations != nil {
		durations := normalizeDurations(input.AllowedDurations)
		if len(durations) == 0 {
			return nil, fmt.Errorf("%w: allowed durations must be 15 to 240 minutes in 15-minute increments", ErrInvalidInput)
		}
		encoded, _ := json.Marshal(durations)
		settings.AllowedDurationsJSON = encoded
	}
	if input.DefaultDurationMinute > 0 {
		settings.DefaultDurationMinutes = input.DefaultDurationMinute
	}
	if !containsDuration(durationList(settings), settings.DefaultDurationMinutes) {
		return nil, fmt.Errorf("%w: default duration must be an allowed duration", ErrInvalidInput)
	}
	settings.UpdatedByID = &actorID
	if err := s.DB.Save(settings).Error; err != nil {
		return nil, err
	}
	if err := s.EnsureRollingSchedule(); err != nil {
		return nil, err
	}
	return settingsResponse(settings), nil
}

func containsDuration(durations []int, want int) bool {
	for _, duration := range durations {
		if duration == want {
			return true
		}
	}
	return false
}

func serviceWindow(settings *SupportScheduleSettings, now time.Time) (time.Time, time.Time, error) {
	loc, err := time.LoadLocation(settings.TimeZone)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	localNow := now.In(loc)
	start, err := parseDateAndTime(localNow.Format("2006-01-02"), settings.WorkdayStart, loc)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	end, err := parseDateAndTime(localNow.Format("2006-01-02"), settings.WorkdayEnd, loc)
	if err != nil || !end.After(start) {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: workday end must be after workday start", ErrInvalidInput)
	}
	return start, end, nil
}

func parseDateAndTime(date, hhmm string, loc *time.Location) (time.Time, error) {
	date = strings.TrimSpace(date)
	// PostgreSQL DATE values normally scan as YYYY-MM-DD. SQLite and some
	// drivers may return legacy date-only values as RFC3339 instead.
	if len(date) >= len("2006-01-02") {
		date = date[:len("2006-01-02")]
	}
	value := date + " " + strings.TrimSpace(hhmm)
	parsed, err := time.ParseInLocation("2006-01-02 15:04", value, loc)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid date or time", ErrInvalidInput)
	}
	return parsed, nil
}

func scheduleDateFor(t time.Time, loc *time.Location) string { return t.In(loc).Format("2006-01-02") }

func isWeekday(date time.Time) bool {
	return date.Weekday() != time.Saturday && date.Weekday() != time.Sunday
}

// EnsureRollingSchedule makes the current business day plus the next thirteen
// calendar days available. Because it runs daily and frequently, the horizon
// advances one day at a time after the initial two-week seed.
func (s *Service) EnsureRollingSchedule() error {
	settings, err := s.settings()
	if err != nil {
		return err
	}
	loc, err := s.location(settings)
	if err != nil {
		return err
	}
	start := s.now().In(loc)
	start = time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, loc)
	for offset := 0; offset < settings.BookingHorizonDays; offset++ {
		date := start.AddDate(0, 0, offset)
		if !isWeekday(date) {
			continue
		}
		if err := s.ensureAssignment(scheduleDateFor(date, loc), nil, ""); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) ensureAssignment(date string, assignedBy *uint, reason string) error {
	var existing SupportDailyAssignment
	err := s.DB.Where("schedule_date = ?", date).First(&existing).Error
	if err == nil {
		return nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	userID, err := s.pickFairStaff(date, nil)
	assignment := SupportDailyAssignment{ScheduleDate: date, Status: AssignmentStatusUncovered, Reason: reason, AssignedByID: assignedBy}
	if err == nil {
		assignment.AssignedUserID = &userID
		assignment.Status = AssignmentStatusScheduled
	} else if !errors.Is(err, ErrNoSupportStaff) {
		return err
	}
	return s.DB.Create(&assignment).Error
}

func (s *Service) ListSchedule(from, to string) ([]SupportDailyAssignment, error) {
	if err := s.EnsureRollingSchedule(); err != nil {
		return nil, err
	}
	settings, err := s.settings()
	if err != nil {
		return nil, err
	}
	loc, _ := s.location(settings)
	if strings.TrimSpace(from) == "" {
		from = scheduleDateFor(s.now(), loc)
	}
	if strings.TrimSpace(to) == "" {
		to = scheduleDateFor(s.now().In(loc).AddDate(0, 0, settings.BookingHorizonDays-1), loc)
	}
	var assignments []SupportDailyAssignment
	err = s.DB.Preload("AssignedUser").Where("schedule_date >= ? AND schedule_date <= ?", from, to).Order("schedule_date ASC").Find(&assignments).Error
	return assignments, err
}

func (s *Service) ListTeam(actorID uint) ([]SupportTeamMember, error) {
	if err := s.requireManager(actorID); err != nil {
		return nil, err
	}
	var members []SupportTeamMember
	err := s.DB.Preload("User").Order("is_active DESC, user_id ASC").Find(&members).Error
	return members, err
}

// ListSelectableStaff is intentionally limited to the fields a caller needs
// to request a particular support person; email addresses remain private.
func (s *Service) ListSelectableStaff() ([]SupportStaffOption, error) {
	var members []SupportTeamMember
	if err := s.DB.Preload("User").Where("is_active = ?", true).Order("user_id ASC").Find(&members).Error; err != nil {
		return nil, err
	}
	options := make([]SupportStaffOption, 0, len(members))
	for _, member := range members {
		options = append(options, SupportStaffOption{UserID: member.UserID, FirstName: member.User.FirstName, LastName: member.User.LastName})
	}
	return options, nil
}

func (s *Service) SetTeamMember(actorID, userID uint, active bool) (*SupportTeamMember, error) {
	if err := s.requireManager(actorID); err != nil {
		return nil, err
	}
	if _, err := s.user(userID); err != nil {
		return nil, err
	}
	member := SupportTeamMember{UserID: userID, IsActive: active, AddedByID: &actorID}
	if err := s.DB.Where("user_id = ?", userID).Assign(member).FirstOrCreate(&member).Error; err != nil {
		return nil, err
	}
	if !active {
		if err := s.RebalanceSchedule(actorID, "team member deactivated"); err != nil {
			return nil, err
		}
	}
	return &member, nil
}

func (s *Service) user(userID uint) (*SupportUser, error) {
	var user SupportUser
	if err := s.DB.First(&user, userID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &user, nil
}

func (s *Service) isManager(userID uint) (bool, error) {
	user, err := s.user(userID)
	if err != nil {
		return false, err
	}
	role := strings.ToLower(strings.TrimSpace(user.Role))
	return role == strings.ToLower(RoleAdmin) || role == strings.ToLower(RoleManager), nil
}

func (s *Service) requireManager(userID uint) error {
	ok, err := s.isManager(userID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrForbidden
	}
	return nil
}

func (s *Service) isActiveTeamMember(userID uint) (bool, error) {
	var member SupportTeamMember
	err := s.DB.Where("user_id = ? AND is_active = ?", userID, true).First(&member).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	return err == nil, err
}

type fairStaffCandidate struct {
	UserID       uint
	WorkedMinute int64
	LastAssigned string
}

func (s *Service) pickFairStaff(date string, excluded map[uint]struct{}) (uint, error) {
	settings, err := s.settings()
	if err != nil {
		return 0, err
	}
	loc, err := s.location(settings)
	if err != nil {
		return 0, err
	}
	dayStart, err := parseDateAndTime(date, "00:00", loc)
	if err != nil {
		return 0, err
	}
	dayEnd := dayStart.AddDate(0, 0, 1)
	var members []SupportTeamMember
	if err := s.DB.Preload("User").Where("is_active = ?", true).Find(&members).Error; err != nil {
		return 0, err
	}
	candidates := make([]fairStaffCandidate, 0, len(members))
	for _, member := range members {
		if _, skip := excluded[member.UserID]; skip {
			continue
		}
		unavailable, err := s.isUnavailable(member.UserID, dayStart, dayEnd)
		if err != nil || unavailable {
			if err != nil {
				return 0, err
			}
			continue
		}
		candidate := fairStaffCandidate{UserID: member.UserID}
		if err := s.DB.Model(&SupportCallRequest{}).
			Where("assigned_user_id = ? AND status = ?", member.UserID, CallStatusCompleted).
			Select("COALESCE(SUM(actual_minutes), 0)").Scan(&candidate.WorkedMinute).Error; err != nil {
			return 0, err
		}
		var previous SupportDailyAssignment
		if err := s.DB.Where("assigned_user_id = ? AND schedule_date < ?", member.UserID, date).
			Order("schedule_date DESC").First(&previous).Error; err == nil {
			candidate.LastAssigned = previous.ScheduleDate
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, err
		}
		candidates = append(candidates, candidate)
	}
	if len(candidates) == 0 {
		return 0, ErrNoSupportStaff
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].WorkedMinute != candidates[j].WorkedMinute {
			return candidates[i].WorkedMinute < candidates[j].WorkedMinute
		}
		if candidates[i].LastAssigned != candidates[j].LastAssigned {
			return candidates[i].LastAssigned < candidates[j].LastAssigned
		}
		return candidates[i].UserID < candidates[j].UserID
	})
	return candidates[0].UserID, nil
}
