package supportschedule

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrForbidden           = errors.New("support schedule access is forbidden")
	ErrNotFound            = errors.New("support schedule record not found")
	ErrInvalidInput        = errors.New("invalid support schedule input")
	ErrUnavailable         = errors.New("requested support time is unavailable")
	ErrNoSupportStaff      = errors.New("no support admin is available")
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
		return nil, errors.New("support schedule database is not initialized")
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
	seen := make(map[int]struct{}, len(input))
	result := make([]int, 0, len(input))
	for _, duration := range input {
		if duration < 15 || duration > 240 || duration%15 != 0 {
			continue
		}
		if _, ok := seen[duration]; ok {
			continue
		}
		seen[duration] = struct{}{}
		result = append(result, duration)
	}
	sort.Ints(result)
	return result
}

func containsDuration(durations []int, wanted int) bool {
	for _, duration := range durations {
		if duration == wanted {
			return true
		}
	}
	return false
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
	if _, _, err := windowForDate(settings, scheduleDateFor(s.now(), mustLocation(settings))); err != nil {
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
	return settingsResponse(settings), s.EnsureRollingSchedule()
}

func mustLocation(settings *SupportScheduleSettings) *time.Location {
	loc, err := time.LoadLocation(settings.TimeZone)
	if err != nil {
		return time.Local
	}
	return loc
}

func parseDateAndTime(date, hhmm string, loc *time.Location) (time.Time, error) {
	date = strings.TrimSpace(date)
	if len(date) >= len("2006-01-02") {
		date = date[:len("2006-01-02")]
	}
	parsed, err := time.ParseInLocation("2006-01-02 15:04", date+" "+strings.TrimSpace(hhmm), loc)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid date or time", ErrInvalidInput)
	}
	return parsed, nil
}

func windowForDate(settings *SupportScheduleSettings, date string) (time.Time, time.Time, error) {
	loc, err := time.LoadLocation(settings.TimeZone)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	start, err := parseDateAndTime(date, settings.WorkdayStart, loc)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	end, err := parseDateAndTime(date, settings.WorkdayEnd, loc)
	if err != nil || !end.After(start) {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: invalid support-hours window", ErrInvalidInput)
	}
	return start, end, nil
}

func scheduleDateFor(t time.Time, loc *time.Location) string { return t.In(loc).Format("2006-01-02") }

// EnsureRollingSchedule creates exactly one assignment record for each of the
// next 14 calendar days. Repeated invocations only fill a genuinely missing
// date, which makes startup and the daily job idempotent.
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
	for offset := 0; offset < DefaultHorizonDays; offset++ {
		if err := s.ensureAssignment(scheduleDateFor(start.AddDate(0, 0, offset), loc)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) ensureAssignment(date string) error {
	return s.DB.Transaction(func(tx *gorm.DB) error {
		var existing SupportAssignment
		if err := tx.Where("assignment_date = ?", date).First(&existing).Error; err == nil {
			return nil
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		assignment := SupportAssignment{AssignmentDate: date, AssignmentSource: AssignmentSourceUncovered}
		assigneeID, err := s.pickFairStaffTx(tx, date, nil)
		if err == nil {
			assignment.PrimaryAssigneeID = &assigneeID
			assignment.AssignmentSource = AssignmentSourceAutomatic
		} else if !errors.Is(err, ErrNoSupportStaff) {
			return err
		}
		result := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "assignment_date"}},
			DoNothing: true,
		}).Create(&assignment)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected > 0 {
			return s.recordAudit(tx, "assignment", &assignment.ID, "automatic_daily_assignment", nil, map[string]interface{}{
				"assignment_date": date, "primary_assignee_id": assignment.PrimaryAssigneeID,
			})
		}
		return nil
	})
}

func (s *Service) ListSchedule(from, to string) ([]SupportAssignment, error) {
	if err := s.EnsureRollingSchedule(); err != nil {
		return nil, err
	}
	settings, err := s.settings()
	if err != nil {
		return nil, err
	}
	loc, err := s.location(settings)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(from) == "" {
		from = scheduleDateFor(s.now(), loc)
	}
	if strings.TrimSpace(to) == "" {
		to = scheduleDateFor(s.now().In(loc).AddDate(0, 0, DefaultHorizonDays-1), loc)
	}
	var assignments []SupportAssignment
	err = s.DB.Preload("PrimaryAssignee").Preload("PreviousAssignee").
		Where("assignment_date >= ? AND assignment_date <= ?", from, to).
		Order("assignment_date ASC").Find(&assignments).Error
	return assignments, err
}

func (s *Service) ListScheduleFor(actorID uint, from, to string) ([]SupportAssignment, error) {
	if err := s.requireManager(actorID); err != nil {
		return nil, err
	}
	return s.ListSchedule(from, to)
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
	return strings.EqualFold(strings.TrimSpace(user.Role), RoleManager), nil
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

func (s *Service) isSupportAdmin(userID uint) (bool, error) {
	user, err := s.user(userID)
	if err != nil {
		return false, err
	}
	if !strings.EqualFold(strings.TrimSpace(user.Role), RoleAdmin) {
		return false, nil
	}
	var member SupportTeamMember
	err = s.DB.Where("user_id = ? AND is_active = ?", userID, true).First(&member).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (s *Service) requireSupportAdmin(userID uint) error {
	ok, err := s.isSupportAdmin(userID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrForbidden
	}
	return nil
}

func (s *Service) ListTeam(actorID uint) ([]SupportTeamMember, error) {
	if err := s.requireManager(actorID); err != nil {
		return nil, err
	}
	var members []SupportTeamMember
	err := s.DB.Preload("User").Order("is_active DESC, user_id ASC").Find(&members).Error
	return members, err
}

func (s *Service) ListSelectableStaff() ([]SupportStaffOption, error) {
	var members []SupportTeamMember
	if err := s.DB.Preload("User").Where("is_active = ?", true).Order("user_id ASC").Find(&members).Error; err != nil {
		return nil, err
	}
	result := make([]SupportStaffOption, 0, len(members))
	for _, member := range members {
		if strings.EqualFold(member.User.Role, RoleAdmin) {
			result = append(result, SupportStaffOption{UserID: member.UserID, FirstName: member.User.FirstName, LastName: member.User.LastName})
		}
	}
	return result, nil
}

func (s *Service) SetTeamMember(actorID, userID uint, active bool) (*SupportTeamMember, error) {
	if err := s.requireManager(actorID); err != nil {
		return nil, err
	}
	user, err := s.user(userID)
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(user.Role, RoleAdmin) {
		return nil, fmt.Errorf("%w: only Admin-role users can be support admins", ErrInvalidInput)
	}
	member := SupportTeamMember{UserID: userID, IsActive: active, AddedByID: &actorID}
	if err := s.DB.Where("user_id = ?", userID).Assign(member).FirstOrCreate(&member).Error; err != nil {
		return nil, err
	}
	if err := s.recordAudit(s.DB, "team_member", &member.ID, "support_admin_membership_changed", &actorID, map[string]interface{}{"user_id": userID, "is_active": active}); err != nil {
		return nil, err
	}
	if err := s.RunScheduledMaintenance(); err != nil {
		return nil, err
	}
	return &member, nil
}

type fairStaffCandidate struct {
	UserID         uint
	WorkedMinutes  int64
	AssignedDays   int64
	LastAssignedAt string
}

func (s *Service) pickFairStaff(date string, excluded map[uint]struct{}) (uint, error) {
	return s.pickFairStaffTx(s.DB, date, excluded)
}

func (s *Service) pickFairStaffTx(tx *gorm.DB, date string, excluded map[uint]struct{}) (uint, error) {
	candidates, err := s.fairStaffCandidatesTx(tx, date, excluded)
	if err != nil {
		return 0, err
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].WorkedMinutes != candidates[j].WorkedMinutes {
			return candidates[i].WorkedMinutes < candidates[j].WorkedMinutes
		}
		if candidates[i].AssignedDays != candidates[j].AssignedDays {
			return candidates[i].AssignedDays < candidates[j].AssignedDays
		}
		if candidates[i].LastAssignedAt != candidates[j].LastAssignedAt {
			return candidates[i].LastAssignedAt < candidates[j].LastAssignedAt
		}
		return candidates[i].UserID < candidates[j].UserID
	})
	return candidates[0].UserID, nil
}

func (s *Service) fairStaffCandidatesTx(tx *gorm.DB, date string, excluded map[uint]struct{}) ([]fairStaffCandidate, error) {
	var members []SupportTeamMember
	if err := tx.Preload("User").Where("is_active = ?", true).Find(&members).Error; err != nil {
		return nil, err
	}
	candidates := make([]fairStaffCandidate, 0, len(members))
	for _, member := range members {
		if _, skip := excluded[member.UserID]; skip || !strings.EqualFold(member.User.Role, RoleAdmin) {
			continue
		}
		unavailable, err := s.fullDayUnavailableTx(tx, member.UserID, date)
		if err != nil {
			return nil, err
		}
		if unavailable {
			continue
		}
		candidate := fairStaffCandidate{UserID: member.UserID}
		if err := tx.Model(&SupportCall{}).
			Where("assigned_staff_id = ? AND status = ?", member.UserID, RequestStatusCompleted).
			Select("COALESCE(SUM(actual_duration_minutes), 0)").Scan(&candidate.WorkedMinutes).Error; err != nil {
			return nil, err
		}
		if err := tx.Model(&SupportAssignment{}).Where("primary_assignee_id = ?", member.UserID).Count(&candidate.AssignedDays).Error; err != nil {
			return nil, err
		}
		var previous SupportAssignment
		if err := tx.Where("primary_assignee_id = ? AND assignment_date < ?", member.UserID, date).
			Order("assignment_date DESC").First(&previous).Error; err == nil {
			candidate.LastAssignedAt = previous.AssignmentDate
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	if len(candidates) == 0 {
		return nil, ErrNoSupportStaff
	}
	return candidates, nil
}

func (s *Service) ListFairnessStats(actorID uint) ([]FairnessStat, error) {
	if err := s.requireManager(actorID); err != nil {
		return nil, err
	}
	candidates, err := s.fairStaffCandidatesTx(s.DB, "9999-12-31", nil)
	if err != nil {
		if errors.Is(err, ErrNoSupportStaff) {
			return []FairnessStat{}, nil
		}
		return nil, err
	}
	var members []SupportTeamMember
	if err := s.DB.Preload("User").Where("is_active = ?", true).Find(&members).Error; err != nil {
		return nil, err
	}
	users := make(map[uint]SupportUser, len(members))
	for _, member := range members {
		users[member.UserID] = member.User
	}
	stats := make([]FairnessStat, 0, len(candidates))
	for _, candidate := range candidates {
		user := users[candidate.UserID]
		stats = append(stats, FairnessStat{
			Staff:              SupportStaffOption{UserID: candidate.UserID, FirstName: user.FirstName, LastName: user.LastName},
			ActualMinutes:      candidate.WorkedMinutes,
			ActualHours:        float64(candidate.WorkedMinutes) / 60,
			AssignedDays:       candidate.AssignedDays,
			LastAssignmentDate: candidate.LastAssignedAt,
		})
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].Staff.UserID < stats[j].Staff.UserID })
	return stats, nil
}

func (s *Service) ListAuditLog(actorID uint) ([]SupportScheduleAuditLog, error) {
	if err := s.requireManager(actorID); err != nil {
		return nil, err
	}
	var logs []SupportScheduleAuditLog
	return logs, s.DB.Order("created_at DESC, id DESC").Limit(500).Find(&logs).Error
}

func (s *Service) fullDayUnavailableTx(tx *gorm.DB, staffID uint, date string) (bool, error) {
	var count int64
	err := tx.Model(&StaffAvailability{}).
		Where("staff_id = ? AND availability_date = ? AND full_day_unavailable = ?", staffID, date, true).
		Count(&count).Error
	return count > 0, err
}

func (s *Service) recordAudit(tx *gorm.DB, entityType string, entityID *uint, action string, actorID *uint, details map[string]interface{}) error {
	encoded, err := json.Marshal(details)
	if err != nil {
		return err
	}
	return tx.Create(&SupportScheduleAuditLog{EntityType: entityType, EntityID: entityID, Action: action, ActorID: actorID, Details: encoded}).Error
}

func (s *Service) RunScheduledMaintenance() error {
	if err := s.EnsureRollingSchedule(); err != nil {
		return err
	}
	return s.rebalanceUnavailableAssignments("daily availability check")
}

func (s *Service) RebalanceSchedule(actorID uint, reason string) error {
	if err := s.requireManager(actorID); err != nil {
		return err
	}
	return s.rebalanceUnavailableAssignments(reason)
}

func (s *Service) rebalanceUnavailableAssignments(reason string) error {
	settings, err := s.settings()
	if err != nil {
		return err
	}
	loc, err := s.location(settings)
	if err != nil {
		return err
	}
	from := scheduleDateFor(s.now(), loc)
	to := scheduleDateFor(s.now().In(loc).AddDate(0, 0, DefaultHorizonDays-1), loc)
	var assignments []SupportAssignment
	if err := s.DB.Where("assignment_date >= ? AND assignment_date <= ?", from, to).Find(&assignments).Error; err != nil {
		return err
	}
	for _, assignment := range assignments {
		if assignment.PrimaryAssigneeID == nil {
			if err := s.reassignUnavailableDay(assignment.AssignmentDate, reason); err != nil {
				return err
			}
			continue
		}
		unavailable, err := s.fullDayUnavailableTx(s.DB, *assignment.PrimaryAssigneeID, assignment.AssignmentDate)
		if err != nil {
			return err
		}
		if unavailable {
			if err := s.reassignUnavailableDay(assignment.AssignmentDate, reason); err != nil {
				return err
			}
		}
	}
	return nil
}
