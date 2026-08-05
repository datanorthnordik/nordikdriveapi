package supportschedule

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
)

// ListAvailability returns bookable starts for the daily on-call person. A
// selected staff member is shown only for their own approval workflow; their
// off-rota choice never changes the daily on-call assignment.
func (s *Service) ListAvailability(date string, duration int, requestedStaffID *uint) (*AvailabilityResponse, error) {
	settings, err := s.settings()
	if err != nil {
		return nil, err
	}
	if duration == 0 {
		duration = settings.DefaultDurationMinutes
	}
	if !containsDuration(durationList(settings), duration) {
		return nil, fmt.Errorf("%w: duration is not enabled", ErrInvalidInput)
	}
	loc, err := s.location(settings)
	if err != nil {
		return nil, err
	}
	dateStart, err := parseDateAndTime(date, "00:00", loc)
	if err != nil || !isWeekday(dateStart) {
		return nil, fmt.Errorf("%w: date must be a weekday", ErrInvalidInput)
	}
	date = scheduleDateFor(dateStart, loc)
	today := scheduleDateFor(s.now(), loc)
	lastDate := scheduleDateFor(s.now().In(loc).AddDate(0, 0, settings.BookingHorizonDays-1), loc)
	if date < today || date > lastDate {
		return nil, fmt.Errorf("%w: date is outside the booking horizon", ErrInvalidInput)
	}
	if err := s.EnsureRollingSchedule(); err != nil {
		return nil, err
	}

	response := &AvailabilityResponse{Date: date, Duration: duration, Slots: []AvailabilitySlot{}}
	var assigneeID uint
	if requestedStaffID != nil {
		active, err := s.isActiveTeamMember(*requestedStaffID)
		if err != nil {
			return nil, err
		}
		if !active {
			return nil, fmt.Errorf("%w: requested staff member is not active", ErrInvalidInput)
		}
		staff, err := s.user(*requestedStaffID)
		if err != nil {
			return nil, err
		}
		assigneeID, response.AssignedStaff = *requestedStaffID, staff
	} else {
		var assignment SupportDailyAssignment
		if err := s.DB.Preload("AssignedUser").Where("schedule_date = ?", date).First(&assignment).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return response, nil
			}
			return nil, err
		}
		if assignment.AssignedUserID == nil || assignment.Status == AssignmentStatusUncovered {
			return response, nil
		}
		assigneeID, response.AssignedStaff = *assignment.AssignedUserID, assignment.AssignedUser
	}

	start, end, err := windowForDate(settings, date)
	if err != nil {
		return nil, err
	}
	for candidate := start; !candidate.Add(time.Duration(duration) * time.Minute).After(end); candidate = candidate.Add(time.Duration(duration) * time.Minute) {
		candidateEnd := candidate.Add(time.Duration(duration) * time.Minute)
		if s.now().In(loc).After(candidate) || s.slotUnavailable(assigneeID, candidate, candidateEnd) {
			continue
		}
		response.Slots = append(response.Slots, AvailabilitySlot{StartAt: candidate, EndAt: candidateEnd})
	}
	return response, nil
}

func windowForDate(settings *SupportScheduleSettings, date string) (time.Time, time.Time, error) {
	loc, err := time.LoadLocation(settings.TimeZone)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	start, err := parseDateAndTime(date, settings.WorkdayStart, loc)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: schedule date %q", err, date)
	}
	end, err := parseDateAndTime(date, settings.WorkdayEnd, loc)
	if err != nil || !end.After(start) {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: invalid workday window", ErrInvalidInput)
	}
	return start, end, nil
}

func (s *Service) CreateCall(actorID uint, input CreateCallInput) (*SupportCallRequest, error) {
	if _, err := s.user(actorID); err != nil {
		return nil, err
	}
	settings, err := s.settings()
	if err != nil {
		return nil, err
	}
	if input.DurationMinutes == 0 {
		input.DurationMinutes = settings.DefaultDurationMinutes
	}
	if !containsDuration(durationList(settings), input.DurationMinutes) {
		return nil, fmt.Errorf("%w: duration is not enabled", ErrInvalidInput)
	}
	if len(strings.TrimSpace(input.Subject)) == 0 || len(strings.TrimSpace(input.Subject)) > 160 {
		return nil, fmt.Errorf("%w: subject is required and must be at most 160 characters", ErrInvalidInput)
	}
	if len(strings.TrimSpace(input.Message)) > 4000 {
		return nil, fmt.Errorf("%w: message must be at most 4000 characters", ErrInvalidInput)
	}
	loc, err := s.location(settings)
	if err != nil {
		return nil, err
	}
	start, err := time.Parse(time.RFC3339, strings.TrimSpace(input.ScheduledStart))
	if err != nil {
		return nil, fmt.Errorf("%w: scheduled_start must be RFC3339", ErrInvalidInput)
	}
	start = start.In(loc)
	date := scheduleDateFor(start, loc)
	end := start.Add(time.Duration(input.DurationMinutes) * time.Minute)
	workdayStart, workdayEnd, err := windowForDate(settings, date)
	if err != nil || !isWeekday(start) || start.Before(workdayStart) || end.After(workdayEnd) {
		return nil, fmt.Errorf("%w: call must fall inside a weekday support window", ErrInvalidInput)
	}
	if !start.After(s.now().In(loc)) {
		return nil, fmt.Errorf("%w: call must be in the future", ErrInvalidInput)
	}
	lastDate := scheduleDateFor(s.now().In(loc).AddDate(0, 0, settings.BookingHorizonDays-1), loc)
	if date > lastDate {
		return nil, fmt.Errorf("%w: date is outside the booking horizon", ErrInvalidInput)
	}
	if err := s.EnsureRollingSchedule(); err != nil {
		return nil, err
	}

	var assignee uint
	status := CallStatusScheduled
	requestedStaffID := input.RequestedStaffID
	if requestedStaffID != nil {
		active, err := s.isActiveTeamMember(*requestedStaffID)
		if err != nil {
			return nil, err
		}
		if !active {
			return nil, fmt.Errorf("%w: requested staff member is not active", ErrInvalidInput)
		}
		assignee = *requestedStaffID
		status = CallStatusAwaitingApproval
	} else {
		var assignment SupportDailyAssignment
		if err := s.DB.Where("schedule_date = ?", date).First(&assignment).Error; err != nil {
			return nil, err
		}
		if assignment.AssignedUserID == nil || assignment.Status == AssignmentStatusUncovered {
			return nil, ErrNoSupportStaff
		}
		assignee = *assignment.AssignedUserID
	}
	if s.slotUnavailable(assignee, start, end) {
		return nil, ErrUnavailable
	}

	call := &SupportCallRequest{
		CreatedByID: actorID, RequestedStaffID: requestedStaffID, AssignedUserID: &assignee,
		ScheduleDate: date, ScheduledStart: start, ScheduledEnd: end, DurationMinutes: input.DurationMinutes,
		Status: status, Subject: strings.TrimSpace(input.Subject), Message: strings.TrimSpace(input.Message),
		MeetingProvider: "zoom", MeetingStatus: MeetingStatusPendingProvider,
	}
	if err := s.DB.Create(call).Error; err != nil {
		return nil, err
	}
	if err := s.preloadCall(call); err != nil {
		return nil, err
	}
	// Zoom is deliberately not called until credentials and an implementation
	// are configured. The persisted pending_provider state is the safe handoff.
	s.sendCallNotification(call)
	return call, nil
}

func (s *Service) ListCalls(actorID uint, scope string) ([]SupportCallRequest, error) {
	query := s.DB.Preload("CreatedBy").Preload("RequestedStaff").Preload("AssignedUser").Order("scheduled_start ASC")
	switch scope {
	case "mine", "":
		query = query.Where("created_by_id = ? OR assigned_user_id = ?", actorID, actorID)
	case "manage":
		if err := s.requireManager(actorID); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("%w: unsupported call scope", ErrInvalidInput)
	}
	var calls []SupportCallRequest
	return calls, query.Find(&calls).Error
}

func (s *Service) ApproveSpecificCall(actorID, callID uint, input SpecificApprovalInput) (*SupportCallRequest, error) {
	call, err := s.call(callID)
	if err != nil {
		return nil, err
	}
	if call.RequestedStaffID == nil || *call.RequestedStaffID != actorID || call.Status != CallStatusAwaitingApproval {
		return nil, ErrForbidden
	}
	call.ApprovalNote = strings.TrimSpace(input.Note)
	if input.Approved {
		if call.AssignedUserID == nil || s.slotUnavailableExcept(*call.AssignedUserID, call.ScheduledStart, call.ScheduledEnd, call.ID) {
			return nil, ErrUnavailable
		}
		call.Status = CallStatusScheduled
	} else {
		call.Status = CallStatusDeclined
	}
	if err := s.DB.Save(call).Error; err != nil {
		return nil, err
	}
	if err := s.preloadCall(call); err != nil {
		return nil, err
	}
	s.sendCallNotification(call)
	return call, nil
}

func (s *Service) CompleteCall(actorID, callID uint, input CompleteCallInput) (*SupportCallRequest, error) {
	call, err := s.call(callID)
	if err != nil {
		return nil, err
	}
	manager, err := s.isManager(actorID)
	if err != nil {
		return nil, err
	}
	if call.AssignedUserID == nil || (*call.AssignedUserID != actorID && !manager) {
		return nil, ErrForbidden
	}
	if call.Status != CallStatusScheduled {
		return nil, ErrInvalidStatusChange
	}
	start, err := time.Parse(time.RFC3339, strings.TrimSpace(input.ActualStart))
	if err != nil {
		return nil, fmt.Errorf("%w: actual_start must be RFC3339", ErrInvalidInput)
	}
	end, err := time.Parse(time.RFC3339, strings.TrimSpace(input.ActualEnd))
	if err != nil || !end.After(start) {
		return nil, fmt.Errorf("%w: actual_end must be after actual_start", ErrInvalidInput)
	}
	minutes := int(end.Sub(start).Minutes())
	if minutes < 1 || minutes > 12*60 {
		return nil, fmt.Errorf("%w: actual duration must be from 1 minute to 12 hours", ErrInvalidInput)
	}
	now := s.now()
	call.ActualStart, call.ActualEnd, call.ActualMinutes = &start, &end, minutes
	call.CompletedByID, call.CompletedAt, call.Status = &actorID, &now, CallStatusCompleted
	if err := s.DB.Save(call).Error; err != nil {
		return nil, err
	}
	return call, s.preloadCall(call)
}

func (s *Service) ReassignCall(actorID, callID uint, input ReassignInput) (*SupportCallRequest, error) {
	if err := s.requireManager(actorID); err != nil {
		return nil, err
	}
	if strings.TrimSpace(input.Reason) == "" {
		return nil, fmt.Errorf("%w: reassignment reason is required", ErrInvalidInput)
	}
	active, err := s.isActiveTeamMember(input.UserID)
	if err != nil {
		return nil, err
	}
	if !active {
		return nil, fmt.Errorf("%w: assignee must be an active team member", ErrInvalidInput)
	}
	call, err := s.call(callID)
	if err != nil {
		return nil, err
	}
	if call.Status == CallStatusCompleted || call.Status == CallStatusCancelled {
		return nil, ErrInvalidStatusChange
	}
	if s.slotUnavailableExcept(input.UserID, call.ScheduledStart, call.ScheduledEnd, call.ID) {
		return nil, ErrUnavailable
	}
	call.AssignedUserID, call.Status = &input.UserID, CallStatusScheduled
	call.ReassignmentReason = strings.TrimSpace(input.Reason)
	if err := s.DB.Save(call).Error; err != nil {
		return nil, err
	}
	if err := s.preloadCall(call); err != nil {
		return nil, err
	}
	s.sendCallNotification(call)
	return call, nil
}

func (s *Service) ReassignDay(actorID uint, date string, input ReassignInput) (*SupportDailyAssignment, error) {
	if err := s.requireManager(actorID); err != nil {
		return nil, err
	}
	if strings.TrimSpace(input.Reason) == "" {
		return nil, fmt.Errorf("%w: reassignment reason is required", ErrInvalidInput)
	}
	active, err := s.isActiveTeamMember(input.UserID)
	if err != nil {
		return nil, err
	}
	if !active {
		return nil, fmt.Errorf("%w: assignee must be active", ErrInvalidInput)
	}
	settings, err := s.settings()
	if err != nil {
		return nil, err
	}
	start, end, err := windowForDate(settings, date)
	if err != nil {
		return nil, err
	}
	fullyUnavailable, err := s.unavailableForWholeWindow(input.UserID, start, end)
	if err != nil {
		return nil, err
	}
	if fullyUnavailable {
		return nil, ErrUnavailable
	}
	var assignment SupportDailyAssignment
	if err := s.DB.Where("schedule_date = ?", date).First(&assignment).Error; err != nil {
		return nil, err
	}
	assignment.AssignedUserID, assignment.AssignedByID = &input.UserID, &actorID
	assignment.Status, assignment.Reason = AssignmentStatusReassigned, strings.TrimSpace(input.Reason)
	if err := s.DB.Save(&assignment).Error; err != nil {
		return nil, err
	}
	return &assignment, s.DB.Preload("AssignedUser").First(&assignment, assignment.ID).Error
}

func (s *Service) CreateUnavailability(actorID uint, input CreateUnavailabilityInput) (*SupportStaffUnavailability, error) {
	if strings.TrimSpace(input.Reason) == "" || len(strings.TrimSpace(input.Reason)) > 1000 {
		return nil, fmt.Errorf("%w: a reason of at most 1000 characters is required", ErrInvalidInput)
	}
	if input.AllTeam {
		if err := s.requireManager(actorID); err != nil {
			return nil, err
		}
	} else {
		if input.UserID == nil {
			input.UserID = &actorID
		}
		manager, err := s.isManager(actorID)
		if err != nil {
			return nil, err
		}
		if *input.UserID != actorID && !manager {
			return nil, ErrForbidden
		}
		active, err := s.isActiveTeamMember(*input.UserID)
		if err != nil {
			return nil, err
		}
		if !active {
			return nil, fmt.Errorf("%w: staff member is not active", ErrInvalidInput)
		}
	}
	settings, err := s.settings()
	if err != nil {
		return nil, err
	}
	loc, err := s.location(settings)
	if err != nil {
		return nil, err
	}
	start, err := time.Parse(time.RFC3339, strings.TrimSpace(input.StartsAt))
	if err != nil {
		return nil, fmt.Errorf("%w: starts_at must be RFC3339", ErrInvalidInput)
	}
	end, err := time.Parse(time.RFC3339, strings.TrimSpace(input.EndsAt))
	if err != nil || !end.After(start) {
		return nil, fmt.Errorf("%w: ends_at must be after starts_at", ErrInvalidInput)
	}
	start, end = start.In(loc), end.In(loc)
	if !isWeekday(start) || !isWeekday(end.Add(-time.Nanosecond)) {
		return nil, fmt.Errorf("%w: unavailability can only be recorded Monday to Friday", ErrInvalidInput)
	}
	record := &SupportStaffUnavailability{UserID: input.UserID, AllTeam: input.AllTeam, StartsAt: start, EndsAt: end, Reason: strings.TrimSpace(input.Reason), CreatedByID: actorID}
	if err := s.DB.Create(record).Error; err != nil {
		return nil, err
	}
	if err := s.rebalanceSchedule("staff availability changed"); err != nil {
		return nil, err
	}
	return record, nil
}

func (s *Service) ListUnavailability(actorID uint) ([]SupportStaffUnavailability, error) {
	manager, err := s.isManager(actorID)
	if err != nil {
		return nil, err
	}
	query := s.DB.Preload("User").Where("ends_at >= ?", s.now().AddDate(0, 0, -1)).Order("starts_at ASC")
	if !manager {
		query = query.Where("user_id = ?", actorID)
	}
	var records []SupportStaffUnavailability
	return records, query.Find(&records).Error
}

func (s *Service) RunScheduledMaintenance() error {
	if err := s.EnsureRollingSchedule(); err != nil {
		return err
	}
	return s.rebalanceSchedule("scheduled availability check")
}

// RebalanceSchedule is called when team availability changes. It only moves a
// daily rota assignment when the member cannot cover the whole workday. Calls
// that conflict with new availability are surfaced as needs_reschedule.
func (s *Service) RebalanceSchedule(actorID uint, reason string) error {
	if actorID != 0 {
		if err := s.requireManager(actorID); err != nil {
			return err
		}
	}
	return s.rebalanceSchedule(reason)
}

func (s *Service) rebalanceSchedule(reason string) error {
	if err := s.EnsureRollingSchedule(); err != nil {
		return err
	}
	settings, err := s.settings()
	if err != nil {
		return err
	}
	loc, err := s.location(settings)
	if err != nil {
		return err
	}
	from := scheduleDateFor(s.now(), loc)
	to := scheduleDateFor(s.now().In(loc).AddDate(0, 0, settings.BookingHorizonDays-1), loc)
	var assignments []SupportDailyAssignment
	if err := s.DB.Where("schedule_date >= ? AND schedule_date <= ?", from, to).Find(&assignments).Error; err != nil {
		return err
	}
	for i := range assignments {
		assignment := &assignments[i]
		if assignment.AssignedUserID == nil {
			if replacement, err := s.pickFairStaff(assignment.ScheduleDate, nil); err == nil {
				assignment.AssignedUserID, assignment.Status, assignment.Reason = &replacement, AssignmentStatusReassigned, reason
				if err := s.DB.Save(assignment).Error; err != nil {
					return err
				}
			}
			continue
		}
		start, end, err := windowForDate(settings, assignment.ScheduleDate)
		if err != nil {
			return err
		}
		unavailable, err := s.unavailableForWholeWindow(*assignment.AssignedUserID, start, end)
		if err != nil {
			return err
		}
		if unavailable {
			excluded := map[uint]struct{}{*assignment.AssignedUserID: {}}
			replacement, pickErr := s.pickFairStaff(assignment.ScheduleDate, excluded)
			if errors.Is(pickErr, ErrNoSupportStaff) {
				assignment.AssignedUserID, assignment.Status, assignment.Reason = nil, AssignmentStatusUncovered, reason
			} else if pickErr != nil {
				return pickErr
			} else {
				assignment.AssignedUserID, assignment.Status, assignment.Reason = &replacement, AssignmentStatusReassigned, reason
			}
			if err := s.DB.Save(assignment).Error; err != nil {
				return err
			}
		}
	}
	var calls []SupportCallRequest
	if err := s.DB.Where("scheduled_start >= ? AND status IN ?", s.now(), []string{CallStatusScheduled, CallStatusAwaitingApproval}).Find(&calls).Error; err != nil {
		return err
	}
	for i := range calls {
		call := &calls[i]
		if call.AssignedUserID != nil && s.slotUnavailableExcept(*call.AssignedUserID, call.ScheduledStart, call.ScheduledEnd, call.ID) {
			call.Status = CallStatusNeedsReschedule
			call.ReassignmentReason = reason
			if err := s.DB.Save(call).Error; err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Service) call(callID uint) (*SupportCallRequest, error) {
	var call SupportCallRequest
	if err := s.DB.First(&call, callID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &call, nil
}

func (s *Service) preloadCall(call *SupportCallRequest) error {
	return s.DB.Preload("CreatedBy").Preload("RequestedStaff").Preload("AssignedUser").First(call, call.ID).Error
}

func (s *Service) isUnavailable(userID uint, start, end time.Time) (bool, error) {
	var count int64
	err := s.DB.Model(&SupportStaffUnavailability{}).
		Where("(all_team = ? OR user_id = ?) AND starts_at < ? AND ends_at > ?", true, userID, end, start).
		Count(&count).Error
	return count > 0, err
}

// unavailableForWholeWindow differs from a slot check: a lunch-hour absence
// removes only that slot, while coverage of the entire support day triggers a
// fair reassignment of that day's on-call person.
func (s *Service) unavailableForWholeWindow(userID uint, start, end time.Time) (bool, error) {
	var records []SupportStaffUnavailability
	if err := s.DB.Where("(all_team = ? OR user_id = ?) AND starts_at < ? AND ends_at > ?", true, userID, end, start).Find(&records).Error; err != nil {
		return false, err
	}
	sort.Slice(records, func(i, j int) bool { return records[i].StartsAt.Before(records[j].StartsAt) })
	coveredUntil := start
	for _, record := range records {
		if record.StartsAt.After(coveredUntil) {
			return false, nil
		}
		if record.EndsAt.After(coveredUntil) {
			coveredUntil = record.EndsAt
		}
		if !coveredUntil.Before(end) {
			return true, nil
		}
	}
	return false, nil
}

func (s *Service) slotUnavailable(userID uint, start, end time.Time) bool {
	return s.slotUnavailableExcept(userID, start, end, 0)
}

func (s *Service) slotUnavailableExcept(userID uint, start, end time.Time, exceptCallID uint) bool {
	unavailable, err := s.isUnavailable(userID, start, end)
	if err != nil || unavailable {
		return true
	}
	query := s.DB.Model(&SupportCallRequest{}).Where("assigned_user_id = ? AND scheduled_start < ? AND scheduled_end > ? AND status IN ?", userID, end, start, []string{CallStatusScheduled, CallStatusAwaitingApproval})
	if exceptCallID > 0 {
		query = query.Where("id <> ?", exceptCallID)
	}
	var count int64
	return query.Count(&count).Error != nil || count > 0
}

func (s *Service) sendCallNotification(call *SupportCallRequest) {
	if s.Mailer == nil || call == nil || call.AssignedUser == nil || strings.TrimSpace(call.AssignedUser.Email) == "" {
		return
	}
	body := fmt.Sprintf("Support call #%d\nStatus: %s\nWhen: %s to %s\nRequester: %s %s\nSubject: %s\n\n%s", call.ID, call.Status, call.ScheduledStart.Format(time.RFC1123), call.ScheduledEnd.Format(time.RFC1123), call.CreatedBy.FirstName, call.CreatedBy.LastName, call.Subject, call.Message)
	_ = s.Mailer.Send([]string{call.AssignedUser.Email}, fmt.Sprintf("Support call #%d: %s", call.ID, call.Subject), body)
}
