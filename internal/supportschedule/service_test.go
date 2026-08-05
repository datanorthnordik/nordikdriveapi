package supportschedule

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

type scheduleFixture struct {
	service  *Service
	location *time.Location
	now      time.Time
	manager  uint
	staffA   uint
	staffB   uint
	survivor uint
}

func newScheduleFixture(t *testing.T) scheduleFixture {
	t.Helper()
	location, err := time.LoadLocation(DefaultTimeZone)
	if err != nil {
		t.Fatal(err)
	}
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := createSupportScheduleTestSchema(db); err != nil {
		t.Fatal(err)
	}
	users := []SupportUser{
		{ID: 1, FirstName: "Maya", LastName: "Manager", Email: "manager@example.test", Role: RoleManager},
		{ID: 2, FirstName: "Alex", LastName: "Support", Email: "alex@example.test", Role: RoleAdmin},
		{ID: 3, FirstName: "Blair", LastName: "Support", Email: "blair@example.test", Role: RoleAdmin},
		{ID: 4, FirstName: "Sam", LastName: "Survivor", Email: "sam@example.test", Role: "User"},
	}
	if err := db.Create(&users).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 3, 8, 0, 0, 0, location)
	service := &Service{DB: db, Clock: func() time.Time { return now }}
	for _, id := range []uint{2, 3} {
		if _, err := service.SetTeamMember(1, id, true); err != nil {
			t.Fatalf("set team member %d: %v", id, err)
		}
	}
	return scheduleFixture{service: service, location: location, now: now, manager: 1, staffA: 2, staffB: 3, survivor: 4}
}

// The test schema is explicit to ensure the package never masks a missing
// production column through GORM AutoMigrate.
func createSupportScheduleTestSchema(db *gorm.DB) error {
	statements := []string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY, firstname TEXT NOT NULL, lastname TEXT NOT NULL, email TEXT NOT NULL, role TEXT NOT NULL)`,
		`CREATE TABLE support_schedule_settings (id INTEGER PRIMARY KEY, time_zone TEXT NOT NULL, workday_start TEXT NOT NULL, workday_end TEXT NOT NULL, allowed_durations_json JSON NOT NULL, default_duration_minutes INTEGER NOT NULL, booking_horizon_days INTEGER NOT NULL, updated_by_id INTEGER, created_at DATETIME, updated_at DATETIME)`,
		`CREATE TABLE support_team_members (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL UNIQUE, is_active BOOLEAN NOT NULL, added_by_id INTEGER, created_at DATETIME, updated_at DATETIME)`,
		`CREATE TABLE support_assignments (id INTEGER PRIMARY KEY AUTOINCREMENT, assignment_date TEXT NOT NULL UNIQUE, primary_assignee_id INTEGER, assignment_source TEXT NOT NULL, previous_assignee_id INTEGER, reassigned_by_id INTEGER, reassignment_reason TEXT NOT NULL DEFAULT '', created_at DATETIME, updated_at DATETIME)`,
		`CREATE TABLE support_staff_availabilities (id INTEGER PRIMARY KEY AUTOINCREMENT, staff_id INTEGER NOT NULL, availability_date TEXT NOT NULL, full_day_unavailable BOOLEAN NOT NULL, unavailable_start_time DATETIME, unavailable_end_time DATETIME, reason TEXT NOT NULL, created_at DATETIME, updated_at DATETIME)`,
		`CREATE TABLE support_call_requests (id INTEGER PRIMARY KEY AUTOINCREMENT, requested_by_user_id INTEGER NOT NULL, request_type TEXT NOT NULL, requested_date TEXT NOT NULL, preferred_start_time DATETIME, preferred_end_time DATETIME, requested_staff_id INTEGER, assigned_staff_id INTEGER, status TEXT NOT NULL, subject TEXT NOT NULL, description TEXT NOT NULL DEFAULT '', rejection_reason TEXT NOT NULL DEFAULT '', alternative_start_time DATETIME, alternative_end_time DATETIME, created_at DATETIME, updated_at DATETIME)`,
		`CREATE TABLE support_calls (id INTEGER PRIMARY KEY AUTOINCREMENT, support_request_id INTEGER NOT NULL UNIQUE, assigned_staff_id INTEGER, scheduled_start_time DATETIME NOT NULL, scheduled_end_time DATETIME NOT NULL, actual_start_time DATETIME, actual_end_time DATETIME, actual_duration_minutes INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, internal_notes TEXT NOT NULL DEFAULT '', completed_by_id INTEGER, completed_at DATETIME, created_at DATETIME, updated_at DATETIME)`,
		`CREATE TABLE support_schedule_audit_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, entity_type TEXT NOT NULL, entity_id INTEGER, action TEXT NOT NULL, actor_id INTEGER, details JSON NOT NULL, created_at DATETIME)`,
	}
	for _, statement := range statements {
		if err := db.Exec(statement).Error; err != nil {
			return err
		}
	}
	return nil
}

func TestPostgresInitSQLDefinesRedesignedSupportScheduleSchema(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "db-init", "init.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sql := string(data)
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS support_assignments",
		"CREATE TABLE IF NOT EXISTS support_staff_availabilities",
		"CREATE TABLE IF NOT EXISTS support_call_requests",
		"CREATE TABLE IF NOT EXISTS support_calls",
		"CREATE TABLE IF NOT EXISTS support_schedule_audit_logs",
		"support_calls_no_overlapping_slots",
		"('Manager',      0)",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("db-init/init.sql is missing %q", required)
		}
	}
	if strings.Contains(sql, "CREATE TABLE IF NOT EXISTS support_daily_assignments") || strings.Contains(sql, "CREATE TABLE IF NOT EXISTS support_staff_unavailabilities") {
		t.Fatal("legacy support scheduling tables must not be recreated by init.sql")
	}
	cleanup, err := os.ReadFile(filepath.Join("..", "..", "db-init", "remove-legacy-support-scheduling.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(cleanup), "DROP TABLE IF EXISTS support_call_requests") {
		t.Fatal("legacy cleanup script must remove the old support-call request table")
	}
}

func (f scheduleFixture) at(dayOffset, hour, minute int) time.Time {
	date := f.now.AddDate(0, 0, dayOffset)
	return time.Date(date.Year(), date.Month(), date.Day(), hour, minute, 0, 0, f.location)
}

func (f scheduleFixture) date(dayOffset int) string {
	return scheduleDateFor(f.now.AddDate(0, 0, dayOffset), f.location)
}

func TestInitialScheduleCreatesExactlyFourteenCalendarAssignmentsAndIsIdempotent(t *testing.T) {
	f := newScheduleFixture(t)
	if err := f.service.EnsureRollingSchedule(); err != nil {
		t.Fatal(err)
	}
	if err := f.service.EnsureRollingSchedule(); err != nil {
		t.Fatal(err)
	}
	assignments, err := f.service.ListSchedule("", "")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(assignments), 14; got != want {
		t.Fatalf("assignments = %d, want %d", got, want)
	}
	for index, assignment := range assignments {
		if assignment.AssignmentDate != f.date(index) || assignment.PrimaryAssigneeID == nil {
			t.Fatalf("unexpected assignment at %d: %#v", index, assignment)
		}
	}
	var count int64
	if err := f.service.DB.Model(&SupportAssignment{}).Count(&count).Error; err != nil || count != 14 {
		t.Fatalf("assignment count = %d, err = %v", count, err)
	}
}

func TestFullDayUnavailabilityReassignsUsingFairRulesAndAudits(t *testing.T) {
	f := newScheduleFixture(t)
	if err := f.service.EnsureRollingSchedule(); err != nil {
		t.Fatal(err)
	}
	date := f.date(1)
	if _, err := f.service.ReassignDay(f.manager, date, ReassignInput{UserID: f.staffA, Reason: "fixture"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.CreateAvailability(f.staffA, CreateAvailabilityInput{Date: date, FullDayUnavailable: true, Reason: "away"}); err != nil {
		t.Fatal(err)
	}
	assignments, err := f.service.ListSchedule(date, date)
	if err != nil {
		t.Fatal(err)
	}
	if len(assignments) != 1 || assignments[0].PrimaryAssigneeID == nil || *assignments[0].PrimaryAssigneeID != f.staffB || assignments[0].AssignmentSource != AssignmentSourceFullDay {
		t.Fatalf("expected reassignment to staff B, got %#v", assignments)
	}
	var audits int64
	if err := f.service.DB.Model(&SupportScheduleAuditLog{}).Where("action = ?", "full_day_assignment_reassignment").Count(&audits).Error; err != nil || audits != 1 {
		t.Fatalf("full-day reassignment audit count = %d, err = %v", audits, err)
	}
}

func TestPartialAvailabilityAndPendingRequestReserveSlots(t *testing.T) {
	f := newScheduleFixture(t)
	if err := f.service.EnsureRollingSchedule(); err != nil {
		t.Fatal(err)
	}
	date := f.date(1)
	if _, err := f.service.ReassignDay(f.manager, date, ReassignInput{UserID: f.staffA, Reason: "fixture"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.CreateAvailability(f.staffA, CreateAvailabilityInput{
		StartsAt: f.at(1, 10, 0).Format(time.RFC3339), EndsAt: f.at(1, 11, 0).Format(time.RFC3339), Reason: "appointment",
	}); err != nil {
		t.Fatal(err)
	}
	availability, err := f.service.ListAvailability(date, 30, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, slot := range availability.Slots {
		if slot.StartAt.Equal(f.at(1, 10, 0)) || slot.StartAt.Equal(f.at(1, 10, 30)) {
			t.Fatalf("partial-unavailability slot was returned: %#v", slot)
		}
	}
	input := CreateCallInput{ScheduledStart: f.at(1, 11, 0).Format(time.RFC3339), DurationMinutes: 30, Subject: "Need help"}
	request, err := f.service.CreateCall(f.survivor, input)
	if err != nil {
		t.Fatal(err)
	}
	if request.RequestType != RequestTypeAutomaticDaily || request.Status != RequestStatusPending || request.AssignedStaffID == nil || *request.AssignedStaffID != f.staffA {
		t.Fatalf("normal request was not correctly routed: %#v", request)
	}
	if _, err := f.service.CreateCall(f.survivor, input); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("overlapping pending request error = %v, want ErrUnavailable", err)
	}
}

func TestSpecificPersonApprovalCompletionAndFairnessUseActualDuration(t *testing.T) {
	f := newScheduleFixture(t)
	if err := f.service.EnsureRollingSchedule(); err != nil {
		t.Fatal(err)
	}
	request, err := f.service.CreateCall(f.survivor, CreateCallInput{
		ScheduledStart: f.at(1, 10, 0).Format(time.RFC3339), DurationMinutes: 30,
		Subject: "Help with my request", Message: "Please review", RequestedStaffID: &f.staffB,
	})
	if err != nil {
		t.Fatal(err)
	}
	if request.Status != RequestStatusAwaitingApproval || request.RequestType != RequestTypeSpecificStaff {
		t.Fatalf("specific-person request status = %q", request.Status)
	}
	request, err = f.service.DecideRequest(f.staffB, request.ID, RequestDecisionInput{Decision: "approve"})
	if err != nil {
		t.Fatal(err)
	}
	if request.Status != RequestStatusApproved || request.Call == nil {
		t.Fatalf("approved request = %#v", request)
	}
	actualStart := f.at(1, 10, 5)
	call, err := f.service.CompleteCall(f.staffB, request.Call.ID, CompleteCallInput{
		ActualStart: actualStart.Format(time.RFC3339), ActualEnd: actualStart.Add(45 * time.Minute).Format(time.RFC3339), InternalNotes: "resolved",
	})
	if err != nil {
		t.Fatal(err)
	}
	if call.Status != RequestStatusCompleted || call.ActualDurationMinutes != 45 {
		t.Fatalf("completed call = %#v", call)
	}
	picked, err := f.service.pickFairStaff(f.date(13), nil)
	if err != nil {
		t.Fatal(err)
	}
	if picked != f.staffA {
		t.Fatalf("fair assignment chose %d, want staff A after staff B worked 45 minutes", picked)
	}
	var audits int64
	if err := f.service.DB.Model(&SupportScheduleAuditLog{}).Where("action = ?", "actual_duration_recorded").Count(&audits).Error; err != nil || audits != 1 {
		t.Fatalf("duration audit count = %d, err = %v", audits, err)
	}
}

func TestRolePermissionsSeparateUserAdminAndManagerWorkflows(t *testing.T) {
	f := newScheduleFixture(t)
	if err := f.service.EnsureRollingSchedule(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.ReassignDay(f.survivor, f.date(1), ReassignInput{UserID: f.staffA, Reason: "nope"}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("regular user reassignment error = %v", err)
	}
	if _, err := f.service.ReassignDay(f.staffA, f.date(1), ReassignInput{UserID: f.staffB, Reason: "nope"}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("support admin reassignment error = %v", err)
	}
	profile, err := f.service.GetProfile(f.staffA)
	if err != nil {
		t.Fatal(err)
	}
	if profile == nil {
		t.Fatal("support-admin profile is nil")
	}
}
