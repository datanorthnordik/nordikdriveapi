package supportschedule

import (
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
		{ID: 2, FirstName: "Alex", LastName: "Support", Email: "alex@example.test", Role: "User"},
		{ID: 3, FirstName: "Blair", LastName: "Support", Email: "blair@example.test", Role: "User"},
		{ID: 4, FirstName: "Sam", LastName: "Survivor", Email: "sam@example.test", Role: "User"},
	}
	if err := db.Create(&users).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 3, 12, 0, 0, 0, location) // Monday
	service := &Service{DB: db, Clock: func() time.Time { return now }}
	for _, id := range []uint{2, 3} {
		if _, err := service.SetTeamMember(1, id, true); err != nil {
			t.Fatalf("set team member %d: %v", id, err)
		}
	}
	return scheduleFixture{service: service, location: location, now: now, manager: 1, staffA: 2, staffB: 3, survivor: 4}
}

// Test databases are explicitly created with SQL too. This keeps tests from
// masking a missing column through implicit schema generation.
func createSupportScheduleTestSchema(db *gorm.DB) error {
	statements := []string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY, firstname TEXT NOT NULL, lastname TEXT NOT NULL, email TEXT NOT NULL, role TEXT NOT NULL)`,
		`CREATE TABLE support_schedule_settings (id INTEGER PRIMARY KEY, time_zone TEXT NOT NULL, workday_start TEXT NOT NULL, workday_end TEXT NOT NULL, allowed_durations_json JSON NOT NULL, default_duration_minutes INTEGER NOT NULL, booking_horizon_days INTEGER NOT NULL, updated_by_id INTEGER, created_at DATETIME, updated_at DATETIME)`,
		`CREATE TABLE support_team_members (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL UNIQUE, is_active BOOLEAN NOT NULL, added_by_id INTEGER, created_at DATETIME, updated_at DATETIME)`,
		`CREATE TABLE support_daily_assignments (id INTEGER PRIMARY KEY AUTOINCREMENT, schedule_date TEXT NOT NULL UNIQUE, assigned_user_id INTEGER, status TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '', assigned_by_id INTEGER, created_at DATETIME, updated_at DATETIME)`,
		`CREATE TABLE support_staff_unavailabilities (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, all_team BOOLEAN NOT NULL, starts_at DATETIME NOT NULL, ends_at DATETIME NOT NULL, reason TEXT NOT NULL, created_by_id INTEGER NOT NULL, created_at DATETIME, updated_at DATETIME)`,
		`CREATE TABLE support_call_requests (id INTEGER PRIMARY KEY AUTOINCREMENT, created_by_id INTEGER NOT NULL, requested_staff_id INTEGER, assigned_user_id INTEGER, schedule_date TEXT NOT NULL, scheduled_start DATETIME NOT NULL, scheduled_end DATETIME NOT NULL, duration_minutes INTEGER NOT NULL, status TEXT NOT NULL, subject TEXT NOT NULL, message TEXT NOT NULL DEFAULT '', meeting_provider TEXT NOT NULL DEFAULT 'zoom', meeting_status TEXT NOT NULL DEFAULT 'pending_provider', meeting_url TEXT NOT NULL DEFAULT '', external_meeting_id TEXT NOT NULL DEFAULT '', approval_note TEXT NOT NULL DEFAULT '', reassignment_reason TEXT NOT NULL DEFAULT '', actual_start DATETIME, actual_end DATETIME, actual_minutes INTEGER NOT NULL DEFAULT 0, completed_by_id INTEGER, completed_at DATETIME, created_at DATETIME, updated_at DATETIME)`,
	}
	for _, statement := range statements {
		if err := db.Exec(statement).Error; err != nil {
			return err
		}
	}
	return nil
}

func TestPostgresInitSQLDefinesSupportScheduleSchema(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "db-init", "init.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sql := string(data)
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS support_schedule_settings",
		"CREATE TABLE IF NOT EXISTS support_team_members",
		"CREATE TABLE IF NOT EXISTS support_daily_assignments",
		"CREATE TABLE IF NOT EXISTS support_staff_unavailabilities",
		"CREATE TABLE IF NOT EXISTS support_call_requests",
		"('Manager',      1)",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("db-init/init.sql is missing %q", required)
		}
	}
}

func (f scheduleFixture) at(dayOffset, hour, minute int) time.Time {
	date := f.now.AddDate(0, 0, dayOffset)
	return time.Date(date.Year(), date.Month(), date.Day(), hour, minute, 0, 0, f.location)
}

func TestEnsureRollingScheduleCreatesTwoWeekBusinessRota(t *testing.T) {
	f := newScheduleFixture(t)
	if err := f.service.EnsureRollingSchedule(); err != nil {
		t.Fatal(err)
	}
	assignments, err := f.service.ListSchedule("", "")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(assignments), 10; got != want { // Aug 3 through Aug 16 has ten weekdays.
		t.Fatalf("assignments = %d, want %d", got, want)
	}
	for _, assignment := range assignments {
		if assignment.Status != AssignmentStatusScheduled || assignment.AssignedUserID == nil {
			t.Fatalf("assignment %#v was not covered", assignment)
		}
	}
}

func TestFullDayUnavailabilityImmediatelyReassignsRota(t *testing.T) {
	f := newScheduleFixture(t)
	if err := f.service.EnsureRollingSchedule(); err != nil {
		t.Fatal(err)
	}
	date := scheduleDateFor(f.now, f.location)
	if _, err := f.service.ReassignDay(f.manager, date, ReassignInput{UserID: f.staffA, Reason: "test deterministic rota"}); err != nil {
		t.Fatal(err)
	}
	_, err := f.service.CreateUnavailability(f.staffA, CreateUnavailabilityInput{
		StartsAt: f.at(0, 8, 30).Format(time.RFC3339),
		EndsAt:   f.at(0, 16, 30).Format(time.RFC3339),
		Reason:   "away all day",
	})
	if err != nil {
		t.Fatal(err)
	}
	assignments, err := f.service.ListSchedule(date, date)
	if err != nil {
		t.Fatal(err)
	}
	if len(assignments) != 1 || assignments[0].AssignedUserID == nil || *assignments[0].AssignedUserID != f.staffB {
		t.Fatalf("expected reassignment to staff B, got %#v", assignments)
	}
}

func TestSpecificStaffApprovalAndCompletionRecordsActualMinutes(t *testing.T) {
	f := newScheduleFixture(t)
	if err := f.service.EnsureRollingSchedule(); err != nil {
		t.Fatal(err)
	}
	call, err := f.service.CreateCall(f.survivor, CreateCallInput{
		ScheduledStart:   f.at(1, 10, 0).Format(time.RFC3339),
		DurationMinutes:  30,
		Subject:          "Help with my request",
		Message:          "I would like to talk through my question.",
		RequestedStaffID: &f.staffB,
	})
	if err != nil {
		t.Fatal(err)
	}
	if call.Status != CallStatusAwaitingApproval {
		t.Fatalf("status = %q", call.Status)
	}
	call, err = f.service.ApproveSpecificCall(f.staffB, call.ID, SpecificApprovalInput{Approved: true})
	if err != nil {
		t.Fatal(err)
	}
	if call.Status != CallStatusScheduled {
		t.Fatalf("approval status = %q", call.Status)
	}
	actualStart := f.at(1, 10, 5)
	call, err = f.service.CompleteCall(f.staffB, call.ID, CompleteCallInput{
		ActualStart: actualStart.Format(time.RFC3339),
		ActualEnd:   actualStart.Add(45 * time.Minute).Format(time.RFC3339),
	})
	if err != nil {
		t.Fatal(err)
	}
	if call.Status != CallStatusCompleted || call.ActualMinutes != 45 {
		t.Fatalf("completed call = status %q, minutes %d", call.Status, call.ActualMinutes)
	}
}

func TestFairAssignmentUsesCompletedSupportMinutes(t *testing.T) {
	f := newScheduleFixture(t)
	if err := f.service.EnsureRollingSchedule(); err != nil {
		t.Fatal(err)
	}
	assigned := f.staffA
	if err := f.service.DB.Create(&SupportCallRequest{
		CreatedByID: f.survivor, AssignedUserID: &assigned, ScheduleDate: scheduleDateFor(f.now, f.location),
		ScheduledStart: f.at(1, 9, 0), ScheduledEnd: f.at(1, 10, 0), DurationMinutes: 60,
		Status: CallStatusCompleted, Subject: "completed support", MeetingProvider: "zoom", MeetingStatus: MeetingStatusPendingProvider,
		ActualMinutes: 120,
	}).Error; err != nil {
		t.Fatal(err)
	}
	chosen, err := f.service.pickFairStaff(scheduleDateFor(f.now.AddDate(0, 0, 13), f.location), nil)
	if err != nil {
		t.Fatal(err)
	}
	if chosen != f.staffB {
		t.Fatalf("fair assignment chose %d, want %d", chosen, f.staffB)
	}
}
