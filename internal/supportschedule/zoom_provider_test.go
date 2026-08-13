package supportschedule

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestZoomMeetingProviderLifecycle(t *testing.T) {
	var mu sync.Mutex
	tokenCalls := 0
	methods := make([]string, 0, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			clientID, secret, ok := r.BasicAuth()
			if !ok || clientID != "client" || secret != "secret" {
				t.Fatalf("unexpected Zoom token credentials")
			}
			if err := r.ParseForm(); err != nil || r.Form.Get("grant_type") != "account_credentials" || r.Form.Get("account_id") != "account" {
				t.Fatalf("unexpected token request: %#v, err=%v", r.Form, err)
			}
			mu.Lock()
			tokenCalls++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token":"token","expires_in":3600}`))
			return
		}
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Fatalf("missing bearer token")
		}
		mu.Lock()
		methods = append(methods, r.Method+" "+r.URL.Path)
		mu.Unlock()
		switch r.Method {
		case http.MethodPost:
			var payload zoomMeetingRequest
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			if payload.Duration != 40 || len(payload.Password) != 10 || !payload.Settings.WaitingRoom || payload.Settings.JoinBeforeHost || payload.Settings.MeetingAuthentication || payload.Settings.AutoRecording != "none" || payload.Settings.UsePMI {
				t.Fatalf("unsafe or incorrect meeting payload: %#v", payload)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":987654321,"host_email":"host@example.test","join_url":"https://zoom.test/j/987654321","password":"ZoomPass42","start_url":"https://zoom.test/s/987654321"}`))
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":987654321,"start_url":"https://zoom.test/s/987654321"}`))
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()

	provider, err := NewZoomMeetingProvider(ZoomConfig{AccountID: "account", ClientID: "client", ClientSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	provider.tokenURL, provider.apiBaseURL, provider.httpClient = server.URL+"/oauth/token", server.URL+"/v2", server.Client()
	start := time.Date(2026, time.August, 14, 14, 0, 0, 0, time.UTC)
	input := MeetingInput{RequestID: 7, HostEmail: "host@example.test", Topic: "Support", Agenda: "Private details remain local", StartTime: start, EndTime: start.Add(40 * time.Minute), TimeZone: DefaultTimeZone}

	details, err := provider.CreateMeeting(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if details.ExternalID != "987654321" || details.HostEmail != input.HostEmail || details.Passcode != "ZoomPass42" || !strings.Contains(details.JoinURL, "/j/") {
		t.Fatalf("unexpected Zoom meeting details: %#v", details)
	}
	if err := provider.UpdateMeeting(context.Background(), details.ExternalID, input); err != nil {
		t.Fatal(err)
	}
	startURL, err := provider.StartURL(context.Background(), details.ExternalID)
	if err != nil || !strings.Contains(startURL, "/s/") {
		t.Fatalf("start URL=%q err=%v", startURL, err)
	}
	if err := provider.DeleteMeeting(context.Background(), details.ExternalID); err != nil {
		t.Fatal(err)
	}
	if tokenCalls != 1 {
		t.Fatalf("token endpoint calls=%d want 1", tokenCalls)
	}
	wantMethods := []string{
		"POST /v2/users/host@example.test/meetings",
		"PATCH /v2/meetings/987654321",
		"GET /v2/meetings/987654321",
		"DELETE /v2/meetings/987654321",
	}
	if strings.Join(methods, "|") != strings.Join(wantMethods, "|") {
		t.Fatalf("Zoom API requests=%#v want %#v", methods, wantMethods)
	}
}

func TestNewZoomMeetingProviderRequiresAllCredentials(t *testing.T) {
	if _, err := NewZoomMeetingProvider(ZoomConfig{AccountID: "account", ClientID: "client"}); err == nil {
		t.Fatal("expected missing Zoom client secret to fail")
	}
}

func TestZoomMeetingProviderPreservesMissingHostError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/oauth/token" {
			_, _ = w.Write([]byte(`{"access_token":"token","expires_in":3600}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":1001,"message":"User does not exist: missing@example.test."}`))
	}))
	defer server.Close()

	provider, err := NewZoomMeetingProvider(ZoomConfig{AccountID: "account", ClientID: "client", ClientSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	provider.tokenURL, provider.apiBaseURL, provider.httpClient = server.URL+"/oauth/token", server.URL+"/v2", server.Client()
	start := time.Date(2026, time.August, 14, 14, 0, 0, 0, time.UTC)
	_, err = provider.CreateMeeting(context.Background(), MeetingInput{
		HostEmail: "missing@example.test", Topic: "Support", StartTime: start,
		EndTime: start.Add(40 * time.Minute), TimeZone: DefaultTimeZone,
	})
	if err == nil || !strings.Contains(err.Error(), "User does not exist: missing@example.test") {
		t.Fatalf("expected useful missing-host error, got %v", err)
	}
	if errors.Is(err, ErrMeetingNotFound) {
		t.Fatalf("missing host must not be reported as a missing meeting: %v", err)
	}
}

type fakeMeetingProvider struct {
	createInputs []MeetingInput
	updatedIDs   []string
	deletedIDs   []string
	createErr    error
}

func (*fakeMeetingProvider) Enabled() bool { return true }
func (p *fakeMeetingProvider) CreateMeeting(_ context.Context, input MeetingInput) (MeetingDetails, error) {
	p.createInputs = append(p.createInputs, input)
	if p.createErr != nil {
		return MeetingDetails{}, p.createErr
	}
	id := string(rune('0' + len(p.createInputs)))
	return MeetingDetails{ExternalID: "meeting-" + id, HostEmail: input.HostEmail, JoinURL: "https://zoom.test/j/" + id, Passcode: "Passcode" + id}, nil
}
func (p *fakeMeetingProvider) UpdateMeeting(_ context.Context, externalID string, _ MeetingInput) error {
	p.updatedIDs = append(p.updatedIDs, externalID)
	return nil
}
func (p *fakeMeetingProvider) DeleteMeeting(_ context.Context, externalID string) error {
	p.deletedIDs = append(p.deletedIDs, externalID)
	return nil
}
func (*fakeMeetingProvider) StartURL(_ context.Context, externalID string) (string, error) {
	return "https://zoom.test/s/" + externalID, nil
}

func TestApprovedCallCreatesZoomMeetingAndReassignmentRecreatesIt(t *testing.T) {
	f := newScheduleFixture(t)
	provider := &fakeMeetingProvider{}
	f.service.MeetingProvider = provider
	mailer := &scheduleMailer{}
	f.service.Mailer = mailer
	created, err := f.service.CreateCall(f.survivor, CreateCallInput{
		ScheduledStart: f.at(1, 10, 0).Format(time.RFC3339), DurationMinutes: 30, Subject: "Help", Message: "Details",
	})
	if err != nil {
		t.Fatal(err)
	}
	approved := created
	if approved.Status != RequestStatusApproved {
		t.Fatalf("daily-assignee call status = %q, want approved", approved.Status)
	}
	if approved.Call == nil || approved.Call.ZoomSyncStatus != ZoomSyncSynced || approved.Call.ZoomMeetingID == "" || approved.Call.ZoomJoinURL == "" {
		t.Fatalf("approved call has incomplete Zoom details: %#v", approved.Call)
	}
	if len(provider.createInputs) != 1 || provider.createInputs[0].HostEmail != approved.AssignedStaff.Email {
		t.Fatalf("Zoom meeting was not created under assigned support person's email: %#v", provider.createInputs)
	}
	if len(mailer.sent) != 2 {
		t.Fatalf("scheduled-call emails = %d, want requester and assigned support person", len(mailer.sent))
	}
	for _, email := range mailer.sent {
		if !strings.Contains(email.Body, approved.Call.ZoomJoinURL) {
			t.Fatalf("scheduled-call email to %v is missing the Zoom participant link", email.To)
		}
	}

	replacement := f.staffA
	if replacement == *approved.AssignedStaffID {
		replacement = f.staffB
	}
	previousEmail := approved.AssignedStaff.Email
	mailer.sent = nil
	reassigned, err := f.service.ReassignCall(f.manager, approved.Call.ID, ReassignInput{UserID: replacement, Reason: "Coverage change"})
	if err != nil {
		t.Fatal(err)
	}
	if len(provider.deletedIDs) != 1 || len(provider.createInputs) != 2 || reassigned.ZoomHostEmail != provider.createInputs[1].HostEmail || reassigned.ZoomMeetingID == approved.Call.ZoomMeetingID {
		t.Fatalf("reassignment did not recreate Zoom meeting: deleted=%#v created=%#v call=%#v", provider.deletedIDs, provider.createInputs, reassigned)
	}
	if len(mailer.sent) != 3 {
		t.Fatalf("reassignment emails = %d, want requester, replacement, and former support person", len(mailer.sent))
	}
	for _, email := range mailer.sent {
		if len(email.To) != 1 {
			t.Fatalf("unexpected reassignment recipients: %#v", email.To)
		}
		if email.To[0] == previousEmail {
			if strings.Contains(email.Body, reassigned.ZoomJoinURL) || !strings.Contains(email.Body, "no longer assigned") {
				t.Fatalf("former support person received incorrect reassignment email: %s", email.Body)
			}
			continue
		}
		if !strings.Contains(email.Body, reassigned.ZoomJoinURL) {
			t.Fatalf("updated Zoom link missing from reassignment email to %s", email.To[0])
		}
	}
	formerRequests, err := f.service.ListRequests(*approved.AssignedStaffID, "staff")
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range formerRequests {
		if request.ID == created.ID {
			t.Fatal("reassigned call still appears in the former support person's assigned-request table")
		}
	}
	replacementRequests, err := f.service.ListRequests(replacement, "staff")
	if err != nil || len(replacementRequests) != 1 || replacementRequests[0].ID != created.ID {
		t.Fatalf("replacement assigned-request table = %#v, err=%v", replacementRequests, err)
	}
	startResult, err := f.service.StartZoomMeeting(replacement, reassigned.ID)
	if err != nil || !strings.Contains(startResult.StartURL, "/s/") {
		t.Fatalf("assigned host start URL=%#v err=%v", startResult, err)
	}
	if _, err := f.service.StartZoomMeeting(f.survivor, reassigned.ID); err == nil {
		t.Fatal("requester must not receive the host start URL")
	}

	cancelled, err := f.service.CancelRequest(f.survivor, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Call == nil || cancelled.Call.ZoomMeetingID != "" || cancelled.Call.ZoomSyncStatus != ZoomSyncDeleted || len(provider.deletedIDs) != 2 {
		t.Fatalf("cancellation did not delete Zoom meeting: request=%#v deleted=%#v", cancelled, provider.deletedIDs)
	}
}

func TestZoomFailureDoesNotRollbackApprovedCall(t *testing.T) {
	f := newScheduleFixture(t)
	provider := &fakeMeetingProvider{createErr: context.DeadlineExceeded}
	f.service.MeetingProvider = provider
	mailer := &scheduleMailer{}
	f.service.Mailer = mailer
	created, err := f.service.CreateCall(f.survivor, CreateCallInput{
		ScheduledStart: f.at(1, 11, 0).Format(time.RFC3339), DurationMinutes: 30, Subject: "Help", Message: "Details",
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Status != RequestStatusApproved || created.Call == nil || created.Call.ZoomSyncStatus != ZoomSyncFailed {
		t.Fatalf("Zoom outage must not roll back the scheduled call: %#v", created)
	}
	if len(mailer.sent) != 0 {
		t.Fatalf("confirmed-call email must wait for its Zoom link; sent=%#v", mailer.sent)
	}

	provider.createErr = nil
	if err := f.service.SyncPendingZoomMeetings(); err != nil {
		t.Fatal(err)
	}
	ready, err := f.service.request(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if ready.Call == nil || ready.Call.ZoomSyncStatus != ZoomSyncSynced || ready.Call.ZoomJoinURL == "" {
		t.Fatalf("Zoom retry did not persist a participant link: %#v", ready.Call)
	}
	if len(mailer.sent) != 2 {
		t.Fatalf("Zoom-ready emails = %d, want requester and assigned host", len(mailer.sent))
	}
	for _, email := range mailer.sent {
		if !strings.Contains(email.Body, ready.Call.ZoomJoinURL) || strings.Contains(email.Body, "still being prepared") {
			t.Fatalf("Zoom-ready email to %v does not contain the final participant link", email.To)
		}
	}
}
