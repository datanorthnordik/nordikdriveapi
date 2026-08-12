package supportschedule

import (
	"context"
	"errors"
	"time"
)

type MeetingInput struct {
	RequestID uint
	HostEmail string
	Topic     string
	Agenda    string
	StartTime time.Time
	EndTime   time.Time
	TimeZone  string
}

type MeetingDetails struct {
	ExternalID string
	HostEmail  string
	JoinURL    string
	Passcode   string
}

// MeetingProvider keeps the scheduling workflow independent from the Zoom
// transport while supporting the complete meeting lifecycle.
type MeetingProvider interface {
	Enabled() bool
	CreateMeeting(ctx context.Context, input MeetingInput) (MeetingDetails, error)
	UpdateMeeting(ctx context.Context, externalID string, input MeetingInput) error
	DeleteMeeting(ctx context.Context, externalID string) error
	StartURL(ctx context.Context, externalID string) (string, error)
}

// HRAvailabilityProvider is the extension point for a future SageHR sync.
// The scheduling service remains authoritative for manager-entered overrides.
type HRAvailabilityProvider interface {
	SyncUnavailability(ctx context.Context, from, to time.Time) error
}

var ErrIntegrationNotConfigured = errors.New("integration is not configured")
var ErrMeetingNotFound = errors.New("meeting does not exist")

type DisabledMeetingProvider struct{}

func (DisabledMeetingProvider) Enabled() bool { return false }

func (DisabledMeetingProvider) CreateMeeting(context.Context, MeetingInput) (MeetingDetails, error) {
	return MeetingDetails{}, ErrIntegrationNotConfigured
}

func (DisabledMeetingProvider) UpdateMeeting(context.Context, string, MeetingInput) error {
	return ErrIntegrationNotConfigured
}

func (DisabledMeetingProvider) DeleteMeeting(context.Context, string) error {
	return ErrIntegrationNotConfigured
}

func (DisabledMeetingProvider) StartURL(context.Context, string) (string, error) {
	return "", ErrIntegrationNotConfigured
}

type DisabledHRAvailabilityProvider struct{}

func (DisabledHRAvailabilityProvider) SyncUnavailability(context.Context, time.Time, time.Time) error {
	return ErrIntegrationNotConfigured
}
