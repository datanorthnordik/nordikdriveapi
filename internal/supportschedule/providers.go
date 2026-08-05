package supportschedule

import (
	"context"
	"errors"
	"time"
)

// MeetingProvider is intentionally narrow so Zoom can be connected later
// without changing the booking, fairness, or scheduling code.
type MeetingProvider interface {
	CreateMeeting(ctx context.Context, request SupportCallRequest) (externalID, joinURL string, err error)
}

// HRAvailabilityProvider is the extension point for a future SageHR sync.
// The scheduling service remains authoritative for manager-entered overrides.
type HRAvailabilityProvider interface {
	SyncUnavailability(ctx context.Context, from, to time.Time) error
}

var ErrIntegrationNotConfigured = errors.New("integration is not configured")

type DisabledMeetingProvider struct{}

func (DisabledMeetingProvider) CreateMeeting(context.Context, SupportCallRequest) (string, string, error) {
	return "", "", ErrIntegrationNotConfigured
}

type DisabledHRAvailabilityProvider struct{}

func (DisabledHRAvailabilityProvider) SyncUnavailability(context.Context, time.Time, time.Time) error {
	return ErrIntegrationNotConfigured
}
