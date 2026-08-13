package supportschedule

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	defaultZoomAPIBaseURL = "https://api.zoom.us/v2"
	defaultZoomTokenURL   = "https://zoom.us/oauth/token"
)

type ZoomConfig struct {
	AccountID    string
	ClientID     string
	ClientSecret string
}

type ZoomMeetingProvider struct {
	accountID    string
	clientID     string
	clientSecret string
	apiBaseURL   string
	tokenURL     string
	httpClient   *http.Client

	tokenMu        sync.Mutex
	accessToken    string
	tokenExpiresAt time.Time
}

func NewZoomMeetingProvider(config ZoomConfig) (*ZoomMeetingProvider, error) {
	config.AccountID = strings.TrimSpace(config.AccountID)
	config.ClientID = strings.TrimSpace(config.ClientID)
	config.ClientSecret = strings.TrimSpace(config.ClientSecret)
	if config.AccountID == "" || config.ClientID == "" || config.ClientSecret == "" {
		return nil, errors.New("Zoom is enabled but ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID, or ZOOM_CLIENT_SECRET is missing")
	}
	return &ZoomMeetingProvider{
		accountID: config.AccountID, clientID: config.ClientID, clientSecret: config.ClientSecret,
		apiBaseURL: defaultZoomAPIBaseURL, tokenURL: defaultZoomTokenURL,
		httpClient: &http.Client{Timeout: 20 * time.Second},
	}, nil
}

func (*ZoomMeetingProvider) Enabled() bool { return true }

type zoomMeetingSettings struct {
	ApprovalType          int    `json:"approval_type"`
	Audio                 string `json:"audio"`
	AutoRecording         string `json:"auto_recording"`
	JoinBeforeHost        bool   `json:"join_before_host"`
	MeetingAuthentication bool   `json:"meeting_authentication"`
	MuteUponEntry         bool   `json:"mute_upon_entry"`
	UsePMI                bool   `json:"use_pmi"`
	WaitingRoom           bool   `json:"waiting_room"`
}

type zoomMeetingRequest struct {
	Agenda          string              `json:"agenda"`
	DefaultPassword bool                `json:"default_password"`
	Duration        int                 `json:"duration"`
	Password        string              `json:"password,omitempty"`
	Settings        zoomMeetingSettings `json:"settings"`
	StartTime       string              `json:"start_time"`
	Timezone        string              `json:"timezone"`
	Topic           string              `json:"topic"`
	Type            int                 `json:"type"`
}

type zoomMeetingResponse struct {
	ID        json.Number `json:"id"`
	HostEmail string      `json:"host_email"`
	JoinURL   string      `json:"join_url"`
	Password  string      `json:"password"`
	StartURL  string      `json:"start_url"`
}

func zoomRequest(input MeetingInput, password string) zoomMeetingRequest {
	duration := int(input.EndTime.Sub(input.StartTime).Minutes())
	if duration < 1 {
		duration = 1
	}
	return zoomMeetingRequest{
		Agenda: strings.TrimSpace(input.Agenda), DefaultPassword: true, Duration: duration, Password: password,
		StartTime: input.StartTime.UTC().Format(time.RFC3339), Timezone: strings.TrimSpace(input.TimeZone),
		Topic: strings.TrimSpace(input.Topic), Type: 2,
		Settings: zoomMeetingSettings{
			ApprovalType: 2, Audio: "both", AutoRecording: "none", JoinBeforeHost: false,
			MeetingAuthentication: false, MuteUponEntry: true, UsePMI: false, WaitingRoom: true,
		},
	}
}

func (p *ZoomMeetingProvider) CreateMeeting(ctx context.Context, input MeetingInput) (MeetingDetails, error) {
	passcode, err := newZoomPasscode()
	if err != nil {
		return MeetingDetails{}, err
	}
	var response zoomMeetingResponse
	path := "/users/" + url.PathEscape(strings.TrimSpace(input.HostEmail)) + "/meetings"
	if err := p.doJSON(ctx, http.MethodPost, path, zoomRequest(input, passcode), &response); err != nil {
		return MeetingDetails{}, err
	}
	externalID := strings.TrimSpace(response.ID.String())
	if externalID == "" || strings.TrimSpace(response.JoinURL) == "" {
		return MeetingDetails{}, errors.New("Zoom created a meeting without an ID or participant link")
	}
	hostEmail := strings.TrimSpace(response.HostEmail)
	if hostEmail == "" {
		hostEmail = strings.TrimSpace(input.HostEmail)
	}
	if strings.TrimSpace(response.Password) != "" {
		passcode = strings.TrimSpace(response.Password)
	}
	return MeetingDetails{ExternalID: externalID, HostEmail: hostEmail, JoinURL: strings.TrimSpace(response.JoinURL), Passcode: passcode}, nil
}

func (p *ZoomMeetingProvider) UpdateMeeting(ctx context.Context, externalID string, input MeetingInput) error {
	return p.doJSON(ctx, http.MethodPatch, "/meetings/"+url.PathEscape(strings.TrimSpace(externalID)), zoomRequest(input, ""), nil)
}

func newZoomPasscode() (string, error) {
	const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789!@#*"
	passcode := make([]byte, 10)
	random := make([]byte, len(passcode))
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate Zoom meeting passcode: %w", err)
	}
	for index := range passcode {
		passcode[index] = alphabet[int(random[index])%len(alphabet)]
	}
	// Satisfy the common locked account requirements without depending on
	// account-specific settings: upper case, lower case, number, special.
	passcode[0] = "ABCDEFGHJKLMNPQRSTUVWXYZ"[int(random[0])%24]
	passcode[1] = "abcdefghijkmnopqrstuvwxyz"[int(random[1])%24]
	passcode[2] = "23456789"[int(random[2])%8]
	passcode[3] = "!@#*"[int(random[3])%4]
	return string(passcode), nil
}

func (p *ZoomMeetingProvider) DeleteMeeting(ctx context.Context, externalID string) error {
	err := p.doJSON(ctx, http.MethodDelete, "/meetings/"+url.PathEscape(strings.TrimSpace(externalID)), nil, nil)
	if errors.Is(err, ErrMeetingNotFound) {
		return nil
	}
	return err
}

func (p *ZoomMeetingProvider) StartURL(ctx context.Context, externalID string) (string, error) {
	var response zoomMeetingResponse
	if err := p.doJSON(ctx, http.MethodGet, "/meetings/"+url.PathEscape(strings.TrimSpace(externalID)), nil, &response); err != nil {
		return "", err
	}
	if strings.TrimSpace(response.StartURL) == "" {
		return "", errors.New("Zoom did not return a host start link")
	}
	return strings.TrimSpace(response.StartURL), nil
}

func (p *ZoomMeetingProvider) accessTokenFor(ctx context.Context, force bool) (string, error) {
	p.tokenMu.Lock()
	defer p.tokenMu.Unlock()
	if !force && p.accessToken != "" && time.Now().Add(30*time.Second).Before(p.tokenExpiresAt) {
		return p.accessToken, nil
	}
	values := url.Values{"grant_type": {"account_credentials"}, "account_id": {p.accountID}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, p.tokenURL, strings.NewReader(values.Encode()))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.SetBasicAuth(p.clientID, p.clientSecret)
	response, err := p.httpClient.Do(request)
	if err != nil {
		return "", fmt.Errorf("request Zoom access token: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", zoomHTTPError(response)
	}
	var token struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(response.Body).Decode(&token); err != nil {
		return "", fmt.Errorf("decode Zoom access token: %w", err)
	}
	if strings.TrimSpace(token.AccessToken) == "" {
		return "", errors.New("Zoom returned an empty access token")
	}
	if token.ExpiresIn < 60 {
		token.ExpiresIn = 3600
	}
	p.accessToken, p.tokenExpiresAt = token.AccessToken, time.Now().Add(time.Duration(token.ExpiresIn)*time.Second)
	return p.accessToken, nil
}

func (p *ZoomMeetingProvider) doJSON(ctx context.Context, method, path string, input, output interface{}) error {
	var payload []byte
	var err error
	if input != nil {
		payload, err = json.Marshal(input)
		if err != nil {
			return err
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		token, err := p.accessTokenFor(ctx, attempt > 0)
		if err != nil {
			return err
		}
		request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(p.apiBaseURL, "/")+path, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		request.Header.Set("Authorization", "Bearer "+token)
		if input != nil {
			request.Header.Set("Content-Type", "application/json")
		}
		response, err := p.httpClient.Do(request)
		if err != nil {
			return fmt.Errorf("call Zoom API: %w", err)
		}
		if response.StatusCode == http.StatusUnauthorized && attempt == 0 {
			response.Body.Close()
			continue
		}
		if response.StatusCode == http.StatusNotFound {
			// Zoom also returns 404 when the user in
			// POST /users/{email}/meetings is not an active account user.
			// Preserve that response so operators see which host is invalid;
			// ErrMeetingNotFound is only meaningful for an existing meeting ID.
			if strings.HasPrefix(path, "/meetings/") {
				response.Body.Close()
				return ErrMeetingNotFound
			}
			apiErr := zoomHTTPError(response)
			response.Body.Close()
			return apiErr
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			apiErr := zoomHTTPError(response)
			response.Body.Close()
			return apiErr
		}
		if output != nil && response.StatusCode != http.StatusNoContent {
			decoder := json.NewDecoder(response.Body)
			decoder.UseNumber()
			if err := decoder.Decode(output); err != nil {
				response.Body.Close()
				return fmt.Errorf("decode Zoom API response: %w", err)
			}
		}
		response.Body.Close()
		return nil
	}
	return errors.New("Zoom authentication failed")
}

func zoomHTTPError(response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	var problem struct {
		Code    json.RawMessage `json:"code"`
		Message string          `json:"message"`
		Errors  []struct {
			Field   string `json:"field"`
			Message string `json:"message"`
		} `json:"errors"`
	}
	if json.Unmarshal(body, &problem) == nil {
		message := strings.TrimSpace(problem.Message)
		for _, detail := range problem.Errors {
			field, detailMessage := strings.TrimSpace(detail.Field), strings.TrimSpace(detail.Message)
			if detailMessage == "" {
				continue
			}
			if field != "" {
				detailMessage = field + ": " + detailMessage
			}
			if message != "" {
				message += "; "
			}
			message += detailMessage
		}
		if message != "" {
			code := strings.Trim(strings.TrimSpace(string(problem.Code)), `"`)
			if code != "" && code != "null" {
				return fmt.Errorf("Zoom API returned HTTP %d (code %s): %s", response.StatusCode, code, message)
			}
			return fmt.Errorf("Zoom API returned HTTP %d: %s", response.StatusCode, message)
		}
	}
	if message := strings.Join(strings.Fields(string(body)), " "); message != "" {
		return fmt.Errorf("Zoom API returned HTTP %d: %s", response.StatusCode, message)
	}
	return fmt.Errorf("Zoom API returned HTTP %d", response.StatusCode)
}
