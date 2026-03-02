package lookup

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

type mockLookupService struct {
	provinces                    []Province
	daySchools                   []DaySchool
	indianHospitals              []IndianHospital
	provincesErr                 error
	daySchoolsErr                error
	indianHospitalsErr           error
	receivedProvID               int
	receivedIndianHospitalProvID int
}

func (m *mockLookupService) GetAllProvinces() ([]Province, error) {
	return m.provinces, m.provincesErr
}

func (m *mockLookupService) GetDaySchoolsByProvince(provinceID int) ([]DaySchool, error) {
	m.receivedProvID = provinceID
	return m.daySchools, m.daySchoolsErr
}

func (m *mockLookupService) GetIndianHospitalsByProvince(provinceID int) ([]IndianHospital, error) {
	m.receivedIndianHospitalProvID = provinceID
	return m.indianHospitals, m.indianHospitalsErr
}

func setupLookupRouter(svc LookupServiceAPI) *gin.Engine {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	controller := &LookupController{Service: svc}

	group := r.Group("/lookup")
	{
		group.GET("/province", controller.GetAllProvinces)
		group.GET("/dayschool/:province", controller.GetDaySchoolsByProvince)
		group.GET("/indian-hospital/:province", controller.GetIndianHospitalsByProvince)
	}

	return r
}

func TestLookupController_GetAllProvinces_Success(t *testing.T) {
	mockSvc := &mockLookupService{
		provinces: []Province{
			{ID: 1, Name: "Ontario"},
			{ID: 2, Name: "Alberta"},
		},
	}

	r := setupLookupRouter(mockSvc)

	req := httptest.NewRequest(http.MethodGet, "/lookup/province", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	var resp []Province
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if len(resp) != 2 {
		t.Fatalf("expected 2 provinces, got %d", len(resp))
	}

	if resp[0].Name != "Ontario" {
		t.Fatalf("expected first province Ontario, got %q", resp[0].Name)
	}
	if resp[1].Name != "Alberta" {
		t.Fatalf("expected second province Alberta, got %q", resp[1].Name)
	}
}

func TestLookupController_GetAllProvinces_ServiceError(t *testing.T) {
	mockSvc := &mockLookupService{
		provincesErr: errors.New("db error"),
	}

	r := setupLookupRouter(mockSvc)

	req := httptest.NewRequest(http.MethodGet, "/lookup/province", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", w.Code)
	}

	var resp map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp["error"] != "db error" {
		t.Fatalf("expected error 'db error', got %q", resp["error"])
	}
}

func TestLookupController_GetDaySchoolsByProvince_Success(t *testing.T) {
	mockSvc := &mockLookupService{
		daySchools: []DaySchool{
			{ID: 10, ProvinceID: 1, Name: "School A"},
			{ID: 11, ProvinceID: 1, Name: "School B"},
		},
	}

	r := setupLookupRouter(mockSvc)

	req := httptest.NewRequest(http.MethodGet, "/lookup/dayschool/1", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	var resp []DaySchool
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if len(resp) != 2 {
		t.Fatalf("expected 2 day schools, got %d", len(resp))
	}

	if resp[0].Name != "School A" {
		t.Fatalf("expected first school School A, got %q", resp[0].Name)
	}
	if resp[1].Name != "School B" {
		t.Fatalf("expected second school School B, got %q", resp[1].Name)
	}

	if mockSvc.receivedProvID != 1 {
		t.Fatalf("expected province id 1, got %d", mockSvc.receivedProvID)
	}
}

func TestLookupController_GetDaySchoolsByProvince_InvalidProvince(t *testing.T) {
	mockSvc := &mockLookupService{}
	r := setupLookupRouter(mockSvc)

	tests := []struct {
		name string
		url  string
	}{
		{name: "non numeric", url: "/lookup/dayschool/abc"},
		{name: "zero", url: "/lookup/dayschool/0"},
		{name: "negative", url: "/lookup/dayschool/-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.url, nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("expected status 400, got %d", w.Code)
			}

			var resp map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Fatalf("failed to unmarshal response: %v", err)
			}

			if resp["error"] != "valid province id is required" {
				t.Fatalf("unexpected error: %q", resp["error"])
			}
		})
	}
}

func TestLookupController_GetDaySchoolsByProvince_ServiceError(t *testing.T) {
	mockSvc := &mockLookupService{
		daySchoolsErr: errors.New("query failed"),
	}

	r := setupLookupRouter(mockSvc)

	req := httptest.NewRequest(http.MethodGet, "/lookup/dayschool/2", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", w.Code)
	}

	var resp map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp["error"] != "query failed" {
		t.Fatalf("expected error 'query failed', got %q", resp["error"])
	}

	if mockSvc.receivedProvID != 2 {
		t.Fatalf("expected province id 2, got %d", mockSvc.receivedProvID)
	}
}

func TestLookupController_GetIndianHospitalsByProvince_Success(t *testing.T) {
	mockSvc := &mockLookupService{
		indianHospitals: []IndianHospital{
			{ID: 20, ProvinceID: 1, Name: "Hospital A", EligibleDates: "January 1, 1936 - December 31, 1981"},
			{ID: 21, ProvinceID: 1, Name: "Hospital B", EligibleDates: "January 1, 1940 - June 30, 1960"},
		},
	}

	r := setupLookupRouter(mockSvc)

	req := httptest.NewRequest(http.MethodGet, "/lookup/indian-hospital/1", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	var resp []IndianHospital
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if len(resp) != 2 {
		t.Fatalf("expected 2 hospitals, got %d", len(resp))
	}

	if resp[0].Name != "Hospital A" {
		t.Fatalf("expected first hospital Hospital A, got %q", resp[0].Name)
	}
	if resp[1].Name != "Hospital B" {
		t.Fatalf("expected second hospital Hospital B, got %q", resp[1].Name)
	}

	if mockSvc.receivedIndianHospitalProvID != 1 {
		t.Fatalf("expected province id 1, got %d", mockSvc.receivedIndianHospitalProvID)
	}
}

func TestLookupController_GetIndianHospitalsByProvince_InvalidProvince(t *testing.T) {
	mockSvc := &mockLookupService{}
	r := setupLookupRouter(mockSvc)

	tests := []struct {
		name string
		url  string
	}{
		{name: "non numeric", url: "/lookup/indian-hospital/abc"},
		{name: "zero", url: "/lookup/indian-hospital/0"},
		{name: "negative", url: "/lookup/indian-hospital/-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.url, nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("expected status 400, got %d", w.Code)
			}

			var resp map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Fatalf("failed to unmarshal response: %v", err)
			}

			if resp["error"] != "valid province id is required" {
				t.Fatalf("unexpected error: %q", resp["error"])
			}
		})
	}
}

func TestLookupController_GetIndianHospitalsByProvince_ServiceError(t *testing.T) {
	mockSvc := &mockLookupService{
		indianHospitalsErr: errors.New("hospital query failed"),
	}

	r := setupLookupRouter(mockSvc)

	req := httptest.NewRequest(http.MethodGet, "/lookup/indian-hospital/2", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", w.Code)
	}

	var resp map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp["error"] != "hospital query failed" {
		t.Fatalf("expected error 'hospital query failed', got %q", resp["error"])
	}

	if mockSvc.receivedIndianHospitalProvID != 2 {
		t.Fatalf("expected province id 2, got %d", mockSvc.receivedIndianHospitalProvID)
	}
}
