package supportschedule

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestScheduleEndpointIsManagerOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)
	f := newScheduleFixture(t)
	if err := f.service.EnsureRollingSchedule(); err != nil {
		t.Fatal(err)
	}
	controller := &Controller{Service: f.service}
	writer := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(writer)
	context.Request = httptest.NewRequest(http.MethodGet, "/api/support-schedule/schedule", nil)
	context.Set("userID", f.survivor)

	controller.ListSchedule(context)

	if writer.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body=%s", writer.Code, http.StatusForbidden, writer.Body.String())
	}
}

func TestProfileAvailabilityEndpointRejectsRegularUser(t *testing.T) {
	gin.SetMode(gin.TestMode)
	f := newScheduleFixture(t)
	controller := &Controller{Service: f.service}
	writer := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(writer)
	context.Request = httptest.NewRequest(http.MethodPost, "/api/support-schedule/profile/availability", bytes.NewBufferString(`{"date":"2026-08-04","full_day_unavailable":true,"reason":"not staff"}`))
	context.Request.Header.Set("Content-Type", "application/json")
	context.Set("userID", f.survivor)

	controller.CreateUnavailability(context)

	if writer.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body=%s", writer.Code, http.StatusForbidden, writer.Body.String())
	}
}
