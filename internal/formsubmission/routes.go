package formsubmission

import (
	"nordik-drive-api/internal/middlewares"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(r *gin.Engine, formSubmissionService *FormSubmissionService) {
	formSubmissionController := &FormSubmissionController{
		FormSubmissionService: formSubmissionService,
	}

	formSubmissionGroup := r.Group("/api/form/answers")
	formSubmissionGroup.Use(middlewares.AuthMiddleware())
	{
		formSubmissionGroup.GET("", formSubmissionController.GetFormSubmission)
		formSubmissionGroup.POST("", formSubmissionController.SaveFormSubmission)
	}
}
