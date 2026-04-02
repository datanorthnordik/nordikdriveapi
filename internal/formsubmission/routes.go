package formsubmission

import (
	"nordik-drive-api/internal/middlewares"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(r *gin.Engine, formSubmissionService FormSubmissionServiceInterface) {
	formSubmissionController := &FormSubmissionController{
		FormSubmissionService: formSubmissionService,
	}

	formSubmissionGroup := r.Group("/api/form")
	formSubmissionGroup.Use(middlewares.AuthMiddleware())
	{
		formSubmissionGroup.GET("", formSubmissionController.GetFormsByFileID)
		formSubmissionGroup.GET("/answers", formSubmissionController.GetFormSubmission)
		formSubmissionGroup.GET("/answers/active", formSubmissionController.GetActiveFormSubmission)
		formSubmissionGroup.POST("/answers", formSubmissionController.SaveFormSubmission)
		formSubmissionGroup.POST("/answers/review", formSubmissionController.ReviewFormSubmission)
		formSubmissionGroup.GET("/answers/upload/:id", formSubmissionController.GetUpload)
		formSubmissionGroup.POST("/search", formSubmissionController.SearchFormSubmissions)
		formSubmissionGroup.POST("/my-requests", formSubmissionController.SearchMyFormSubmissions)
	}
}
