package lookup

import (
	"github.com/gin-gonic/gin"
)

func RegisterRoutes(r *gin.Engine, lookupService LookupServiceAPI) {
	lookupController := &LookupController{Service: lookupService}

	userGroup := r.Group("/lookup")
	{
		userGroup.GET("/province", lookupController.GetAllProvinces)
		userGroup.GET("/dayschool/:province", lookupController.GetDaySchoolsByProvince)
	}
}
