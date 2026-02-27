package lookup

import (
	"gorm.io/gorm"
)

type LookupServiceAPI interface {
	GetAllProvinces() ([]Province, error)
	GetDaySchoolsByProvince(provinceID int) ([]DaySchool, error)
}

type LookupService struct {
	DB *gorm.DB
}

func NewLookupService(db *gorm.DB) *LookupService {
	return &LookupService{DB: db}
}

func (ls *LookupService) GetAllProvinces() ([]Province, error) {
	var provinces []Province
	result := ls.DB.Order("name ASC").Find(&provinces)
	if result.Error != nil {
		return nil, result.Error
	}
	return provinces, nil
}

func (ls *LookupService) GetDaySchoolsByProvince(provinceID int) ([]DaySchool, error) {
	var daySchools []DaySchool
	result := ls.DB.
		Where("province_id = ?", provinceID).
		Order("school_name ASC").
		Find(&daySchools)

	if result.Error != nil {
		return nil, result.Error
	}
	return daySchools, nil
}
