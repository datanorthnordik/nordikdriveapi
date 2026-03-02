package lookup

import (
	"gorm.io/gorm"
)

type LookupServiceAPI interface {
	GetAllProvinces() ([]Province, error)
	GetDaySchoolsByProvince(provinceID int) ([]DaySchool, error)
	GetIndianHospitalsByProvince(provinceID int) ([]IndianHospital, error)
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

func (ls *LookupService) GetIndianHospitalsByProvince(provinceID int) ([]IndianHospital, error) {
	var hospitals []IndianHospital

	result := ls.DB.
		Where("province_id = ?", provinceID).
		Order("hospital_name ASC").
		Find(&hospitals)

	if result.Error != nil {
		return nil, result.Error
	}

	return hospitals, nil
}
