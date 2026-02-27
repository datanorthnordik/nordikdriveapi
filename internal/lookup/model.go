package lookup

import (
	"time"
)

type Province struct {
	ID        int       `gorm:"primaryKey;autoIncrement" json:"id"`
	Name      string    `gorm:"size:255;not null;column:name" json:"name"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (Province) TableName() string {
	return "provinces"
}

type DaySchool struct {
	ID                   int       `gorm:"primaryKey;autoIncrement" json:"id"`
	ProvinceID           int       `gorm:"not null;column:province_id" json:"province_id"`
	Name                 string    `gorm:"type:text;not null;column:school_name" json:"name"`
	NameVariants         *string   `gorm:"type:text;column:name_variants" json:"name_variants"`
	OpeningDate          *string   `gorm:"type:text;column:opening_date" json:"opening_date"`
	ClosingDate          *string   `gorm:"type:text;column:closing_date" json:"closing_date"`
	Location             *string   `gorm:"type:text;column:location" json:"location"`
	ReligiousAffiliation *string   `gorm:"type:text;column:religious_affiliation" json:"religious_affiliation"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

func (DaySchool) TableName() string {
	return "day_schools"
}
