package community

import (
	"time"
)

type Community struct {
	ID        int       `gorm:"primaryKey;autoIncrement" json:"id"`
	Name      string    `gorm:"size:100;not null;column:community_name" json:"name"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Approved  bool      `gorm:"default:false" json:"approved"`
}

func (Community) TableName() string {
	return "communities"
}
