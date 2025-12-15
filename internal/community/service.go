package community

import (
	"gorm.io/gorm"
)

type CommunityService struct {
	DB *gorm.DB
}

func (fs *CommunityService) GetAllCommunities() ([]Community, error) {
	var files []Community
	result := fs.DB.Find(&files)
	if result.Error != nil {
		return nil, result.Error
	}
	return files, nil
}

func (fs *CommunityService) AddCommunities(communities []string) error {
	communitiesToAdd := []Community{}
	for _, communityName := range communities {
		community := Community{Name: communityName, Approved: false}
		communitiesToAdd = append(communitiesToAdd, community)
	}
	result := fs.DB.Create(&communitiesToAdd)
	return result.Error
}
