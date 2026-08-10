package dataconfig

import "gorm.io/datatypes"

// configForResponse returns the stored configuration unchanged. Consumers need
// the full field definition, including additional fields and source-file
// sections, to render every configured tab and document field.
func configForResponse(raw datatypes.JSON) datatypes.JSON {
	return raw
}
