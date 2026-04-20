package file

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	currentNormalizationVersion = 1
	defaultNormalizationBatch   = 250
)

var (
	normalizationWhitespaceRe = regexp.MustCompile(`\s+`)
	normalizationYearRe       = regexp.MustCompile(`\b\d{3,4}\b`)
)

type FileDataNormalized struct {
	ID                   uint           `gorm:"primaryKey" json:"id"`
	SourceRowID          uint           `gorm:"not null;uniqueIndex" json:"source_row_id"`
	FileID               uint           `gorm:"not null;index:idx_file_data_normalized_file_version,priority:1" json:"file_id"`
	Version              int            `gorm:"not null;index:idx_file_data_normalized_file_version,priority:2" json:"version"`
	RowDataNormalized    datatypes.JSON `gorm:"type:jsonb;not null" json:"row_data_normalized"`
	SearchText           string         `gorm:"type:text" json:"search_text"`
	CanonicalName        string         `gorm:"type:text;index" json:"canonical_name"`
	CanonicalCommunity   string         `gorm:"type:text;index" json:"canonical_community"`
	CanonicalSchool      string         `gorm:"type:text;index" json:"canonical_school"`
	Status               string         `gorm:"type:varchar(20);not null;default:'ready';index" json:"status"`
	ErrorMessage         string         `gorm:"type:text" json:"error_message"`
	SourceCreatedAt      time.Time      `gorm:"not null" json:"source_created_at"`
	SourceUpdatedAt      time.Time      `gorm:"not null;index" json:"source_updated_at"`
	NormalizationVersion int            `gorm:"not null;default:1" json:"normalization_version"`
	NormalizedAt         time.Time      `gorm:"not null" json:"normalized_at"`
	CreatedAt            time.Time      `gorm:"not null;autoCreateTime" json:"created_at"`
	UpdatedAt            time.Time      `gorm:"not null;autoUpdateTime" json:"updated_at"`
}

type NormalizationSyncResult struct {
	Processed int `json:"processed"`
	Inserted  int `json:"inserted"`
	Updated   int `json:"updated"`
	Failed    int `json:"failed"`
}

type NormalizationSyncOptions struct {
	FileID     *uint
	Version    *int
	BatchSize  int
	MaxBatches int
}

type normalizedRowPayload struct {
	Fields       map[string]normalizedField `json:"fields"`
	Names        []string                   `json:"names,omitempty"`
	Communities  []string                   `json:"communities,omitempty"`
	Schools      []string                   `json:"schools,omitempty"`
	Locations    []string                   `json:"locations,omitempty"`
	SearchTokens []string                   `json:"search_tokens,omitempty"`
}

type normalizedField struct {
	Raw        string          `json:"raw"`
	Normalized string          `json:"normalized"`
	Tokens     []string        `json:"tokens,omitempty"`
	Role       string          `json:"role"`
	DateHint   *normalizedDate `json:"date_hint,omitempty"`
}

type normalizedDate struct {
	Raw         string `json:"raw"`
	Kind        string `json:"kind"`
	LowerYear   *int   `json:"lower_year,omitempty"`
	UpperYear   *int   `json:"upper_year,omitempty"`
	Approximate bool   `json:"approximate,omitempty"`
}

type normalizationCandidate struct {
	ID                      uint           `gorm:"column:id"`
	FileID                  uint           `gorm:"column:file_id"`
	RowData                 datatypes.JSON `gorm:"column:row_data"`
	InsertedBy              uint           `gorm:"column:inserted_by"`
	CreatedAt               time.Time      `gorm:"column:created_at"`
	UpdatedAt               time.Time      `gorm:"column:updated_at"`
	Version                 int            `gorm:"column:version"`
	NormalizedID            *uint          `gorm:"column:normalized_id"`
	ExistingSourceUpdatedAt *time.Time     `gorm:"column:existing_source_updated_at"`
	ExistingVersion         *int           `gorm:"column:existing_normalization_version"`
}

func (FileDataNormalized) TableName() string {
	return "file_data_normalized"
}

func ensureNormalizationSchema(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("db not initialized")
	}
	return db.AutoMigrate(&FileDataNormalized{})
}

func RunNormalizationSync(db *gorm.DB, options NormalizationSyncOptions) (*NormalizationSyncResult, error) {
	if err := ensureNormalizationSchema(db); err != nil {
		return nil, err
	}

	batchSize := options.BatchSize
	if batchSize <= 0 {
		batchSize = defaultNormalizationBatch
	}

	maxBatches := options.MaxBatches
	if maxBatches <= 0 {
		maxBatches = int(^uint(0) >> 1)
	}

	total := &NormalizationSyncResult{}
	for i := 0; i < maxBatches; i++ {
		batch, err := SyncPendingNormalizedRows(db, options.FileID, options.Version, batchSize)
		if err != nil {
			return nil, err
		}
		if batch == nil || batch.Processed == 0 {
			break
		}

		total.Processed += batch.Processed
		total.Inserted += batch.Inserted
		total.Updated += batch.Updated
		total.Failed += batch.Failed

		if batch.Processed < batchSize {
			break
		}
	}

	return total, nil
}

func SyncPendingNormalizedRows(db *gorm.DB, fileID *uint, version *int, limit int) (*NormalizationSyncResult, error) {
	if err := ensureNormalizationSchema(db); err != nil {
		return nil, err
	}

	if limit <= 0 {
		limit = defaultNormalizationBatch
	}

	query := db.Table("file_data fd").
		Select(`
			fd.id,
			fd.file_id,
			fd.row_data,
			fd.inserted_by,
			fd.created_at,
			fd.updated_at,
			fd.version,
			fdn.id AS normalized_id,
			fdn.source_updated_at AS existing_source_updated_at,
			fdn.normalization_version AS existing_normalization_version
		`).
		Joins("JOIN file f ON f.id = fd.file_id").
		Joins("LEFT JOIN file_data_normalized fdn ON fdn.source_row_id = fd.id").
		Where("f.is_delete = ?", false).
		Where("(fdn.id IS NULL OR fdn.source_updated_at < fd.updated_at OR fdn.normalization_version <> ?)", currentNormalizationVersion)

	if fileID != nil {
		query = query.Where("fd.file_id = ?", *fileID)
	}
	if version != nil {
		query = query.Where("fd.version = ?", *version)
	}

	var candidates []normalizationCandidate
	if err := query.
		Order(`
			CASE WHEN fd.version = f.version THEN 0 ELSE 1 END ASC,
			fd.file_id ASC,
			fd.version DESC,
			fd.updated_at ASC,
			fd.id ASC
		`).
		Limit(limit).
		Scan(&candidates).Error; err != nil {
		return nil, err
	}

	result := &NormalizationSyncResult{}
	if len(candidates) == 0 {
		return result, nil
	}

	for _, candidate := range candidates {
		result.Processed++

		payloadJSON, searchText, canonicalName, canonicalCommunity, canonicalSchool, normalizeErr := normalizeRowData(candidate.RowData)
		status := "ready"
		errorMessage := ""
		if normalizeErr != nil {
			result.Failed++
			status = "failed"
			errorMessage = normalizeErr.Error()
			payloadJSON = datatypes.JSON([]byte("{}"))
			searchText = ""
			canonicalName = ""
			canonicalCommunity = ""
			canonicalSchool = ""
		}

		now := time.Now()
		record := FileDataNormalized{
			SourceRowID:          candidate.ID,
			FileID:               candidate.FileID,
			Version:              candidate.Version,
			RowDataNormalized:    payloadJSON,
			SearchText:           searchText,
			CanonicalName:        canonicalName,
			CanonicalCommunity:   canonicalCommunity,
			CanonicalSchool:      canonicalSchool,
			Status:               status,
			ErrorMessage:         errorMessage,
			SourceCreatedAt:      candidate.CreatedAt,
			SourceUpdatedAt:      candidate.UpdatedAt,
			NormalizationVersion: currentNormalizationVersion,
			NormalizedAt:         now,
		}

		if err := db.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "source_row_id"}},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"file_id":               record.FileID,
				"version":               record.Version,
				"row_data_normalized":   record.RowDataNormalized,
				"search_text":           record.SearchText,
				"canonical_name":        record.CanonicalName,
				"canonical_community":   record.CanonicalCommunity,
				"canonical_school":      record.CanonicalSchool,
				"status":                record.Status,
				"error_message":         record.ErrorMessage,
				"source_created_at":     record.SourceCreatedAt,
				"source_updated_at":     record.SourceUpdatedAt,
				"normalization_version": record.NormalizationVersion,
				"normalized_at":         record.NormalizedAt,
				"updated_at":            now,
			}),
		}).Create(&record).Error; err != nil {
			return nil, err
		}

		if candidate.NormalizedID == nil {
			result.Inserted++
		} else {
			result.Updated++
		}
	}

	return result, nil
}

func normalizeRowData(rowData datatypes.JSON) (datatypes.JSON, string, string, string, string, error) {
	var raw map[string]interface{}
	if err := json.Unmarshal(rowData, &raw); err != nil {
		return nil, "", "", "", "", fmt.Errorf("invalid row data json: %w", err)
	}

	payload := normalizedRowPayload{
		Fields: make(map[string]normalizedField, len(raw)),
	}

	searchTokens := make([]string, 0)
	names := make([]string, 0)
	communities := make([]string, 0)
	schools := make([]string, 0)
	locations := make([]string, 0)

	fieldNames := make([]string, 0, len(raw))
	for fieldName := range raw {
		fieldNames = append(fieldNames, fieldName)
	}
	sort.Strings(fieldNames)

	for _, fieldName := range fieldNames {
		rawValue := stringifyRowValue(raw[fieldName])
		role := inferFieldRole(fieldName)
		normalizedValue := normalizeSearchValue(rawValue)
		tokens := tokenizeSearchValue(rawValue)

		field := normalizedField{
			Raw:        rawValue,
			Normalized: normalizedValue,
			Tokens:     tokens,
			Role:       role,
		}

		if role == "date" {
			field.DateHint = inferDateHint(rawValue)
		}

		payload.Fields[fieldName] = field
		searchTokens = append(searchTokens, tokens...)

		switch role {
		case "name":
			if normalizedValue != "" {
				names = append(names, normalizedValue)
			}
		case "community":
			if normalizedValue != "" {
				communities = append(communities, normalizedValue)
			}
		case "school":
			if normalizedValue != "" {
				schools = append(schools, normalizedValue)
			}
		case "location":
			if normalizedValue != "" {
				locations = append(locations, normalizedValue)
			}
		}
	}

	payload.Names = uniqueStrings(names)
	payload.Communities = uniqueStrings(communities)
	payload.Schools = uniqueStrings(schools)
	payload.Locations = uniqueStrings(locations)
	payload.SearchTokens = uniqueStrings(searchTokens)

	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, "", "", "", "", fmt.Errorf("failed to marshal normalized row: %w", err)
	}

	return datatypes.JSON(jsonBytes),
		strings.Join(payload.SearchTokens, " "),
		firstOrEmpty(payload.Names),
		firstOrEmpty(payload.Communities),
		firstOrEmpty(payload.Schools),
		nil
}

func stringifyRowValue(value interface{}) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func inferFieldRole(fieldName string) string {
	normalizedField := normalizeSearchValue(fieldName)
	switch {
	case strings.Contains(normalizedField, "community"), strings.Contains(normalizedField, "reserve"), strings.Contains(normalizedField, "first nation"):
		return "community"
	case strings.Contains(normalizedField, "school"), strings.Contains(normalizedField, "residential"), strings.Contains(normalizedField, "institution"):
		return "school"
	case strings.Contains(normalizedField, "place"), strings.Contains(normalizedField, "location"), strings.Contains(normalizedField, "town"), strings.Contains(normalizedField, "city"):
		return "location"
	case strings.Contains(normalizedField, "date"), strings.Contains(normalizedField, "dob"), strings.Contains(normalizedField, "birth"), strings.Contains(normalizedField, "death"):
		return "date"
	case strings.Contains(normalizedField, "name"), strings.Contains(normalizedField, "student"), strings.Contains(normalizedField, "child"), strings.Contains(normalizedField, "person"):
		return "name"
	default:
		return "text"
	}
}

func normalizeSearchValue(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}

	var b strings.Builder
	lastSpace := false
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			b.WriteRune(r)
			lastSpace = false
			continue
		}
		if lastSpace {
			continue
		}
		b.WriteByte(' ')
		lastSpace = true
	}

	return strings.TrimSpace(normalizationWhitespaceRe.ReplaceAllString(b.String(), " "))
}

func tokenizeSearchValue(value string) []string {
	normalized := normalizeSearchValue(value)
	if normalized == "" {
		return nil
	}

	parts := strings.Fields(normalized)
	tokens := make([]string, 0, len(parts))
	for _, part := range parts {
		if len(part) <= 1 {
			continue
		}
		tokens = append(tokens, part)
		if strings.HasSuffix(part, "ing") && len(part) > 4 {
			tokens = append(tokens, strings.TrimSuffix(part, "ing"))
		}
		if strings.HasSuffix(part, "ed") && len(part) > 3 {
			tokens = append(tokens, strings.TrimSuffix(part, "ed"))
		}
		if strings.HasSuffix(part, "s") && len(part) > 3 {
			tokens = append(tokens, strings.TrimSuffix(part, "s"))
		}
	}

	return uniqueStrings(tokens)
}

func inferDateHint(value string) *normalizedDate {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return nil
	}

	normalized := strings.ToLower(raw)
	years := uniqueInts(extractYears(raw))
	if len(years) == 0 {
		return &normalizedDate{Raw: raw, Kind: "unparsed"}
	}

	approximate := strings.Contains(normalized, "abt") ||
		strings.Contains(normalized, "about") ||
		strings.Contains(normalized, "approx") ||
		strings.Contains(normalized, "circa") ||
		strings.Contains(normalized, "ca ") ||
		strings.Contains(normalized, "~")

	kind := "year"
	if approximate {
		kind = "approximate_year"
	}
	if len(years) > 1 {
		kind = "range"
	}
	if _, err := tryParseDate(raw); err == nil {
		kind = "exact_date"
	}

	lower := years[0]
	upper := years[len(years)-1]

	return &normalizedDate{
		Raw:         raw,
		Kind:        kind,
		LowerYear:   intPtr(lower),
		UpperYear:   intPtr(upper),
		Approximate: approximate,
	}
}

func extractYears(value string) []int {
	matches := normalizationYearRe.FindAllString(value, -1)
	years := make([]int, 0, len(matches))
	for _, match := range matches {
		year, err := strconv.Atoi(match)
		if err != nil {
			continue
		}
		years = append(years, year)
	}
	sort.Ints(years)
	return years
}

func tryParseDate(value string) (time.Time, error) {
	layouts := []string{
		"2006-01-02",
		"02-01-2006",
		"01-02-2006",
		"2-1-2006",
		"02/01/2006",
		"01/02/2006",
		"2/1/2006",
		"02 Jan 2006",
		"2 Jan 2006",
		"02 January 2006",
		"2 January 2006",
		"02-Jan-2006",
		"2-Jan-2006",
		"02-Jan-06",
		"2-Jan-06",
		"Jan 2 2006",
		"January 2 2006",
		"2006/01/02",
		"2006.01.02",
	}

	for _, layout := range layouts {
		parsed, err := time.Parse(layout, strings.TrimSpace(value))
		if err == nil {
			return parsed, nil
		}
	}

	return time.Time{}, fmt.Errorf("unparsed date")
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func uniqueInts(values []int) []int {
	if len(values) == 0 {
		return nil
	}
	out := make([]int, 0, len(values))
	last := values[0] - 1
	for _, value := range values {
		if len(out) > 0 && value == last {
			continue
		}
		out = append(out, value)
		last = value
	}
	return out
}

func firstOrEmpty(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func intPtr(v int) *int {
	return &v
}
