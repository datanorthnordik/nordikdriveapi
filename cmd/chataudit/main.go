package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	filepkg "nordik-drive-api/internal/file"

	_ "github.com/lib/pq"
)

const (
	defaultSSLMode            = "disable"
	maxDistinctValuesPerField = 5000
	maxSampleValues           = 5
	maxTopValueRows           = 10
)

var (
	whitespaceRe = regexp.MustCompile(`\s+`)
	yearOnlyRe   = regexp.MustCompile(`^\d{4}$`)
	yearRangeRe  = regexp.MustCompile(`^\d{4}\s*[-/]\s*\d{4}$`)
	isoDateRe    = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	slashDateRe  = regexp.MustCompile(`^\d{1,2}/\d{1,2}/\d{2,4}$`)
	dashDateRe   = regexp.MustCompile(`^\d{1,2}-\d{1,2}-\d{2,4}$`)
	monthDateRe  = regexp.MustCompile(`(?i)^(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec|january|february|march|april|june|july|august|september|october|november|december)\b`)
	sourceTagRe  = regexp.MustCompile(`(?i)(\((nctr|cirnac|source|office of the registrar general|library and archives canada|coroner|vital stats)|\brg\s*\d+\b|\bvolume\s*\d+\b|\breel\s*[a-z]?\d+\b|\bimage\s*\d+\b|\bfile\s+\d+(?:[-/]\d+)+(?:,\s*part\s*\d+)?\b|\bstudent register\b|\baccession\s*\d{4}[-_/]\d+)`)
)

type auditConfig struct {
	DBHost   string
	DBPort   string
	DBName   string
	DBUser   string
	DBPass   string
	SSLMode  string
	FileName string
}

type fileRecord struct {
	ID              int64
	Filename        string
	Version         int
	Rows            int
	CommunityFilter bool
}

type dataConfigRecord struct {
	Version   int
	Checksum  string
	ConfigRaw []byte
	UpdatedAt time.Time
}

type normalizedSummary struct {
	Total             int
	StatusCounts      map[string]int
	EmptyCanonical    map[string]int
	FailedSampleError string
}

type auditReport struct {
	RunAtUTC                   time.Time                 `json:"run_at_utc"`
	File                       fileRecord                `json:"file"`
	ResolvedBy                 string                    `json:"resolved_by"`
	DataConfig                 *dataConfigSummary        `json:"data_config,omitempty"`
	Normalized                 normalizedSummary         `json:"normalized"`
	TotalRowsScanned           int                       `json:"total_rows_scanned"`
	HeadersCount               int                       `json:"headers_count"`
	PromptEstimateBytes        int                       `json:"prompt_estimate_bytes"`
	CompactPromptEstimateBytes int                       `json:"compact_prompt_estimate_bytes"`
	CompactPromptReductionPct  float64                   `json:"compact_prompt_reduction_pct"`
	AverageRowBytes            float64                   `json:"average_row_bytes"`
	P50RowBytes                int                       `json:"p50_row_bytes"`
	P95RowBytes                int                       `json:"p95_row_bytes"`
	MaxRowBytes                int                       `json:"max_row_bytes"`
	FieldSummaries             []fieldSummary            `json:"field_summaries"`
	RoleSummaries              map[string][]fieldSummary `json:"role_summaries"`
	DateFormatSummaries        []dateFieldSummary        `json:"date_format_summaries"`
	LargeRows                  []largeRowSummary         `json:"large_rows"`
	PreprocessingSignals       preprocessingSignals      `json:"preprocessing_signals"`
	Phase3Readiness            phase3Readiness           `json:"phase3_readiness"`
}

type dataConfigSummary struct {
	Version      int      `json:"version"`
	Checksum     string   `json:"checksum"`
	UpdatedAtUTC string   `json:"updated_at_utc"`
	TopLevelKeys []string `json:"top_level_keys"`
}

type fieldSummary struct {
	Name                    string   `json:"name"`
	Role                    string   `json:"role"`
	PresentRows             int      `json:"present_rows"`
	NonEmptyRows            int      `json:"non_empty_rows"`
	BlankRows               int      `json:"blank_rows"`
	MissingRows             int      `json:"missing_rows"`
	CompletenessPct         float64  `json:"completeness_pct"`
	DistinctValues          string   `json:"distinct_values"`
	SampleValues            []string `json:"sample_values,omitempty"`
	SourceTagValueCount     int      `json:"source_tag_value_count"`
	NormalizedVariantGroups int      `json:"normalized_variant_groups"`
}

type dateFieldSummary struct {
	Name          string         `json:"name"`
	Role          string         `json:"role"`
	FormatCounts  map[string]int `json:"format_counts"`
	ExampleValues []string       `json:"example_values,omitempty"`
}

type largeRowSummary struct {
	RowID          int64    `json:"row_id"`
	RowBytes       int      `json:"row_bytes"`
	FieldCount     int      `json:"field_count"`
	LargestFields  []string `json:"largest_fields"`
	SourceTagCount int      `json:"source_tag_count"`
}

type preprocessingSignals struct {
	FieldsWithSourceTags        []string            `json:"fields_with_source_tags"`
	HighPriorityCanonicalFields []string            `json:"high_priority_canonical_fields"`
	VariantHeavyFields          []variantHeavyField `json:"variant_heavy_fields"`
	DateCleaningPriorityFields  []string            `json:"date_cleaning_priority_fields"`
	SparseImportantFields       []string            `json:"sparse_important_fields"`
}

type phase3Readiness struct {
	DataConfigActive            bool               `json:"data_config_active"`
	CurrentNormalizationVersion int                `json:"current_normalization_version"`
	RowsOnCurrentVersion        int                `json:"rows_on_current_version"`
	DefaultBundleRows           int                `json:"default_bundle_rows"`
	NarrativeBundleRows         int                `json:"narrative_bundle_rows"`
	DetectedRecordProfiles      []string           `json:"detected_record_profiles,omitempty"`
	CanonicalCoverage           map[string]float64 `json:"canonical_coverage,omitempty"`
	CompactPromptEstimateBytes  int                `json:"compact_prompt_estimate_bytes"`
	CompactPromptReductionPct   float64            `json:"compact_prompt_reduction_pct"`
	PrerequisitesMet            bool               `json:"prerequisites_met"`
	MissingPrerequisites        []string           `json:"missing_prerequisites,omitempty"`
}

type auditNormalizedPayload struct {
	Canonical *auditCanonicalPayload `json:"canonical,omitempty"`
	Chat      *auditChatPayload      `json:"chat,omitempty"`
}

type auditCanonicalPayload struct {
	RecordProfile string `json:"record_profile,omitempty"`
	DisplayName   string `json:"display_name,omitempty"`
	Community     string `json:"community,omitempty"`
	School        string `json:"school,omitempty"`
}

type auditChatPayload struct {
	DefaultBundle   map[string]any `json:"default_bundle,omitempty"`
	NarrativeBundle map[string]any `json:"narrative_bundle,omitempty"`
}

type variantHeavyField struct {
	Name   string              `json:"name"`
	Role   string              `json:"role"`
	Groups []normalizedVariant `json:"groups"`
}

type normalizedVariant struct {
	Normalized string   `json:"normalized"`
	RawValues  []string `json:"raw_values"`
}

type fieldAccumulator struct {
	Name                string
	Role                string
	PresentRows         int
	NonEmptyRows        int
	BlankRows           int
	SourceTagValueCount int
	Samples             []string
	sampleSet           map[string]struct{}
	DistinctValues      map[string]struct{}
	DistinctOverflow    bool
	NormalizedVariants  map[string]map[string]struct{}
	DateFormats         map[string]int
	DateExamples        []string
	dateExampleSet      map[string]struct{}
}

type rowAccumulator struct {
	RowSizes   []int
	LargeRows  []largeRowSummary
	TotalBytes int
}

func main() {
	cfg := parseFlags()
	if err := validateConfig(cfg); err != nil {
		exitErr(err)
	}

	db, err := sql.Open("postgres", buildDSN(cfg))
	if err != nil {
		exitErr(err)
	}
	defer db.Close()

	db.SetConnMaxLifetime(2 * time.Minute)
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(2)

	if err := db.Ping(); err != nil {
		exitErr(fmt.Errorf("db ping failed: %w", err))
	}

	fileRec, resolvedBy, err := resolveFile(db, cfg.FileName)
	if err != nil {
		exitErr(err)
	}

	report, err := runAudit(db, fileRec, resolvedBy)
	if err != nil {
		exitErr(err)
	}

	out, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		exitErr(err)
	}

	fmt.Println(string(out))
}

func parseFlags() auditConfig {
	cfg := auditConfig{}
	flag.StringVar(&cfg.DBHost, "db-host", getenvDefault("CHAT_AUDIT_DB_HOST", ""), "Postgres host")
	flag.StringVar(&cfg.DBPort, "db-port", getenvDefault("CHAT_AUDIT_DB_PORT", "5432"), "Postgres port")
	flag.StringVar(&cfg.DBName, "db-name", getenvDefault("CHAT_AUDIT_DB_NAME", "postgres"), "Postgres database name")
	flag.StringVar(&cfg.DBUser, "db-user", getenvDefault("CHAT_AUDIT_DB_USER", ""), "Postgres user")
	flag.StringVar(&cfg.DBPass, "db-password", getenvDefault("CHAT_AUDIT_DB_PASSWORD", ""), "Postgres password")
	flag.StringVar(&cfg.SSLMode, "db-sslmode", getenvDefault("CHAT_AUDIT_DB_SSLMODE", defaultSSLMode), "Postgres sslmode")
	flag.StringVar(&cfg.FileName, "file-name", getenvDefault("CHAT_AUDIT_FILE_NAME", ""), "Target filename or substring")
	flag.Parse()
	return cfg
}

func getenvDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func validateConfig(cfg auditConfig) error {
	switch {
	case strings.TrimSpace(cfg.DBHost) == "":
		return errors.New("missing db host")
	case strings.TrimSpace(cfg.DBPort) == "":
		return errors.New("missing db port")
	case strings.TrimSpace(cfg.DBName) == "":
		return errors.New("missing db name")
	case strings.TrimSpace(cfg.DBUser) == "":
		return errors.New("missing db user")
	case strings.TrimSpace(cfg.DBPass) == "":
		return errors.New("missing db password")
	case strings.TrimSpace(cfg.FileName) == "":
		return errors.New("missing file name")
	default:
		return nil
	}
}

func buildDSN(cfg auditConfig) string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.DBHost,
		cfg.DBPort,
		cfg.DBUser,
		cfg.DBPass,
		cfg.DBName,
		cfg.SSLMode,
	)
}

func resolveFile(db *sql.DB, fileName string) (fileRecord, string, error) {
	fileName = strings.TrimSpace(fileName)

	var rec fileRecord
	err := db.QueryRow(`
		SELECT id, filename, version, rows, community_filter
		FROM file
		WHERE is_delete = false
		  AND lower(filename) = lower($1)
		ORDER BY version DESC, id DESC
		LIMIT 1
	`, fileName).Scan(&rec.ID, &rec.Filename, &rec.Version, &rec.Rows, &rec.CommunityFilter)
	if err == nil {
		return rec, "exact", nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fileRecord{}, "", err
	}

	rows, err := db.Query(`
		SELECT id, filename, version, rows, community_filter
		FROM file
		WHERE is_delete = false
		  AND filename ILIKE $1
		ORDER BY filename ASC, version DESC, id DESC
		LIMIT 10
	`, "%"+fileName+"%")
	if err != nil {
		return fileRecord{}, "", err
	}
	defer rows.Close()

	var matches []fileRecord
	for rows.Next() {
		var item fileRecord
		if scanErr := rows.Scan(&item.ID, &item.Filename, &item.Version, &item.Rows, &item.CommunityFilter); scanErr != nil {
			return fileRecord{}, "", scanErr
		}
		matches = append(matches, item)
	}
	if len(matches) == 0 {
		return fileRecord{}, "", fmt.Errorf("no file found for %q", fileName)
	}
	if len(matches) > 1 {
		names := make([]string, 0, len(matches))
		for _, match := range matches {
			names = append(names, fmt.Sprintf("%s (id=%d version=%d)", match.Filename, match.ID, match.Version))
		}
		return fileRecord{}, "", fmt.Errorf("multiple files matched %q: %s", fileName, strings.Join(names, "; "))
	}

	return matches[0], "substring", nil
}

func runAudit(db *sql.DB, fileRec fileRecord, resolvedBy string) (*auditReport, error) {
	report := &auditReport{
		RunAtUTC:      time.Now().UTC(),
		File:          fileRec,
		ResolvedBy:    resolvedBy,
		RoleSummaries: make(map[string][]fieldSummary),
	}

	dc, err := loadDataConfigSummary(db, fileRec.ID)
	if err != nil {
		return nil, err
	}
	report.DataConfig = dc

	normalized, err := loadNormalizedSummary(db, fileRec.ID, fileRec.Version)
	if err != nil {
		return nil, err
	}
	report.Normalized = normalized

	fields := make(map[string]*fieldAccumulator)
	rowsAcc := rowAccumulator{}

	rows, err := db.Query(`
		SELECT id, row_data::text
		FROM file_data
		WHERE file_id = $1 AND version = $2
		ORDER BY id ASC
	`, fileRec.ID, fileRec.Version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var rowID int64
		var rowJSON string
		if scanErr := rows.Scan(&rowID, &rowJSON); scanErr != nil {
			return nil, scanErr
		}

		report.TotalRowsScanned++
		rowBytes := len([]byte(rowJSON))
		rowsAcc.TotalBytes += rowBytes
		rowsAcc.RowSizes = append(rowsAcc.RowSizes, rowBytes)
		report.PromptEstimateBytes += rowBytes + estimatePromptRowOverhead(report.TotalRowsScanned)

		var raw map[string]any
		if err := json.Unmarshal([]byte(rowJSON), &raw); err != nil {
			continue
		}

		largestFields := topLargestFields(raw, 3)
		sourceTagCount := 0

		for key, rawValue := range raw {
			acc := fields[key]
			if acc == nil {
				acc = &fieldAccumulator{
					Name:               key,
					Role:               inferFieldRole(key),
					sampleSet:          make(map[string]struct{}),
					DistinctValues:     make(map[string]struct{}),
					NormalizedVariants: make(map[string]map[string]struct{}),
					DateFormats:        make(map[string]int),
					dateExampleSet:     make(map[string]struct{}),
				}
				fields[key] = acc
			}

			acc.PresentRows++
			value := stringify(rawValue)
			if strings.TrimSpace(value) == "" {
				acc.BlankRows++
				continue
			}

			acc.NonEmptyRows++
			if hasSourceContamination(value) {
				acc.SourceTagValueCount++
				sourceTagCount++
			}
			acc.addSample(value)
			acc.addDistinct(value)
			acc.addNormalizedVariant(value)
			acc.addDateSignal(value)
		}

		rowsAcc.maybeAddLargeRow(largeRowSummary{
			RowID:          rowID,
			RowBytes:       rowBytes,
			FieldCount:     len(raw),
			LargestFields:  largestFields,
			SourceTagCount: sourceTagCount,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	report.HeadersCount = len(fields)
	report.LargeRows = rowsAcc.sortedLargeRows()
	report.AverageRowBytes = averageBytes(rowsAcc.RowSizes)
	report.P50RowBytes = percentileInt(rowsAcc.RowSizes, 0.50)
	report.P95RowBytes = percentileInt(rowsAcc.RowSizes, 0.95)
	report.MaxRowBytes = percentileInt(rowsAcc.RowSizes, 1.0)

	report.FieldSummaries = buildFieldSummaries(fields, report.TotalRowsScanned)
	report.DateFormatSummaries = buildDateSummaries(fields)
	report.PreprocessingSignals = buildSignals(fields, report.TotalRowsScanned)
	report.PromptEstimateBytes += estimatePromptFixedOverhead()

	readiness, err := loadPhase3Readiness(db, fileRec.ID, fileRec.Version, report.TotalRowsScanned, report.PromptEstimateBytes, report.DataConfig != nil)
	if err != nil {
		return nil, err
	}
	report.Phase3Readiness = readiness
	report.CompactPromptEstimateBytes = readiness.CompactPromptEstimateBytes
	report.CompactPromptReductionPct = readiness.CompactPromptReductionPct

	grouped := make(map[string][]fieldSummary)
	for _, field := range report.FieldSummaries {
		grouped[field.Role] = append(grouped[field.Role], field)
	}
	for role := range grouped {
		sort.Slice(grouped[role], func(i, j int) bool {
			left := grouped[role][i]
			right := grouped[role][j]
			if left.NonEmptyRows != right.NonEmptyRows {
				return left.NonEmptyRows > right.NonEmptyRows
			}
			return left.Name < right.Name
		})
		if len(grouped[role]) > 8 {
			grouped[role] = grouped[role][:8]
		}
		report.RoleSummaries[role] = grouped[role]
	}

	return report, nil
}

func loadPhase3Readiness(db *sql.DB, fileID int64, version int, totalRows int, rawPromptEstimateBytes int, dataConfigActive bool) (phase3Readiness, error) {
	readiness := phase3Readiness{
		DataConfigActive:            dataConfigActive,
		CurrentNormalizationVersion: filepkg.CurrentNormalizationVersion(),
		CanonicalCoverage:           map[string]float64{},
	}

	rows, err := db.Query(`
		SELECT status, normalization_version, row_data_normalized::text
		FROM file_data_normalized
		WHERE file_id = $1 AND version = $2
		ORDER BY source_row_id ASC
	`, fileID, version)
	if err != nil {
		readiness.MissingPrerequisites = append(readiness.MissingPrerequisites, "file_data_normalized rows unavailable")
		readiness.PrerequisitesMet = false
		return readiness, nil
	}
	defer rows.Close()

	type compactPromptRow struct {
		RowRef  string         `json:"row_ref"`
		RowData map[string]any `json:"row_data"`
	}

	compactRows := make([]compactPromptRow, 0)
	profiles := make(map[string]struct{})
	displayNameCount := 0
	communityCount := 0
	schoolCount := 0

	for rows.Next() {
		var status string
		var normalizationVersion int
		var payloadJSON string
		if scanErr := rows.Scan(&status, &normalizationVersion, &payloadJSON); scanErr != nil {
			return readiness, scanErr
		}
		if normalizationVersion == readiness.CurrentNormalizationVersion {
			readiness.RowsOnCurrentVersion++
		}
		if strings.TrimSpace(status) != "ready" {
			continue
		}

		var payload auditNormalizedPayload
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			readiness.MissingPrerequisites = appendUnique(readiness.MissingPrerequisites, "row_data_normalized contains invalid JSON")
			continue
		}

		if payload.Canonical != nil {
			if strings.TrimSpace(payload.Canonical.RecordProfile) != "" {
				profiles[payload.Canonical.RecordProfile] = struct{}{}
			}
			if strings.TrimSpace(payload.Canonical.DisplayName) != "" {
				displayNameCount++
			}
			if strings.TrimSpace(payload.Canonical.Community) != "" {
				communityCount++
			}
			if strings.TrimSpace(payload.Canonical.School) != "" {
				schoolCount++
			}
		}

		if payload.Chat != nil && len(payload.Chat.DefaultBundle) > 0 {
			readiness.DefaultBundleRows++
			compactRows = append(compactRows, compactPromptRow{
				RowRef:  "R" + strconv.Itoa(len(compactRows)+1),
				RowData: payload.Chat.DefaultBundle,
			})
		}
		if payload.Chat != nil && len(payload.Chat.NarrativeBundle) > 0 {
			readiness.NarrativeBundleRows++
		}
	}
	if err := rows.Err(); err != nil {
		return readiness, err
	}

	readiness.DetectedRecordProfiles = sortedKeys(profiles)
	readiness.CanonicalCoverage["display_name"] = percent(displayNameCount, totalRows)
	readiness.CanonicalCoverage["community"] = percent(communityCount, totalRows)
	readiness.CanonicalCoverage["school"] = percent(schoolCount, totalRows)

	if len(compactRows) > 0 {
		payloadBytes, err := json.Marshal(compactRows)
		if err != nil {
			return readiness, err
		}
		readiness.CompactPromptEstimateBytes = len(payloadBytes) + estimatePromptFixedOverhead()
	}
	if rawPromptEstimateBytes > 0 && readiness.CompactPromptEstimateBytes > 0 {
		readiness.CompactPromptReductionPct = math.Round((1.0-(float64(readiness.CompactPromptEstimateBytes)/float64(rawPromptEstimateBytes)))*1000) / 10
	}

	if !readiness.DataConfigActive {
		readiness.MissingPrerequisites = append(readiness.MissingPrerequisites, "active data_config missing")
	}
	if totalRows > 0 && readiness.RowsOnCurrentVersion != totalRows {
		readiness.MissingPrerequisites = append(readiness.MissingPrerequisites, "normalized rows are not all on the current normalization version")
	}
	if totalRows > 0 && readiness.DefaultBundleRows != totalRows {
		readiness.MissingPrerequisites = append(readiness.MissingPrerequisites, "default chat bundles are missing for some rows")
	}
	if len(readiness.DetectedRecordProfiles) == 0 && readiness.DefaultBundleRows == 0 {
		readiness.MissingPrerequisites = append(readiness.MissingPrerequisites, "no reusable canonical or compact bundle payloads were generated")
	}

	readiness.PrerequisitesMet = len(readiness.MissingPrerequisites) == 0
	return readiness, nil
}

func loadDataConfigSummary(db *sql.DB, fileID int64) (*dataConfigSummary, error) {
	var rec dataConfigRecord
	err := db.QueryRow(`
		SELECT version, checksum, config, updated_at
		FROM data_config
		WHERE file_id = $1 AND is_active = true
		ORDER BY updated_at DESC, id DESC
		LIMIT 1
	`, fileID).Scan(&rec.Version, &rec.Checksum, &rec.ConfigRaw, &rec.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	var raw map[string]any
	if err := json.Unmarshal(rec.ConfigRaw, &raw); err != nil {
		return &dataConfigSummary{
			Version:      rec.Version,
			Checksum:     rec.Checksum,
			UpdatedAtUTC: rec.UpdatedAt.UTC().Format(time.RFC3339),
		}, nil
	}

	keys := make([]string, 0, len(raw))
	for key := range raw {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	return &dataConfigSummary{
		Version:      rec.Version,
		Checksum:     rec.Checksum,
		UpdatedAtUTC: rec.UpdatedAt.UTC().Format(time.RFC3339),
		TopLevelKeys: keys,
	}, nil
}

func loadNormalizedSummary(db *sql.DB, fileID int64, version int) (normalizedSummary, error) {
	summary := normalizedSummary{
		StatusCounts:   map[string]int{},
		EmptyCanonical: map[string]int{},
	}

	rows, err := db.Query(`
		SELECT status,
		       canonical_name,
		       canonical_community,
		       canonical_school,
		       COALESCE(error_message, '')
		FROM file_data_normalized
		WHERE file_id = $1 AND version = $2
	`, fileID, version)
	if err != nil {
		return summary, nil
	}
	defer rows.Close()

	for rows.Next() {
		var status, canonicalName, canonicalCommunity, canonicalSchool, errMsg string
		if scanErr := rows.Scan(&status, &canonicalName, &canonicalCommunity, &canonicalSchool, &errMsg); scanErr != nil {
			return summary, scanErr
		}
		summary.Total++
		summary.StatusCounts[status]++
		if strings.TrimSpace(canonicalName) == "" {
			summary.EmptyCanonical["canonical_name"]++
		}
		if strings.TrimSpace(canonicalCommunity) == "" {
			summary.EmptyCanonical["canonical_community"]++
		}
		if strings.TrimSpace(canonicalSchool) == "" {
			summary.EmptyCanonical["canonical_school"]++
		}
		if summary.FailedSampleError == "" && strings.TrimSpace(errMsg) != "" {
			summary.FailedSampleError = errMsg
		}
	}
	return summary, rows.Err()
}

func estimatePromptRowOverhead(position int) int {
	rowRef := "R" + strconv.Itoa(position)
	return len(`{"row_ref":"","row_data":}`) + len(rowRef) + 1
}

func estimatePromptFixedOverhead() int {
	return len(`Structured output requirements:
User question:
DATA (only source of truth):
`)
}

func topLargestFields(raw map[string]any, limit int) []string {
	type fieldSize struct {
		Name string
		Size int
	}
	items := make([]fieldSize, 0, len(raw))
	for key, value := range raw {
		items = append(items, fieldSize{
			Name: key,
			Size: len([]byte(stringify(value))),
		})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Size != items[j].Size {
			return items[i].Size > items[j].Size
		}
		return items[i].Name < items[j].Name
	})
	if len(items) > limit {
		items = items[:limit]
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, fmt.Sprintf("%s(%d)", item.Name, item.Size))
	}
	return out
}

func (r *rowAccumulator) maybeAddLargeRow(row largeRowSummary) {
	r.LargeRows = append(r.LargeRows, row)
	sort.Slice(r.LargeRows, func(i, j int) bool {
		if r.LargeRows[i].RowBytes != r.LargeRows[j].RowBytes {
			return r.LargeRows[i].RowBytes > r.LargeRows[j].RowBytes
		}
		return r.LargeRows[i].RowID < r.LargeRows[j].RowID
	})
	if len(r.LargeRows) > maxTopValueRows {
		r.LargeRows = r.LargeRows[:maxTopValueRows]
	}
}

func (r *rowAccumulator) sortedLargeRows() []largeRowSummary {
	out := append([]largeRowSummary(nil), r.LargeRows...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].RowBytes != out[j].RowBytes {
			return out[i].RowBytes > out[j].RowBytes
		}
		return out[i].RowID < out[j].RowID
	})
	return out
}

func (f *fieldAccumulator) addSample(value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	if len(f.Samples) >= maxSampleValues {
		return
	}
	if _, exists := f.sampleSet[value]; exists {
		return
	}
	f.sampleSet[value] = struct{}{}
	f.Samples = append(f.Samples, value)
}

func (f *fieldAccumulator) addDistinct(value string) {
	if f.DistinctOverflow {
		return
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	f.DistinctValues[value] = struct{}{}
	if len(f.DistinctValues) > maxDistinctValuesPerField {
		f.DistinctOverflow = true
		f.DistinctValues = nil
	}
}

func (f *fieldAccumulator) addNormalizedVariant(value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	normalized := normalizeValue(value)
	if normalized == "" {
		return
	}
	bucket := f.NormalizedVariants[normalized]
	if bucket == nil {
		bucket = make(map[string]struct{})
		f.NormalizedVariants[normalized] = bucket
	}
	if len(bucket) >= maxSampleValues {
		return
	}
	bucket[value] = struct{}{}
}

func (f *fieldAccumulator) addDateSignal(value string) {
	if f.Role != "date" {
		return
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	format := classifyDateFormat(value)
	f.DateFormats[format]++
	if len(f.DateExamples) >= maxSampleValues {
		return
	}
	key := format + "::" + value
	if _, exists := f.dateExampleSet[key]; exists {
		return
	}
	f.dateExampleSet[key] = struct{}{}
	f.DateExamples = append(f.DateExamples, value)
}

func buildFieldSummaries(fields map[string]*fieldAccumulator, totalRows int) []fieldSummary {
	out := make([]fieldSummary, 0, len(fields))
	for _, field := range fields {
		distinct := "unknown"
		if field.DistinctOverflow {
			distinct = fmt.Sprintf("%d+", maxDistinctValuesPerField)
		} else {
			distinct = strconv.Itoa(len(field.DistinctValues))
		}
		missingRows := totalRows - field.PresentRows
		if missingRows < 0 {
			missingRows = 0
		}
		out = append(out, fieldSummary{
			Name:                    field.Name,
			Role:                    field.Role,
			PresentRows:             field.PresentRows,
			NonEmptyRows:            field.NonEmptyRows,
			BlankRows:               field.BlankRows,
			MissingRows:             missingRows,
			CompletenessPct:         percent(field.NonEmptyRows, totalRows),
			DistinctValues:          distinct,
			SampleValues:            append([]string(nil), field.Samples...),
			SourceTagValueCount:     field.SourceTagValueCount,
			NormalizedVariantGroups: countVariantGroups(field.NormalizedVariants),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CompletenessPct != out[j].CompletenessPct {
			return out[i].CompletenessPct > out[j].CompletenessPct
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func buildDateSummaries(fields map[string]*fieldAccumulator) []dateFieldSummary {
	out := make([]dateFieldSummary, 0)
	for _, field := range fields {
		if field.Role != "date" || len(field.DateFormats) == 0 {
			continue
		}
		formatCounts := make(map[string]int, len(field.DateFormats))
		for key, value := range field.DateFormats {
			formatCounts[key] = value
		}
		out = append(out, dateFieldSummary{
			Name:          field.Name,
			Role:          field.Role,
			FormatCounts:  formatCounts,
			ExampleValues: append([]string(nil), field.DateExamples...),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

func buildSignals(fields map[string]*fieldAccumulator, totalRows int) preprocessingSignals {
	signals := preprocessingSignals{}

	for _, field := range fields {
		if field.SourceTagValueCount > 0 {
			signals.FieldsWithSourceTags = append(signals.FieldsWithSourceTags, field.Name)
		}

		switch field.Role {
		case "name", "community", "school", "date", "location":
			signals.HighPriorityCanonicalFields = appendUnique(signals.HighPriorityCanonicalFields, field.Name)
		}

		if field.Role == "date" && len(field.DateFormats) > 1 {
			signals.DateCleaningPriorityFields = append(signals.DateCleaningPriorityFields, field.Name)
		}

		if isImportantRole(field.Role) && percent(field.NonEmptyRows, totalRows) < 60 {
			signals.SparseImportantFields = append(signals.SparseImportantFields, field.Name)
		}
	}

	for _, field := range fields {
		groups := collectVariantGroups(field.NormalizedVariants)
		if len(groups) == 0 {
			continue
		}
		signals.VariantHeavyFields = append(signals.VariantHeavyFields, variantHeavyField{
			Name:   field.Name,
			Role:   field.Role,
			Groups: groups,
		})
	}

	sort.Strings(signals.FieldsWithSourceTags)
	sort.Strings(signals.HighPriorityCanonicalFields)
	sort.Strings(signals.DateCleaningPriorityFields)
	sort.Strings(signals.SparseImportantFields)
	sort.Slice(signals.VariantHeavyFields, func(i, j int) bool {
		if len(signals.VariantHeavyFields[i].Groups) != len(signals.VariantHeavyFields[j].Groups) {
			return len(signals.VariantHeavyFields[i].Groups) > len(signals.VariantHeavyFields[j].Groups)
		}
		return signals.VariantHeavyFields[i].Name < signals.VariantHeavyFields[j].Name
	})
	if len(signals.VariantHeavyFields) > 10 {
		signals.VariantHeavyFields = signals.VariantHeavyFields[:10]
	}

	return signals
}

func collectVariantGroups(variants map[string]map[string]struct{}) []normalizedVariant {
	out := make([]normalizedVariant, 0)
	for normalized, rawSet := range variants {
		if len(rawSet) < 2 {
			continue
		}
		rawValues := make([]string, 0, len(rawSet))
		for raw := range rawSet {
			rawValues = append(rawValues, raw)
		}
		sort.Strings(rawValues)
		out = append(out, normalizedVariant{
			Normalized: normalized,
			RawValues:  rawValues,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if len(out[i].RawValues) != len(out[j].RawValues) {
			return len(out[i].RawValues) > len(out[j].RawValues)
		}
		return out[i].Normalized < out[j].Normalized
	})
	if len(out) > 5 {
		out = out[:5]
	}
	return out
}

func countVariantGroups(variants map[string]map[string]struct{}) int {
	count := 0
	for _, rawSet := range variants {
		if len(rawSet) >= 2 {
			count++
		}
	}
	return count
}

func isImportantRole(role string) bool {
	switch role {
	case "name", "community", "school", "date":
		return true
	default:
		return false
	}
}

func classifyDateFormat(value string) string {
	value = strings.TrimSpace(value)
	switch {
	case value == "":
		return "blank"
	case isoDateRe.MatchString(value):
		return "iso_date"
	case yearOnlyRe.MatchString(value):
		return "year_only"
	case yearRangeRe.MatchString(value):
		return "year_range"
	case slashDateRe.MatchString(value):
		return "slash_date"
	case dashDateRe.MatchString(value):
		return "dash_date"
	case monthDateRe.MatchString(value):
		return "month_name_date"
	case strings.Contains(strings.ToLower(value), "abt") ||
		strings.Contains(strings.ToLower(value), "about") ||
		strings.Contains(strings.ToLower(value), "approx") ||
		strings.Contains(strings.ToLower(value), "circa"):
		return "approximate_text"
	case strings.Count(value, " ") >= 2:
		return "free_text"
	default:
		return "other"
	}
}

func inferFieldRole(fieldName string) string {
	normalized := normalizeValue(fieldName)
	switch {
	case strings.Contains(normalized, "number"), strings.Contains(normalized, "identifier"), strings.HasSuffix(normalized, " id"), strings.HasPrefix(normalized, "id "):
		return "identifier"
	case strings.Contains(normalized, "community"), strings.Contains(normalized, "reserve"), strings.Contains(normalized, "first nation"):
		return "community"
	case strings.Contains(normalized, "school"), strings.Contains(normalized, "residential"), strings.Contains(normalized, "institution"):
		return "school"
	case strings.Contains(normalized, "place"), strings.Contains(normalized, "location"), strings.Contains(normalized, "city"), strings.Contains(normalized, "town"):
		return "location"
	case strings.Contains(normalized, "cause"), strings.Contains(normalized, "injury"), strings.Contains(normalized, "disease"), strings.Contains(normalized, "incident"):
		return "cause"
	case strings.Contains(normalized, "date"), strings.Contains(normalized, "dob"), strings.Contains(normalized, "birth"), strings.Contains(normalized, "death"):
		return "date"
	case strings.Contains(normalized, "name"), strings.Contains(normalized, "student"), strings.Contains(normalized, "child"), strings.Contains(normalized, "resident"):
		return "name"
	case strings.Contains(normalized, "note"), strings.Contains(normalized, "comment"), strings.Contains(normalized, "summary"), strings.Contains(normalized, "details"):
		return "notes"
	default:
		return "text"
	}
}

func stringify(value any) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func normalizeValue(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}

	var b strings.Builder
	lastSpace := false
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
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
	return strings.TrimSpace(whitespaceRe.ReplaceAllString(b.String(), " "))
}

func hasSourceContamination(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	return sourceTagRe.MatchString(value)
}

func appendUnique(values []string, candidate string) []string {
	for _, value := range values {
		if value == candidate {
			return values
		}
	}
	return append(values, candidate)
}

func sortedKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func averageBytes(values []int) float64 {
	if len(values) == 0 {
		return 0
	}
	total := 0
	for _, value := range values {
		total += value
	}
	return float64(total) / float64(len(values))
}

func percentileInt(values []int, p float64) int {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]int(nil), values...)
	sort.Ints(sorted)
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := int(math.Ceil((float64(len(sorted)) * p) - 1.0))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func percent(part, total int) float64 {
	if total <= 0 {
		return 0
	}
	return math.Round((float64(part)/float64(total))*1000) / 10
}

func exitErr(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
