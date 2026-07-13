package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	filepkg "nordik-drive-api/internal/file"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const defaultSyncSSLMode = "disable"

type syncConfig struct {
	DBHost     string
	DBPort     string
	DBName     string
	DBUser     string
	DBPass     string
	SSLMode    string
	FileName   string
	Version    int
	BatchSize  int
	MaxBatches int
}

type syncReport struct {
	RunAtUTC                    time.Time                        `json:"run_at_utc"`
	CurrentNormalizationVersion int                              `json:"current_normalization_version"`
	FileName                    string                           `json:"file_name,omitempty"`
	FileID                      *uint                            `json:"file_id,omitempty"`
	Version                     int                              `json:"version,omitempty"`
	Result                      *filepkg.NormalizationSyncResult `json:"result,omitempty"`
}

func main() {
	cfg := parseFlags()
	if err := validateConfig(cfg); err != nil {
		exitErr(err)
	}

	db, err := gorm.Open(postgres.Open(buildDSN(cfg)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		exitErr(err)
	}

	options := filepkg.NormalizationSyncOptions{
		BatchSize:  cfg.BatchSize,
		MaxBatches: cfg.MaxBatches,
	}

	report := syncReport{
		RunAtUTC:                    time.Now().UTC(),
		CurrentNormalizationVersion: filepkg.CurrentNormalizationVersion(),
		Version:                     cfg.Version,
	}

	if strings.TrimSpace(cfg.FileName) != "" {
		fileRecord, err := resolveFile(db, cfg.FileName)
		if err != nil {
			exitErr(err)
		}
		options.FileID = &fileRecord.ID
		report.FileID = &fileRecord.ID
		report.FileName = fileRecord.Filename
		if cfg.Version <= 0 {
			cfg.Version = fileRecord.Version
			report.Version = fileRecord.Version
		}
	}

	if cfg.Version > 0 {
		versionCopy := cfg.Version
		options.Version = &versionCopy
	}

	result, err := filepkg.RunNormalizationSync(db, options)
	if err != nil {
		exitErr(err)
	}
	report.Result = result

	fmt.Printf("{\"run_at_utc\":\"%s\",\"current_normalization_version\":%d,\"file_name\":%q,\"file_id\":%s,\"version\":%d,\"processed\":%d,\"inserted\":%d,\"updated\":%d,\"failed\":%d}\n",
		report.RunAtUTC.Format(time.RFC3339),
		report.CurrentNormalizationVersion,
		report.FileName,
		formatOptionalUint(report.FileID),
		report.Version,
		result.Processed,
		result.Inserted,
		result.Updated,
		result.Failed,
	)
}

func parseFlags() syncConfig {
	cfg := syncConfig{}
	flag.StringVar(&cfg.DBHost, "db-host", getenvDefault("NORMALIZE_SYNC_DB_HOST", ""), "Postgres host")
	flag.StringVar(&cfg.DBPort, "db-port", getenvDefault("NORMALIZE_SYNC_DB_PORT", "5432"), "Postgres port")
	flag.StringVar(&cfg.DBName, "db-name", getenvDefault("NORMALIZE_SYNC_DB_NAME", "postgres"), "Postgres database name")
	flag.StringVar(&cfg.DBUser, "db-user", getenvDefault("NORMALIZE_SYNC_DB_USER", ""), "Postgres user")
	flag.StringVar(&cfg.DBPass, "db-password", getenvDefault("NORMALIZE_SYNC_DB_PASSWORD", ""), "Postgres password")
	flag.StringVar(&cfg.SSLMode, "db-sslmode", getenvDefault("NORMALIZE_SYNC_DB_SSLMODE", defaultSyncSSLMode), "Postgres sslmode")
	flag.StringVar(&cfg.FileName, "file-name", getenvDefault("NORMALIZE_SYNC_FILE_NAME", ""), "Target filename or substring")
	flag.IntVar(&cfg.Version, "version", 0, "Target version (defaults to latest file version when file-name is set)")
	flag.IntVar(&cfg.BatchSize, "batch-size", 250, "Rows per batch")
	flag.IntVar(&cfg.MaxBatches, "max-batches", 25, "Maximum batches to process")
	flag.Parse()
	return cfg
}

func validateConfig(cfg syncConfig) error {
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
	case cfg.BatchSize <= 0:
		return errors.New("batch size must be greater than zero")
	case cfg.MaxBatches <= 0:
		return errors.New("max batches must be greater than zero")
	default:
		return nil
	}
}

func getenvDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func buildDSN(cfg syncConfig) string {
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

func resolveFile(db *gorm.DB, fileName string) (*filepkg.File, error) {
	fileName = strings.TrimSpace(fileName)

	var file filepkg.File
	err := db.Where("is_delete = ? AND lower(filename) = lower(?)", false, fileName).
		Order("version DESC, id DESC").
		First(&file).Error
	if err == nil {
		return &file, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	var matches []filepkg.File
	if err := db.Where("is_delete = ? AND filename ILIKE ?", false, "%"+fileName+"%").
		Order("filename ASC, version DESC, id DESC").
		Limit(10).
		Find(&matches).Error; err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("no file found for %q", fileName)
	}
	if len(matches) > 1 {
		names := make([]string, 0, len(matches))
		for _, match := range matches {
			names = append(names, fmt.Sprintf("%s (id=%d version=%d)", match.Filename, match.ID, match.Version))
		}
		return nil, fmt.Errorf("multiple files matched %q: %s", fileName, strings.Join(names, "; "))
	}
	return &matches[0], nil
}

func formatOptionalUint(value *uint) string {
	if value == nil {
		return "null"
	}
	return fmt.Sprintf("%d", *value)
}

func exitErr(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
