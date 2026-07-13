package file

import (
	"sort"
	"strings"
)

const (
	recordProfilePersonRegistry = "person_registry"
	recordProfileTabularRecord  = "tabular_record"
)

var canonicalFieldAliases = map[string][]string{
	"display_name": {
		"Name",
		"Full Name",
		"Resident Name",
		"Student Name",
		"Child Name",
		"Person Name",
	},
	"first_name": {
		"First Name",
		"First Names",
		"Given Name",
		"Given Names",
	},
	"middle_names": {
		"Middle Name",
		"Middle Names",
		"Other Given Names",
	},
	"last_name": {
		"Last Name",
		"Last Names",
		"Surname",
		"Family Name",
	},
	"indigenous_name": {
		"Indigenous Name",
		"Indigenous Names",
		"Indigenous Name/Spirit Name",
		"Spirit Name",
		"Traditional Name",
		"Native Name",
	},
	"identifier": {
		"Student Number",
		"Student ID",
		"ID Number",
		"Record Number",
		"Registration Number",
		"Admission Number",
		"Identifier",
	},
	"community": {
		"First Nation/Community",
		"First Nation",
		"Community",
		"Reserve",
		"Band",
		"Home Community",
	},
	"school": {
		"School",
		"Residential School",
		"Institution",
		"Home School",
	},
	"deceased_status": {
		"Deceased?",
		"Deceased",
		"Death Status",
		"Is Deceased",
	},
	"parents_names": {
		"Parents Names",
		"Parent Names",
		"Parents",
	},
	"mapping_location": {
		"Mapping Location",
		"Location",
		"Place",
		"Place Name",
		"Town",
		"City",
	},
	"lat": {
		"Lat",
		"Latitude",
	},
	"lng": {
		"Lng",
		"Longitude",
		"Long",
	},
	"birth_date": {
		"Date of Birth",
		"DOB",
		"Birth Date",
	},
	"admitted_date": {
		"Admitted",
		"Admission Date",
		"Date Admitted",
	},
	"discharged_date": {
		"Discharged",
		"Discharge Date",
		"Date Discharged",
	},
	"notes": {
		"Notes",
		"Note",
		"Comments",
		"Comment",
	},
	"additional_information": {
		"Additional Information",
		"Additional Info",
		"Extra Information",
	},
	"death_details": {
		"Death details",
		"Death Details",
		"Death Detail",
	},
	"photos": {
		"Photos",
		"Photo",
	},
}

type normalizationContext struct {
	FileID      uint
	Version     int
	SchemaHints *normalizationSchemaHints
}

type normalizedCanonicalRow struct {
	RecordProfile    string                   `json:"record_profile,omitempty"`
	DerivedFrom      map[string]string        `json:"derived_from,omitempty"`
	DisplayName      string                   `json:"display_name,omitempty"`
	FirstName        string                   `json:"first_name,omitempty"`
	MiddleNames      string                   `json:"middle_names,omitempty"`
	LastName         string                   `json:"last_name,omitempty"`
	IndigenousName   string                   `json:"indigenous_name,omitempty"`
	NameAliases      []string                 `json:"name_aliases,omitempty"`
	StudentNumber    string                   `json:"student_number,omitempty"`
	StudentNumberRaw string                   `json:"student_number_raw,omitempty"`
	Community        string                   `json:"community,omitempty"`
	School           string                   `json:"school,omitempty"`
	DeceasedStatus   string                   `json:"deceased_status,omitempty"`
	ParentsNames     string                   `json:"parents_names,omitempty"`
	MappingLocation  string                   `json:"mapping_location,omitempty"`
	Lat              string                   `json:"lat,omitempty"`
	Lng              string                   `json:"lng,omitempty"`
	Dates            normalizedCanonicalDates `json:"dates,omitempty"`
}

type normalizedCanonicalDates struct {
	Birth      *normalizedCanonicalDate `json:"birth,omitempty"`
	Admitted   *normalizedCanonicalDate `json:"admitted,omitempty"`
	Discharged *normalizedCanonicalDate `json:"discharged,omitempty"`
}

type normalizedCanonicalDate struct {
	Raw         string `json:"raw,omitempty"`
	ISO         string `json:"iso,omitempty"`
	Year        *int   `json:"year,omitempty"`
	UpperYear   *int   `json:"upper_year,omitempty"`
	Approximate bool   `json:"approximate,omitempty"`
	Kind        string `json:"kind,omitempty"`
}

type normalizedChatReadyRow struct {
	DefaultBundle   *normalizedChatBundle      `json:"default_bundle,omitempty"`
	NarrativeBundle *normalizedNarrativeBundle `json:"narrative_bundle,omitempty"`
	NarrativeFields []string                   `json:"narrative_fields,omitempty"`
	OmittedFields   []string                   `json:"omitted_fields,omitempty"`
}

type normalizedChatBundle struct {
	RecordProfile            string   `json:"record_profile,omitempty"`
	Name                     string   `json:"name,omitempty"`
	Aliases                  []string `json:"aliases,omitempty"`
	StudentNumber            string   `json:"student_number,omitempty"`
	Community                string   `json:"community,omitempty"`
	School                   string   `json:"school,omitempty"`
	DeceasedStatus           string   `json:"deceased_status,omitempty"`
	DateOfBirth              string   `json:"date_of_birth,omitempty"`
	Admitted                 string   `json:"admitted,omitempty"`
	Discharged               string   `json:"discharged,omitempty"`
	ParentsNames             string   `json:"parents_names,omitempty"`
	MappingLocation          string   `json:"mapping_location,omitempty"`
	HasNotes                 bool     `json:"has_notes,omitempty"`
	HasAdditionalInformation bool     `json:"has_additional_information,omitempty"`
	HasDeathDetails          bool     `json:"has_death_details,omitempty"`
	HasPhotos                bool     `json:"has_photos,omitempty"`
}

type normalizedNarrativeBundle struct {
	Notes                 string `json:"notes,omitempty"`
	AdditionalInformation string `json:"additional_information,omitempty"`
	DeathDetails          string `json:"death_details,omitempty"`
}

type rowFieldLookup struct {
	values  map[string]string
	sources map[string]string
}

func buildNormalizedCanonicalRow(ctx normalizationContext, raw map[string]interface{}, payload normalizedRowPayload) *normalizedCanonicalRow {
	lookup := buildNormalizedFieldLookup(raw)

	firstNamesRaw, firstNameSource := lookupConceptValue(lookup, ctx.SchemaHints, "first_name")
	middleNames, middleNamesSource := lookupConceptValue(lookup, ctx.SchemaHints, "middle_names")
	lastNames, lastNamesSource := lookupConceptValue(lookup, ctx.SchemaHints, "last_name")
	indigenousName, indigenousNameSource := lookupConceptValue(lookup, ctx.SchemaHints, "indigenous_name")
	explicitName, explicitNameSource := lookupConceptValue(lookup, ctx.SchemaHints, "display_name")

	displayName := joinDisplayParts(primaryDisplayName(firstNamesRaw), middleNames, lastNames)
	displayNameSource := combineSourceLabels(firstNameSource, middleNamesSource, lastNamesSource)
	if displayName == "" {
		displayName = cleanDisplayValue(explicitName)
		displayNameSource = explicitNameSource
	}
	if displayName == "" && len(payload.Names) > 0 {
		displayName = payload.Names[0]
		displayNameSource = "inferred name field"
	}

	community, communitySource := lookupConceptValue(lookup, ctx.SchemaHints, "community")
	school, schoolSource := lookupConceptValue(lookup, ctx.SchemaHints, "school")
	studentNumberRaw, studentNumberSource := lookupConceptValue(lookup, ctx.SchemaHints, "identifier")
	deceasedRaw, deceasedSource := lookupConceptValue(lookup, ctx.SchemaHints, "deceased_status")
	parentsNames, parentsSource := lookupConceptValue(lookup, ctx.SchemaHints, "parents_names")
	mappingLocation, mappingLocationSource := lookupConceptValue(lookup, ctx.SchemaHints, "mapping_location")
	lat, latSource := lookupConceptValue(lookup, ctx.SchemaHints, "lat")
	lng, lngSource := lookupConceptValue(lookup, ctx.SchemaHints, "lng")
	birthRaw, birthSource := lookupConceptValue(lookup, ctx.SchemaHints, "birth_date")
	admittedRaw, admittedSource := lookupConceptValue(lookup, ctx.SchemaHints, "admitted_date")
	dischargedRaw, dischargedSource := lookupConceptValue(lookup, ctx.SchemaHints, "discharged_date")

	birth := buildNormalizedCanonicalDate(birthRaw)
	admitted := buildNormalizedCanonicalDate(admittedRaw)
	discharged := buildNormalizedCanonicalDate(dischargedRaw)

	canonical := &normalizedCanonicalRow{
		DisplayName:      displayName,
		FirstName:        primaryDisplayName(firstNamesRaw),
		MiddleNames:      cleanDisplayValue(middleNames),
		LastName:         cleanDisplayValue(lastNames),
		IndigenousName:   cleanDisplayValue(indigenousName),
		NameAliases:      extractNameAliases(firstNamesRaw, indigenousName),
		StudentNumber:    normalizeIdentifierValue(studentNumberRaw),
		StudentNumberRaw: cleanDisplayValue(studentNumberRaw),
		Community:        cleanDisplayValue(community),
		School:           cleanDisplayValue(school),
		DeceasedStatus:   normalizeDeceasedStatus(deceasedRaw),
		ParentsNames:     cleanDisplayValue(parentsNames),
		MappingLocation:  cleanDisplayValue(mappingLocation),
		Lat:              cleanDisplayValue(lat),
		Lng:              cleanDisplayValue(lng),
		Dates: normalizedCanonicalDates{
			Birth:      birth,
			Admitted:   admitted,
			Discharged: discharged,
		},
	}

	if !hasCanonicalRowContent(canonical) {
		return nil
	}

	canonical.RecordProfile = detectRecordProfile(canonical)
	canonical.DerivedFrom = map[string]string{}
	setDerivedField(canonical.DerivedFrom, "display_name", displayNameSource)
	setDerivedField(canonical.DerivedFrom, "first_name", firstNameSource)
	setDerivedField(canonical.DerivedFrom, "middle_names", middleNamesSource)
	setDerivedField(canonical.DerivedFrom, "last_name", lastNamesSource)
	setDerivedField(canonical.DerivedFrom, "indigenous_name", indigenousNameSource)
	setDerivedField(canonical.DerivedFrom, "student_number", studentNumberSource)
	setDerivedField(canonical.DerivedFrom, "community", communitySource)
	setDerivedField(canonical.DerivedFrom, "school", schoolSource)
	setDerivedField(canonical.DerivedFrom, "deceased_status", deceasedSource)
	setDerivedField(canonical.DerivedFrom, "parents_names", parentsSource)
	setDerivedField(canonical.DerivedFrom, "mapping_location", mappingLocationSource)
	setDerivedField(canonical.DerivedFrom, "lat", latSource)
	setDerivedField(canonical.DerivedFrom, "lng", lngSource)
	if birth != nil {
		setDerivedField(canonical.DerivedFrom, "dates.birth", birthSource)
	}
	if admitted != nil {
		setDerivedField(canonical.DerivedFrom, "dates.admitted", admittedSource)
	}
	if discharged != nil {
		setDerivedField(canonical.DerivedFrom, "dates.discharged", dischargedSource)
	}
	if len(canonical.DerivedFrom) == 0 {
		canonical.DerivedFrom = nil
	}

	return canonical
}

func buildNormalizedChatReadyRow(ctx normalizationContext, canonical *normalizedCanonicalRow, raw map[string]interface{}) *normalizedChatReadyRow {
	lookup := buildNormalizedFieldLookup(raw)

	notes, _ := lookupConceptValue(lookup, ctx.SchemaHints, "notes")
	additionalInformation, _ := lookupConceptValue(lookup, ctx.SchemaHints, "additional_information")
	deathDetails, _ := lookupConceptValue(lookup, ctx.SchemaHints, "death_details")
	photos, _ := lookupConceptValue(lookup, ctx.SchemaHints, "photos")

	notes = cleanNarrativeValue(notes)
	additionalInformation = cleanNarrativeValue(additionalInformation)
	deathDetails = cleanNarrativeValue(deathDetails)
	photos = cleanDisplayValue(photos)

	narrativeFields := make([]string, 0, 3)
	omittedFields := make([]string, 0, 6)
	if notes != "" {
		narrativeFields = append(narrativeFields, "notes")
		omittedFields = append(omittedFields, "notes")
	}
	if additionalInformation != "" {
		narrativeFields = append(narrativeFields, "additional_information")
		omittedFields = append(omittedFields, "additional_information")
	}
	if deathDetails != "" {
		narrativeFields = append(narrativeFields, "death_details")
		omittedFields = append(omittedFields, "death_details")
	}
	if canonical != nil {
		if canonical.Lat != "" {
			omittedFields = append(omittedFields, "lat")
		}
		if canonical.Lng != "" {
			omittedFields = append(omittedFields, "lng")
		}
	}
	if photos != "" {
		omittedFields = append(omittedFields, "photos")
	}

	var defaultBundle *normalizedChatBundle
	if canonical != nil {
		defaultBundle = &normalizedChatBundle{
			RecordProfile:            canonical.RecordProfile,
			Name:                     canonical.DisplayName,
			Aliases:                  append([]string(nil), canonical.NameAliases...),
			StudentNumber:            firstNonEmpty(canonical.StudentNumberRaw, canonical.StudentNumber),
			Community:                canonical.Community,
			School:                   canonical.School,
			DeceasedStatus:           canonical.DeceasedStatus,
			DateOfBirth:              displayCanonicalDate(canonical.Dates.Birth),
			Admitted:                 displayCanonicalDate(canonical.Dates.Admitted),
			Discharged:               displayCanonicalDate(canonical.Dates.Discharged),
			ParentsNames:             canonical.ParentsNames,
			MappingLocation:          canonical.MappingLocation,
			HasNotes:                 notes != "",
			HasAdditionalInformation: additionalInformation != "",
			HasDeathDetails:          deathDetails != "",
			HasPhotos:                photos != "",
		}
		if isEmptyNormalizedChatBundle(defaultBundle) {
			defaultBundle = nil
		}
	}

	var narrativeBundle *normalizedNarrativeBundle
	if notes != "" || additionalInformation != "" || deathDetails != "" {
		narrativeBundle = &normalizedNarrativeBundle{
			Notes:                 notes,
			AdditionalInformation: additionalInformation,
			DeathDetails:          deathDetails,
		}
	}

	if defaultBundle == nil && narrativeBundle == nil {
		return nil
	}

	return &normalizedChatReadyRow{
		DefaultBundle:   defaultBundle,
		NarrativeBundle: narrativeBundle,
		NarrativeFields: uniqueStrings(narrativeFields),
		OmittedFields:   uniqueStrings(omittedFields),
	}
}

func searchTokensForCanonical(canonical *normalizedCanonicalRow) []string {
	if canonical == nil {
		return nil
	}

	values := []string{
		canonical.DisplayName,
		canonical.FirstName,
		canonical.MiddleNames,
		canonical.LastName,
		canonical.IndigenousName,
		canonical.StudentNumber,
		canonical.StudentNumberRaw,
		canonical.Community,
		canonical.School,
		canonical.ParentsNames,
		canonical.MappingLocation,
	}
	for _, alias := range canonical.NameAliases {
		values = append(values, alias)
	}

	tokens := make([]string, 0, len(values)*2)
	for _, value := range values {
		tokens = append(tokens, tokenizeSearchValue(value)...)
	}
	return uniqueStrings(tokens)
}

func buildNormalizedFieldLookup(raw map[string]interface{}) rowFieldLookup {
	fieldNames := make([]string, 0, len(raw))
	for fieldName := range raw {
		fieldNames = append(fieldNames, fieldName)
	}
	sort.Strings(fieldNames)

	lookup := rowFieldLookup{
		values:  make(map[string]string, len(raw)),
		sources: make(map[string]string, len(raw)),
	}
	for _, fieldName := range fieldNames {
		normalizedFieldName := normalizeSearchValue(fieldName)
		if normalizedFieldName == "" {
			continue
		}
		value := stringifyRowValue(raw[fieldName])
		if existing, ok := lookup.values[normalizedFieldName]; ok && strings.TrimSpace(existing) != "" {
			continue
		}
		lookup.values[normalizedFieldName] = value
		lookup.sources[normalizedFieldName] = fieldName
	}
	return lookup
}

func lookupConceptValue(lookup rowFieldLookup, hints *normalizationSchemaHints, concept string) (string, string) {
	for _, alias := range hintedConceptFields(hints, concept) {
		key := normalizeSearchValue(alias)
		value := strings.TrimSpace(lookup.values[key])
		if value == "" {
			continue
		}
		source := lookup.sources[key]
		if strings.TrimSpace(source) == "" {
			source = alias
		}
		return value, source
	}

	for _, alias := range canonicalFieldAliases[concept] {
		key := normalizeSearchValue(alias)
		value := strings.TrimSpace(lookup.values[key])
		if value == "" {
			continue
		}
		source := lookup.sources[key]
		if strings.TrimSpace(source) == "" {
			source = alias
		}
		return value, source
	}
	return "", ""
}

func buildNormalizedCanonicalDate(raw string) *normalizedCanonicalDate {
	raw = cleanDisplayValue(raw)
	if raw == "" {
		return nil
	}

	hint := inferDateHint(raw)
	date := &normalizedCanonicalDate{Raw: raw}
	if hint != nil {
		date.Kind = hint.Kind
		date.Approximate = hint.Approximate
		if hint.LowerYear != nil {
			lowerYear := *hint.LowerYear
			date.Year = &lowerYear
		}
		if hint.UpperYear != nil && (hint.LowerYear == nil || *hint.UpperYear != *hint.LowerYear) {
			upperYear := *hint.UpperYear
			date.UpperYear = &upperYear
		}
	}

	if parsed, err := tryParseDate(raw); err == nil {
		date.ISO = parsed.Format("2006-01-02")
		year := parsed.Year()
		date.Year = &year
		date.UpperYear = nil
		if date.Kind == "" {
			date.Kind = "exact_date"
		}
	}

	return date
}

func displayCanonicalDate(date *normalizedCanonicalDate) string {
	if date == nil {
		return ""
	}
	return strings.TrimSpace(date.Raw)
}

func detectRecordProfile(canonical *normalizedCanonicalRow) string {
	if canonical == nil {
		return ""
	}

	personSignals := 0
	if strings.TrimSpace(canonical.DisplayName) != "" ||
		strings.TrimSpace(canonical.FirstName) != "" ||
		strings.TrimSpace(canonical.LastName) != "" ||
		len(canonical.NameAliases) > 0 {
		personSignals++
	}
	if strings.TrimSpace(canonical.StudentNumber) != "" || strings.TrimSpace(canonical.StudentNumberRaw) != "" {
		personSignals++
	}
	if canonical.Dates.Birth != nil || canonical.Dates.Admitted != nil || canonical.Dates.Discharged != nil ||
		strings.TrimSpace(canonical.ParentsNames) != "" || strings.TrimSpace(canonical.DeceasedStatus) != "" {
		personSignals++
	}
	if personSignals > 0 {
		return recordProfilePersonRegistry
	}
	return recordProfileTabularRecord
}

func normalizeDeceasedStatus(raw string) string {
	switch normalizeSearchValue(raw) {
	case "", "blank":
		return ""
	case "yes", "y", "true", "t", "1", "deceased":
		return "yes"
	case "no", "n", "false", "f", "0", "living":
		return "no"
	case "unknown", "unk", "not known":
		return "unknown"
	default:
		return ""
	}
}

func normalizeIdentifierValue(value string) string {
	value = strings.TrimSpace(strings.ToUpper(value))
	if value == "" {
		return ""
	}

	var b strings.Builder
	lastDash := false
	for _, r := range value {
		switch {
		case (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
			lastDash = false
		case lastDash:
			continue
		default:
			b.WriteByte('-')
			lastDash = true
		}
	}

	return strings.Trim(b.String(), "-")
}

func extractNameAliases(firstNamesRaw string, indigenousName string) []string {
	aliases := make([]string, 0, 4)
	firstNamesRaw = cleanDisplayValue(firstNamesRaw)
	indigenousName = cleanDisplayValue(indigenousName)

	primaryName := primaryDisplayName(firstNamesRaw)
	for _, part := range splitNameVariants(firstNamesRaw) {
		if normalized := normalizeSearchValue(part); normalized == "" || normalized == normalizeSearchValue(primaryName) {
			continue
		}
		aliases = append(aliases, part)
	}

	if indigenousName != "" && normalizeSearchValue(indigenousName) != normalizeSearchValue(primaryName) {
		aliases = append(aliases, indigenousName)
	}

	return uniqueStrings(aliases)
}

func splitNameVariants(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}

	var out []string
	if slashParts := strings.Split(value, "/"); len(slashParts) > 1 {
		for _, part := range slashParts {
			out = append(out, cleanDisplayValue(removeParenthetical(part)))
		}
	}

	start := strings.IndexByte(value, '(')
	end := strings.IndexByte(value, ')')
	if start >= 0 && end > start {
		alias := cleanDisplayValue(value[start+1 : end])
		if alias != "" {
			out = append(out, alias)
		}
	}

	return uniqueStrings(out)
}

func primaryDisplayName(value string) string {
	value = cleanDisplayValue(value)
	if value == "" {
		return ""
	}
	value = removeParenthetical(value)
	if slashIndex := strings.IndexByte(value, '/'); slashIndex >= 0 {
		value = value[:slashIndex]
	}
	return cleanDisplayValue(value)
}

func removeParenthetical(value string) string {
	start := strings.IndexByte(value, '(')
	end := strings.IndexByte(value, ')')
	if start >= 0 && end > start {
		value = strings.TrimSpace(value[:start] + " " + value[end+1:])
	}
	return collapseWhitespacePreserveCase(value)
}

func joinDisplayParts(parts ...string) string {
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		part = cleanDisplayValue(part)
		if part == "" {
			continue
		}
		cleaned = append(cleaned, part)
	}
	return strings.Join(cleaned, " ")
}

func cleanDisplayValue(value string) string {
	value = collapseWhitespacePreserveCase(value)
	return trimTerminalPunctuation(value)
}

func cleanNarrativeValue(value string) string {
	return collapseWhitespacePreserveCase(value)
}

func collapseWhitespacePreserveCase(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return normalizationWhitespaceRe.ReplaceAllString(value, " ")
}

func trimTerminalPunctuation(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimRight(value, " \t\r\n.,;:")
	return strings.TrimSpace(value)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func combineSourceLabels(labels ...string) string {
	cleaned := make([]string, 0, len(labels))
	seen := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if label == "" {
			continue
		}
		if _, exists := seen[label]; exists {
			continue
		}
		seen[label] = struct{}{}
		cleaned = append(cleaned, label)
	}
	return strings.Join(cleaned, " + ")
}

func setDerivedField(derived map[string]string, key string, source string) {
	source = strings.TrimSpace(source)
	if source == "" {
		return
	}
	derived[key] = source
}

func hasCanonicalRowContent(row *normalizedCanonicalRow) bool {
	if row == nil {
		return false
	}
	return strings.TrimSpace(row.DisplayName) != "" ||
		strings.TrimSpace(row.StudentNumber) != "" ||
		strings.TrimSpace(row.StudentNumberRaw) != "" ||
		strings.TrimSpace(row.Community) != "" ||
		strings.TrimSpace(row.School) != "" ||
		strings.TrimSpace(row.DeceasedStatus) != "" ||
		strings.TrimSpace(row.ParentsNames) != "" ||
		strings.TrimSpace(row.MappingLocation) != "" ||
		row.Dates.Birth != nil ||
		row.Dates.Admitted != nil ||
		row.Dates.Discharged != nil
}

func isEmptyNormalizedChatBundle(bundle *normalizedChatBundle) bool {
	if bundle == nil {
		return true
	}
	return strings.TrimSpace(bundle.RecordProfile) == "" &&
		strings.TrimSpace(bundle.Name) == "" &&
		len(bundle.Aliases) == 0 &&
		strings.TrimSpace(bundle.StudentNumber) == "" &&
		strings.TrimSpace(bundle.Community) == "" &&
		strings.TrimSpace(bundle.School) == "" &&
		strings.TrimSpace(bundle.DeceasedStatus) == "" &&
		strings.TrimSpace(bundle.DateOfBirth) == "" &&
		strings.TrimSpace(bundle.Admitted) == "" &&
		strings.TrimSpace(bundle.Discharged) == "" &&
		strings.TrimSpace(bundle.ParentsNames) == "" &&
		strings.TrimSpace(bundle.MappingLocation) == "" &&
		!bundle.HasNotes &&
		!bundle.HasAdditionalInformation &&
		!bundle.HasDeathDetails &&
		!bundle.HasPhotos
}
