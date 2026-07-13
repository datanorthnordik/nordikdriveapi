package chat

import "encoding/json"

type chatPromptProjectionProfile struct {
	Mode                string
	IncludeAllDefault   bool
	IncludeAllNarrative bool
	DefaultFields       map[string]struct{}
	NarrativeFields     map[string]struct{}
}

func buildChatPromptProjectionProfile(question string, retrievalMode string, selectedRows int, includeNarrative bool) chatPromptProjectionProfile {
	profile := buildChatQuestionProfile(question)
	if includeNarrative || profile.LooksLikeEntity || profile.WantsNarrative || selectedRows <= 5 {
		return chatPromptProjectionProfile{
			Mode:                "full",
			IncludeAllDefault:   true,
			IncludeAllNarrative: true,
		}
	}
	if retrievalMode == "compact_dataset" && selectedRows <= 8 {
		return chatPromptProjectionProfile{
			Mode:                "full",
			IncludeAllDefault:   true,
			IncludeAllNarrative: true,
		}
	}

	defaultFields := map[string]struct{}{
		"name": {},
	}

	for _, field := range detectRelevantDefaultFields(profile.NormalizedQuestion, selectedRows) {
		defaultFields[field] = struct{}{}
	}

	return chatPromptProjectionProfile{
		Mode:              "relevant_only",
		DefaultFields:     defaultFields,
		NarrativeFields:   map[string]struct{}{},
		IncludeAllDefault: false,
	}
}

func detectRelevantDefaultFields(normalizedQuestion string, selectedRows int) []string {
	fields := make([]string, 0, 8)

	if selectedRows > 1 {
		fields = append(fields, "community", "school")
	}

	if questionMentionsAny(normalizedQuestion, "student number", "student id", "id number", "record number", "registration number", "admission number", "identifier") {
		fields = append(fields, "student_number")
	}
	if questionMentionsAny(normalizedQuestion, "community", "first nation", "reserve", "band") || containsStructuredToken(normalizedQuestion, "from") {
		fields = append(fields, "community")
	}
	if questionMentionsAny(normalizedQuestion, "school", "residential school", "institution") {
		fields = append(fields, "school")
	}
	if questionMentionsAny(normalizedQuestion, "deceased", "died", "dead", "alive", "living", "death") {
		fields = append(fields, "deceased_status")
	}
	if questionMentionsAny(normalizedQuestion, "date of birth", "birth date", "dob", "born") {
		fields = append(fields, "date_of_birth")
	}
	if questionMentionsAny(normalizedQuestion, "admitted", "admission date", "date admitted") {
		fields = append(fields, "admitted")
	}
	if questionMentionsAny(normalizedQuestion, "discharged", "discharge date", "date discharged") {
		fields = append(fields, "discharged")
	}
	if questionMentionsAny(normalizedQuestion, "parents", "parent", "mother", "father", "family") {
		fields = append(fields, "parents_names")
	}
	if questionMentionsAny(normalizedQuestion, "location", "mapping location", "map location", "place", "town", "city") {
		fields = append(fields, "mapping_location")
	}

	return uniqueChatTokens(fields)
}

func marshalProjectedDefaultBundle(bundle structuredChatDefaultBundle, projection chatPromptProjectionProfile) (string, error) {
	if projection.IncludeAllDefault {
		out, err := json.Marshal(defaultBundleMap(bundle))
		if err != nil {
			return "", err
		}
		return string(out), nil
	}

	projected := map[string]any{}
	if _, ok := projection.DefaultFields["name"]; ok && bundle.Name != "" {
		projected["name"] = bundle.Name
	}
	if _, ok := projection.DefaultFields["aliases"]; ok && len(bundle.Aliases) > 0 {
		projected["aliases"] = append([]string(nil), bundle.Aliases...)
	}
	if _, ok := projection.DefaultFields["student_number"]; ok && bundle.StudentNumber != "" {
		projected["student_number"] = bundle.StudentNumber
	}
	if _, ok := projection.DefaultFields["community"]; ok && bundle.Community != "" {
		projected["community"] = bundle.Community
	}
	if _, ok := projection.DefaultFields["school"]; ok && bundle.School != "" {
		projected["school"] = bundle.School
	}
	if _, ok := projection.DefaultFields["deceased_status"]; ok && bundle.DeceasedStatus != "" {
		projected["deceased_status"] = bundle.DeceasedStatus
	}
	if _, ok := projection.DefaultFields["date_of_birth"]; ok && bundle.DateOfBirth != "" {
		projected["date_of_birth"] = bundle.DateOfBirth
	}
	if _, ok := projection.DefaultFields["admitted"]; ok && bundle.Admitted != "" {
		projected["admitted"] = bundle.Admitted
	}
	if _, ok := projection.DefaultFields["discharged"]; ok && bundle.Discharged != "" {
		projected["discharged"] = bundle.Discharged
	}
	if _, ok := projection.DefaultFields["parents_names"]; ok && bundle.ParentsNames != "" {
		projected["parents_names"] = bundle.ParentsNames
	}
	if _, ok := projection.DefaultFields["mapping_location"]; ok && bundle.MappingLocation != "" {
		projected["mapping_location"] = bundle.MappingLocation
	}
	if bundle.HasNotes {
		projected["has_notes"] = true
	}
	if bundle.HasAdditionalInformation {
		projected["has_additional_information"] = true
	}
	if bundle.HasDeathDetails {
		projected["has_death_details"] = true
	}
	if bundle.HasPhotos {
		projected["has_photos"] = true
	}

	out, err := json.Marshal(projected)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func marshalProjectedNarrativeBundle(bundle structuredChatNarrativeBundle, projection chatPromptProjectionProfile) (string, error) {
	if projection.IncludeAllNarrative {
		out, err := json.Marshal(narrativeBundleMap(bundle))
		if err != nil {
			return "", err
		}
		return string(out), nil
	}

	projected := map[string]any{}
	if _, ok := projection.NarrativeFields["notes"]; ok && bundle.Notes != "" {
		projected["notes"] = bundle.Notes
	}
	if _, ok := projection.NarrativeFields["additional_information"]; ok && bundle.AdditionalInformation != "" {
		projected["additional_information"] = bundle.AdditionalInformation
	}
	if _, ok := projection.NarrativeFields["death_details"]; ok && bundle.DeathDetails != "" {
		projected["death_details"] = bundle.DeathDetails
	}

	out, err := json.Marshal(projected)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func defaultBundleMap(bundle structuredChatDefaultBundle) map[string]any {
	out := map[string]any{}
	if bundle.RecordProfile != "" {
		out["record_profile"] = bundle.RecordProfile
	}
	if bundle.Name != "" {
		out["name"] = bundle.Name
	}
	if len(bundle.Aliases) > 0 {
		out["aliases"] = append([]string(nil), bundle.Aliases...)
	}
	if bundle.StudentNumber != "" {
		out["student_number"] = bundle.StudentNumber
	}
	if bundle.Community != "" {
		out["community"] = bundle.Community
	}
	if bundle.School != "" {
		out["school"] = bundle.School
	}
	if bundle.DeceasedStatus != "" {
		out["deceased_status"] = bundle.DeceasedStatus
	}
	if bundle.DateOfBirth != "" {
		out["date_of_birth"] = bundle.DateOfBirth
	}
	if bundle.Admitted != "" {
		out["admitted"] = bundle.Admitted
	}
	if bundle.Discharged != "" {
		out["discharged"] = bundle.Discharged
	}
	if bundle.ParentsNames != "" {
		out["parents_names"] = bundle.ParentsNames
	}
	if bundle.MappingLocation != "" {
		out["mapping_location"] = bundle.MappingLocation
	}
	if bundle.HasNotes {
		out["has_notes"] = true
	}
	if bundle.HasAdditionalInformation {
		out["has_additional_information"] = true
	}
	if bundle.HasDeathDetails {
		out["has_death_details"] = true
	}
	if bundle.HasPhotos {
		out["has_photos"] = true
	}
	return out
}

func narrativeBundleMap(bundle structuredChatNarrativeBundle) map[string]any {
	out := map[string]any{}
	if bundle.Notes != "" {
		out["notes"] = bundle.Notes
	}
	if bundle.AdditionalInformation != "" {
		out["additional_information"] = bundle.AdditionalInformation
	}
	if bundle.DeathDetails != "" {
		out["death_details"] = bundle.DeathDetails
	}
	return out
}

func questionMentionsAny(normalizedQuestion string, phrases ...string) bool {
	for _, phrase := range phrases {
		if containsStructuredTokenSequence(normalizedQuestion, normalizeChatSearchValue(phrase)) {
			return true
		}
	}
	return false
}
