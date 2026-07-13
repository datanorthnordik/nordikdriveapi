package dataconfig

import (
	"encoding/json"
	"testing"

	"gorm.io/datatypes"
)

func TestConfigForResponse_LeavesLegacyConfigUnchanged(t *testing.T) {
	raw := datatypes.JSON([]byte(`{"name":"athul","enabled":true}`))

	got := configForResponse(raw)

	assertJSONEqual(t, got, raw)
}

func TestConfigForResponse_FiltersAdditionalFieldsFromCurrentConfig(t *testing.T) {
	raw := datatypes.JSON([]byte(`{
		"fields": [
			{"key":"name","label":"Name","additional_field":false},
			{"key":"nia_comments","label":"NIA comments","additional_field":true},
			{"key":"school","label":"School"}
		]
	}`))

	got := configForResponse(raw)

	want := datatypes.JSON([]byte(`{
		"fields": [
			{"key":"name","label":"Name","additional_field":false},
			{"key":"school","label":"School"}
		]
	}`))
	assertJSONEqual(t, got, want)
}

func TestConfigForResponse_PrefersSourceFileDataConfigAndFiltersAdditionalFields(t *testing.T) {
	raw := datatypes.JSON([]byte(`{
		"fields": [
			{"key":"derived","additional_field":true}
		],
		"source_file": {
			"data_config": {
				"fields": [
					{"key":"student_name","label":"Student Name","additional_field":false},
					{"key":"nia_comments","label":"NIA comments","additional_field":true},
					{"key":"school","label":"School","additional_field":"false"}
				]
			}
		}
	}`))

	got := configForResponse(raw)

	want := datatypes.JSON([]byte(`{
		"fields": [
			{"key":"student_name","label":"Student Name","additional_field":false},
			{"key":"school","label":"School","additional_field":"false"}
		]
	}`))
	assertJSONEqual(t, got, want)
}

func TestConfigForResponse_PrefersSourceFileFieldsWhenSourceConfigIsInline(t *testing.T) {
	raw := datatypes.JSON([]byte(`{
		"source_file": {
			"fields": [
				{"key":"id","additional_field":true},
				{"key":"community","additional_field":false}
			]
		},
		"fields": [
			{"key":"fallback","additional_field":false}
		]
	}`))

	got := configForResponse(raw)

	want := datatypes.JSON([]byte(`{
		"fields": [
			{"key":"community","additional_field":false}
		]
	}`))
	assertJSONEqual(t, got, want)
}

func TestConfigForResponse_SupportsCamelCaseConfigKeys(t *testing.T) {
	raw := datatypes.JSON([]byte(`{
		"sourceFile": {
			"dataConfig": {
				"fields": [
					{"key":"id","additionalField":true},
					{"key":"community","additionalField":false}
				]
			}
		}
	}`))

	got := configForResponse(raw)

	want := datatypes.JSON([]byte(`{
		"fields": [
			{"key":"community","additionalField":false}
		]
	}`))
	assertJSONEqual(t, got, want)
}

func assertJSONEqual(t *testing.T, got datatypes.JSON, want datatypes.JSON) {
	t.Helper()

	var gotValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("unmarshal got: %v", err)
	}

	var wantValue any
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}

	gotJSON, err := json.Marshal(gotValue)
	if err != nil {
		t.Fatalf("marshal got: %v", err)
	}

	wantJSON, err := json.Marshal(wantValue)
	if err != nil {
		t.Fatalf("marshal want: %v", err)
	}

	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("got %s want %s", gotJSON, wantJSON)
	}
}
