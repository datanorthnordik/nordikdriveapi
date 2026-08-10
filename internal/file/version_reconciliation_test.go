package file

import "testing"

func TestBuildVersionReconciliationPlan_ExactNameFallbacks(t *testing.T) {
	config := &versionReconciliationConfig{
		FieldHeaders: map[string]string{
			"firstname": "STUDENT NAME",
			"lastname":  "LAST NAME",
		},
		Weights:   map[string]float64{},
		Threshold: 45,
		Margin:    8,
	}

	makeRow := func(id uint, values map[string]string) versionRowDescriptor {
		return versionRowDescriptor{
			storedVersionRow: storedVersionRow{ID: id},
			RowDataMap:       values,
			ApprovedUpdates:  map[string]string{},
		}
	}

	t.Run("matches a unique single configured name when last name is absent", func(t *testing.T) {
		source := []versionRowDescriptor{makeRow(1, map[string]string{"STUDENT NAME": "Alexander KNAGGS"})}
		target := []versionRowDescriptor{makeRow(2, map[string]string{"STUDENT NAME": "Alexander KNAGGS"})}

		links, carryForward := buildVersionReconciliationPlan(source, target, config)
		if len(carryForward) != 0 {
			t.Fatalf("unexpected carry-forward rows: %#v", carryForward)
		}
		if len(links) != 1 {
			t.Fatalf("expected one link, got %#v", links)
		}
		if links[0].SourceID != 1 || links[0].TargetID != 2 || links[0].Method != "exact_firstname_only" {
			t.Fatalf("unexpected single-name link: %#v", links[0])
		}
	})

	t.Run("continues to match separate first and last names", func(t *testing.T) {
		source := []versionRowDescriptor{makeRow(3, map[string]string{"STUDENT NAME": "Alexander", "LAST NAME": "KNAGGS"})}
		target := []versionRowDescriptor{makeRow(4, map[string]string{"STUDENT NAME": "Alexander", "LAST NAME": "KNAGGS"})}

		links, _ := buildVersionReconciliationPlan(source, target, config)
		if len(links) != 1 {
			t.Fatalf("expected one link, got %#v", links)
		}
		if links[0].SourceID != 3 || links[0].TargetID != 4 || links[0].Method != "exact_name" {
			t.Fatalf("unexpected separate-name link: %#v", links[0])
		}
	})

	t.Run("does not auto-match duplicate single names", func(t *testing.T) {
		source := []versionRowDescriptor{
			makeRow(5, map[string]string{"STUDENT NAME": "Alexander KNAGGS"}),
			makeRow(6, map[string]string{"STUDENT NAME": "Alexander KNAGGS"}),
		}
		target := []versionRowDescriptor{
			makeRow(7, map[string]string{"STUDENT NAME": "Alexander KNAGGS"}),
			makeRow(8, map[string]string{"STUDENT NAME": "Alexander KNAGGS"}),
		}

		links, _ := buildVersionReconciliationPlan(source, target, config)
		if len(links) != 0 {
			t.Fatalf("expected duplicate single names to remain unmatched, got %#v", links)
		}
	})
}
