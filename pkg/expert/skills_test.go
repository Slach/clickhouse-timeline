package expert

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSkillMD(t *testing.T) {
	tests := []struct {
		name        string
		content     string
		wantDisplay string
		wantDesc    string
	}{
		{
			name: "standard skill",
			content: `# Memory Analysis

Analyze ClickHouse memory usage patterns and identify potential memory issues.

## Instructions

Check system.asynchronous_metrics for memory metrics.`,
			wantDisplay: "Memory Analysis",
			wantDesc:    "Analyze ClickHouse memory usage patterns and identify potential memory issues.",
		},
		{
			name: "no heading",
			content: `Some content without a heading.

More content here.`,
			wantDisplay: "test-skill",
			wantDesc:    "",
		},
		{
			name: "heading with blank lines before description",
			content: `# Merge Diagnostics


Diagnose merge performance issues
in ClickHouse MergeTree tables.

## Details`,
			wantDisplay: "Merge Diagnostics",
			wantDesc:    "Diagnose merge performance issues in ClickHouse MergeTree tables.",
		},
		{
			name:        "empty content",
			content:     "",
			wantDisplay: "empty-skill",
			wantDesc:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := tt.name
			if tt.name == "no heading" {
				name = "test-skill"
			} else if tt.name == "empty content" {
				name = "empty-skill"
			}

			skill := parseSkillMD(name, "/tmp/"+name, tt.content)
			assert.Equal(t, tt.wantDisplay, skill.DisplayName)
			assert.Equal(t, tt.wantDesc, skill.Description)
			assert.Equal(t, tt.content, skill.Content)
		})
	}
}

func TestSkillNames(t *testing.T) {
	skills := []Skill{
		{Name: "merges"},
		{Name: "memory"},
		{Name: "replication"},
	}

	names := SkillNames(skills)
	assert.Equal(t, []string{"merges", "memory", "replication"}, names)
}

func TestFindSkillByName(t *testing.T) {
	skills := []Skill{
		{Name: "merges", DisplayName: "Merge Diagnostics"},
		{Name: "memory", DisplayName: "Memory Analysis"},
	}

	found := FindSkillByName(skills, "memory")
	assert.NotNil(t, found)
	if found != nil {
		assert.Equal(t, "Memory Analysis", found.DisplayName)
	}

	notFound := FindSkillByName(skills, "nonexistent")
	assert.Nil(t, notFound)
}
