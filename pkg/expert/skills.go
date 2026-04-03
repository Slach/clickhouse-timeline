package expert

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/rs/zerolog/log"
)

const (
	DefaultSkillsRepo = "https://github.com/Altinity/Skills"
	skillsSubDir      = "altinity-expert-clickhouse/skills"
	skillFileName     = "SKILL.md"
)

// Skill represents a loaded expert skill from the Skills repository.
type Skill struct {
	Name        string // directory name (e.g. "merges")
	DisplayName string // parsed from # heading in SKILL.md
	Description string // first paragraph after heading
	Content     string // full SKILL.md content
	Path        string // absolute path to skill directory
}

// SkillsDir returns the path to the skills directory.
func SkillsDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".", ".clickhouse-timeline", "skills")
	}
	return filepath.Join(home, ".clickhouse-timeline", "skills")
}

// CloneOrUpdateSkills clones the skills repo if not present, or pulls updates.
func CloneOrUpdateSkills(ctx context.Context, repoURL string) error {
	if repoURL == "" {
		repoURL = DefaultSkillsRepo
	}
	dir := SkillsDir()

	// Check if already cloned
	if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
		return pullSkills(ctx, dir)
	}

	// Clone with shallow depth
	log.Info().Str("repo", repoURL).Str("dir", dir).Msg("Cloning skills repository")
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return fmt.Errorf("create skills parent dir: %w", err)
	}

	_, err := git.PlainCloneContext(ctx, dir, false, &git.CloneOptions{
		URL:           repoURL,
		Depth:         1,
		SingleBranch:  true,
		ReferenceName: plumbing.HEAD,
	})
	if err != nil {
		return fmt.Errorf("clone skills repo: %w", err)
	}

	log.Info().Msg("Skills repository cloned successfully")
	return nil
}

func pullSkills(ctx context.Context, dir string) error {
	log.Info().Str("dir", dir).Msg("Updating skills repository")

	repo, err := git.PlainOpen(dir)
	if err != nil {
		return fmt.Errorf("open skills repo: %w", err)
	}

	wt, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("get worktree: %w", err)
	}

	err = wt.PullContext(ctx, &git.PullOptions{
		RemoteName: "origin",
		Force:      true,
	})
	if err == git.NoErrAlreadyUpToDate {
		log.Info().Msg("Skills already up to date")
		return nil
	}
	if err != nil {
		// go-git has issues pulling shallow repos; re-clone as fallback
		log.Warn().Err(err).Msg("Pull failed, re-cloning skills repository")
		if removeErr := os.RemoveAll(dir); removeErr != nil {
			return fmt.Errorf("remove skills dir for re-clone: %w", removeErr)
		}
		return CloneOrUpdateSkills(ctx, "")
	}

	log.Info().Msg("Skills repository updated")
	return nil
}

// LoadSkills walks the skills directory and loads all SKILL.md files.
func LoadSkills() ([]Skill, error) {
	skillsRoot := filepath.Join(SkillsDir(), skillsSubDir)

	entries, err := os.ReadDir(skillsRoot)
	if err != nil {
		return nil, fmt.Errorf("read skills directory %s: %w", skillsRoot, err)
	}

	var skills []Skill
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		skillPath := filepath.Join(skillsRoot, entry.Name())
		skillFile := filepath.Join(skillPath, skillFileName)

		data, err := os.ReadFile(skillFile)
		if err != nil {
			log.Warn().Str("skill", entry.Name()).Err(err).Msg("Skipping skill without SKILL.md")
			continue
		}

		skill := parseSkillMD(entry.Name(), skillPath, string(data))
		skills = append(skills, skill)
	}

	sort.Slice(skills, func(i, j int) bool {
		return skills[i].Name < skills[j].Name
	})

	return skills, nil
}

// parseSkillMD extracts display name, description, and content from SKILL.md.
func parseSkillMD(name, path, content string) Skill {
	skill := Skill{
		Name:    name,
		Path:    path,
		Content: content,
	}

	lines := strings.Split(content, "\n")
	var descLines []string
	foundHeading := false
	inDescription := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Extract heading
		if !foundHeading && strings.HasPrefix(trimmed, "# ") {
			skill.DisplayName = strings.TrimPrefix(trimmed, "# ")
			foundHeading = true
			inDescription = true
			continue
		}

		// Collect description (first paragraph after heading)
		if inDescription {
			if trimmed == "" {
				if len(descLines) > 0 {
					break // End of first paragraph
				}
				continue // Skip blank lines before first paragraph
			}
			if strings.HasPrefix(trimmed, "#") {
				break // Next heading
			}
			descLines = append(descLines, trimmed)
		}
	}

	if skill.DisplayName == "" {
		skill.DisplayName = name
	}
	skill.Description = strings.Join(descLines, " ")

	return skill
}

// SkillNames returns a sorted list of skill names for autocomplete.
func SkillNames(skills []Skill) []string {
	names := make([]string, len(skills))
	for i, s := range skills {
		names[i] = s.Name
	}
	return names
}

// FindSkillByName looks up a skill by name.
func FindSkillByName(skills []Skill, name string) *Skill {
	for i := range skills {
		if skills[i].Name == name {
			return &skills[i]
		}
	}
	return nil
}
