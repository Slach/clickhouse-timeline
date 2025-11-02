package tui

import (
	"database/sql"
	"fmt"
	"math"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Frame represents a node in the flamegraph tree
type Frame struct {
	Name     string
	Count    int
	Parent   *Frame
	Children []*Frame
}

// AddStack inserts a stack of frames into the tree, accumulating the count
func (f *Frame) AddStack(stack []string, count int) {
	if len(stack) == 0 {
		return
	}
	name := stack[0]
	var child *Frame
	for _, c := range f.Children {
		if c.Name == name {
			child = c
			break
		}
	}
	if child == nil {
		child = &Frame{
			Name:   name,
			Parent: f,
		}
		f.Children = append(f.Children, child)
	}
	child.Count += count
	child.AddStack(stack[1:], count)
}

// FocusedFrame represents a frame that currently has focus
type FocusedFrame struct {
	frame *Frame
	x     int
	y     int
	width int
	stack []string // Full stack from root to this frame
}

// FlamegraphDataMsg is sent when flamegraph data is loaded
type FlamegraphDataMsg struct {
	Root     *Frame
	MaxDepth int
	MaxCount int
	Err      error
}

// flamegraphViewer displays the flamegraph
type flamegraphViewer struct {
	root          *Frame
	maxDepth      int
	maxCount      int
	frames        []*FocusedFrame
	currentIdx    int
	width         int
	height        int
	loading       bool
	err           error
	categoryType  CategoryType
	categoryValue string
	traceType     TraceType
}

func newFlamegraphViewer(width, height int) flamegraphViewer {
	return flamegraphViewer{
		width:   width,
		height:  height,
		loading: true,
	}
}

func (m flamegraphViewer) Init() tea.Cmd {
	return nil
}

func (m flamegraphViewer) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case FlamegraphDataMsg:
		m.loading = false
		if msg.Err != nil {
			m.err = msg.Err
			return m, nil
		}

		m.root = msg.Root
		m.maxDepth = msg.MaxDepth
		m.maxCount = msg.MaxCount

		// Build frames list for navigation
		m.frames = m.buildFramesList()
		if len(m.frames) > 0 {
			m.currentIdx = 0
		}
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "esc", "q":
			return m, func() tea.Msg {
				return tea.KeyMsg{Type: tea.KeyEsc}
			}
		case "up":
			if m.currentIdx > 0 {
				m.currentIdx--
			}
		case "down":
			if m.currentIdx < len(m.frames)-1 {
				m.currentIdx++
			}
		case "left":
			// Navigate to parent
			if m.currentIdx > 0 && m.frames[m.currentIdx].frame.Parent != nil {
				// Find parent in frames list
				parent := m.frames[m.currentIdx].frame.Parent
				for i, f := range m.frames {
					if f.frame == parent {
						m.currentIdx = i
						break
					}
				}
			}
		case "right":
			// Navigate to first child
			if len(m.frames) > m.currentIdx {
				current := m.frames[m.currentIdx].frame
				if len(current.Children) > 0 {
					child := current.Children[0]
					for i, f := range m.frames {
						if f.frame == child {
							m.currentIdx = i
							break
						}
					}
				}
			}
		case "enter":
			// Show details for selected frame
			if m.currentIdx < len(m.frames) {
				// Could add a details view here
			}
		}
	}

	return m, nil
}

func (m flamegraphViewer) View() string {
	if m.loading {
		return "Loading flamegraph data..."
	}
	if m.err != nil {
		return fmt.Sprintf("Error loading flamegraph: %v\n\nPress ESC to return", m.err)
	}
	if m.root == nil || m.maxDepth == 0 || m.root.Count == 0 {
		return "No data available for selected parameters\n\nPress ESC to return"
	}

	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))

	title := fmt.Sprintf("Flamegraph - %s: %s (Trace: %s)",
		m.categoryType, m.categoryValue, m.traceType)

	// Render flamegraph
	flamegraphLines := m.renderFlamegraph()

	// Show focused frame info
	var focusInfo string
	if m.currentIdx < len(m.frames) {
		focused := m.frames[m.currentIdx]
		percentage := float64(focused.frame.Count) / float64(m.root.Count) * 100
		focusInfo = fmt.Sprintf("\nSelected: %s (samples: %d, %.2f%%)",
			focused.frame.Name, focused.frame.Count, percentage)
	}

	help := "\n\nArrows: Navigate | Enter: Details | Esc: Back"

	content := lipgloss.JoinVertical(lipgloss.Left,
		titleStyle.Render(title),
		focusInfo,
		"",
		flamegraphLines,
		helpStyle.Render(help),
	)

	return content
}

// renderFlamegraph renders the flamegraph as colored text blocks
func (m flamegraphViewer) renderFlamegraph() string {
	if m.root == nil || len(m.root.Children) == 0 {
		return "No frames to display"
	}

	// Calculate available width for rendering
	maxWidth := m.width - 4 // Leave some margin

	var lines []string

	// Render each level
	for level := 0; level < m.maxDepth && level < 40; level++ {
		line := m.renderLevel(level, maxWidth)
		if line != "" {
			lines = append(lines, line)
		}
	}

	return strings.Join(lines, "\n")
}

// renderLevel renders a single level of the flamegraph
func (m flamegraphViewer) renderLevel(level int, maxWidth int) string {
	// Collect all frames at this level
	levelFrames := m.getFramesAtLevel(level)
	if len(levelFrames) == 0 {
		return ""
	}

	// Calculate total count at this level
	totalCount := 0
	for _, frame := range levelFrames {
		totalCount += frame.Count
	}

	var blocks []string
	for _, frame := range levelFrames {
		// Calculate width proportional to count
		width := int(float64(maxWidth) * float64(frame.Count) / float64(totalCount))
		if width < 1 {
			width = 1
		}

		// Determine if this is the focused frame
		isFocused := false
		if m.currentIdx < len(m.frames) && m.frames[m.currentIdx].frame == frame {
			isFocused = true
		}

		// Get color based on heat
		color := m.getColorForCount(frame.Count, float64(frame.Count)/float64(totalCount))

		// Create styled block
		name := frame.Name
		if len(name) > width {
			name = name[:width]
		} else if len(name) < width {
			// Center the name
			padding := (width - len(name)) / 2
			name = strings.Repeat(" ", padding) + name + strings.Repeat(" ", width-len(name)-padding)
		}

		style := lipgloss.NewStyle().
			Foreground(lipgloss.Color("0")).
			Background(lipgloss.Color(color))

		if isFocused {
			style = style.
				Foreground(lipgloss.Color("0")).
				Background(lipgloss.Color("15")).
				Bold(true)
		}

		blocks = append(blocks, style.Render(name))
	}

	return strings.Join(blocks, "")
}

// getFramesAtLevel returns all frames at a specific depth level
func (m flamegraphViewer) getFramesAtLevel(targetLevel int) []*Frame {
	var frames []*Frame
	var collect func(frame *Frame, currentLevel int)
	collect = func(frame *Frame, currentLevel int) {
		if currentLevel == targetLevel {
			frames = append(frames, frame)
			return
		}
		for _, child := range frame.Children {
			collect(child, currentLevel+1)
		}
	}

	// Start from root's children (level 0)
	for _, child := range m.root.Children {
		collect(child, 0)
	}

	return frames
}

// getColorForCount returns a lipgloss color string based on count intensity
func (m flamegraphViewer) getColorForCount(count int, relativeRatio float64) string {
	if m.maxCount == 0 {
		return "11" // Yellow
	}

	// Use log2 scale for overall intensity
	logRatio := 0.0
	if count > 0 && m.maxCount > 0 {
		logRatio = math.Log2(1.0+7.0*float64(count)/float64(m.maxCount)) / math.Log2(8.0)
	}

	// Blend the log scale with the relative ratio
	blendedRatio := 0.7*logRatio + 0.3*relativeRatio

	// Map to ANSI 256 colors (red-orange-yellow spectrum)
	// Colors 196-226 are red to yellow in 256-color mode
	if blendedRatio < 0.3 {
		// Light colors (yellow-orange)
		return "11" // Yellow
	} else if blendedRatio < 0.6 {
		// Medium colors (orange)
		return "208" // Orange
	} else {
		// Hot colors (red-orange)
		return "196" // Red
	}
}

// buildFramesList creates a flat list of all frames for navigation
func (m flamegraphViewer) buildFramesList() []*FocusedFrame {
	var frames []*FocusedFrame

	var traverse func(frame *Frame, depth int, stack []string)
	traverse = func(frame *Frame, depth int, stack []string) {
		currentStack := append(stack, frame.Name)
		frames = append(frames, &FocusedFrame{
			frame: frame,
			y:     depth,
			stack: currentStack,
		})
		for _, child := range frame.Children {
			traverse(child, depth+1, currentStack)
		}
	}

	if m.root != nil {
		for _, child := range m.root.Children {
			traverse(child, 0, []string{})
		}
	}

	return frames
}

// buildFlamegraphFromRows builds the flamegraph tree from database rows
func buildFlamegraphFromRows(rows *sql.Rows) (*Frame, int, int, error) {
	root := &Frame{Name: "root"}
	maxDepth := 0

	for rows.Next() {
		var stack string
		var count int
		if err := rows.Scan(&count, &stack); err != nil {
			return nil, 0, 0, err
		}

		stackParts := strings.Split(stack, ";")
		if len(stackParts) > maxDepth {
			maxDepth = len(stackParts)
		}
		root.AddStack(stackParts, count)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, 0, err
	}

	// Update root count and compute max count
	total := 0
	for _, child := range root.Children {
		total += child.Count
	}
	root.Count = total

	// Compute maximum count among all frames
	maxCount := 0
	var computeMax func(frame *Frame)
	computeMax = func(frame *Frame) {
		for _, child := range frame.Children {
			if child.Count > maxCount {
				maxCount = child.Count
			}
			computeMax(child)
		}
	}
	computeMax(root)

	return root, maxDepth, maxCount, nil
}
