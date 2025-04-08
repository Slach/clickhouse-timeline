package flamegraph

import (
	"database/sql"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"math"
	"strconv"
	"strings"
	"time"
)

// FormatStackWithNumbers adds numbering to each frame in the stack
func FormatStackWithNumbers(stack []string) string {
	var builder strings.Builder
	for i, frame := range stack {
		builder.WriteString(strconv.Itoa(i + 1))
		builder.WriteString(". ")
		builder.WriteString(frame)
		builder.WriteString("\n")
	}
	return builder.String()
}

// Frame represents a node in the flamegraph.
type Frame struct {
	Name     string
	Count    int
	Parent   *Frame
	Children []*Frame
}

// AddStack inserts a stack of frames into the tree, accumulating the count.
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

// Direction defines the drawing direction of the flamegraph.
type Direction int

const (
	// DirectionTopDown draws from top to bottom (root at top)
	DirectionTopDown Direction = iota
	// DirectionBottomUp draws from bottom to top (root at bottom)
	DirectionBottomUp
)

// FocusedFrame represents a frame that currently has focus.
type FocusedFrame struct {
	frame *Frame
	x     int
	y     int
	width int
}

// FrameHandler is a function that handles frame selection events.
// It receives the full stack trace from root to the selected frame and the count.
type FrameHandler func(stack []string, count int)

// PageSwitcherFunc is a function that switches to a specified page
type PageSwitcherFunc func(targetPage string)

// FlameView is a custom tview widget for drawing flamegraphs.
type FlameView struct {
	*tview.Box
	data         string
	root         *Frame
	maxDepth     int
	maxCount     int
	direction    Direction
	focused      *FocusedFrame
	frames       []*FocusedFrame  // All frames for navigation
	currentIdx   int              // Current focused frame index
	handler      FrameHandler     // Handler for selection events
	lastClick    time.Time        // For double-click detection
	sourcePage   string           // The page that opened this flamegraph
	pageSwitcher PageSwitcherFunc // Function to switch pages
}

// NewFlamegraphView creates a new instance of the flamegraph widget.
// Default direction is top-down.
func NewFlamegraphView() *FlameView {
	f := &FlameView{
		Box:       tview.NewBox(),
		direction: DirectionTopDown,
	}
	f.SetInputCapture(f.handleInput)
	f.SetMouseCapture(func(action tview.MouseAction, event *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse) {
		return f.handleMouse(action, event)
	})
	return f
}

// SetFrameHandler sets the handler function for frame selection events.
func (f *FlameView) SetFrameHandler(handler FrameHandler) {
	f.handler = handler
}

// SetSourcePage sets the name of the page that opened this flamegraph
func (f *FlameView) SetSourcePage(sourcePage string) {
	f.sourcePage = sourcePage
}

// SetPageSwitcher sets the function that will be called to switch pages
func (f *FlameView) SetPageSwitcher(switcher PageSwitcherFunc) {
	f.pageSwitcher = switcher
}

// SetDirection sets the drawing direction of the flamegraph.
func (f *FlameView) SetDirection(d Direction) {
	f.direction = d
}

// SetData accepts the raw flamegraph data as a string.
func (f *FlameView) SetData(data string) {
	f.data = data
	f.parseData()
}

// BuildFromRows builds the flamegraph tree directly from database rows.
// This avoids the need to write to and read from temporary files.
func (f *FlameView) BuildFromRows(rows *sql.Rows) error {
	f.root = &Frame{Name: "root"}
	f.maxDepth = 0

	for rows.Next() {
		var stack string
		var count int
		if err := rows.Scan(&count, &stack); err != nil {
			return err
		}

		stackParts := strings.Split(stack, ";")
		if len(stackParts) > f.maxDepth {
			f.maxDepth = len(stackParts)
		}
		f.root.AddStack(stackParts, count)
	}

	// Check for errors after the loop
	if err := rows.Err(); err != nil {
		return err
	}

	// Update root count and compute max count
	f.updateRootAndMaxCount()
	return nil
}

// parseData builds the flamegraph tree from the provided data.
func (f *FlameView) parseData() {
	f.root = &Frame{Name: "root"}
	f.maxDepth = 0
	lines := strings.Split(f.data, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// Expected format: "root;func1;func2 30"
		parts := strings.Split(line, " ")
		if len(parts) < 2 {
			continue
		}
		stackStr := parts[0]
		countStr := parts[1]
		count, err := strconv.Atoi(countStr)
		if err != nil {
			continue
		}
		stack := strings.Split(stackStr, ";")
		if len(stack) > f.maxDepth {
			f.maxDepth = len(stack)
		}
		f.root.AddStack(stack, count)
	}

	f.updateRootAndMaxCount()
}

// updateRootAndMaxCount updates the root count from children totals and computes max count
func (f *FlameView) updateRootAndMaxCount() {
	// Update root count from children totals.
	total := 0
	for _, child := range f.root.Children {
		total += child.Count
	}
	f.root.Count = total

	// Compute maximum count among all frames (excluding root).
	f.maxCount = 0
	var computeMax func(frame *Frame)
	computeMax = func(frame *Frame) {
		for _, child := range frame.Children {
			if child.Count > f.maxCount {
				f.maxCount = child.Count
			}
			computeMax(child)
		}
	}
	computeMax(f.root)
}

// colorForCount returns a color based on the frame's count relative to maxCount,
// using a log2 scale and focusing more on red colors.
func colorForCount(count, maxCount int, relativeRatio float64) tcell.Color {
	if maxCount == 0 {
		return tcell.ColorYellow
	}

	// Use log2 scale for overall intensity
	logRatio := 0.0
	if count > 0 && maxCount > 0 {
		// Log scale from 0 to 1
		logRatio = math.Log2(1.0+7.0*float64(count)/float64(maxCount)) / math.Log2(8.0)
	}

	// Blend the log scale with the relative ratio within the same level
	// This gives more weight to the relative size within siblings
	blendedRatio := 0.7*logRatio + 0.3*relativeRatio

	// Calculate green component - less green means more red
	// Start with more red (less green) even for small values
	var g int
	if blendedRatio < 0.3 {
		// For small values, start at yellow-orange (g=220) and go to orange (g=165)
		g = int(220 - blendedRatio/0.3*55)
	} else {
		// For larger values, go from orange (g=165) to deep red (g=0)
		g = int(165 - (blendedRatio-0.3)/0.7*165)
	}

	if g < 0 {
		g = 0
	} else if g > 255 {
		g = 255
	}

	return tcell.NewRGBColor(int32(255), int32(g), int32(0))
}

// Draw renders the flamegraph widget on the screen.
func (f *FlameView) Draw(screen tcell.Screen) {
	f.Box.DrawForSubclass(screen, f)
	x, y, width, height := f.GetInnerRect()
	if f.root == nil || f.maxDepth == 0 {
		return
	}

	// Reset frames collection before drawing
	f.frames = []*FocusedFrame{}

	// Each level occupies one row.
	levels := f.maxDepth
	levelHeight := 1

	var baseY int
	if f.direction == DirectionTopDown {
		// Top-down: root at top, children below
		baseY = y
	} else {
		// Bottom-up: root at bottom, children above
		baseY = y + height - levels*levelHeight
	}

	// Start drawing from the root's children.
	drawFrame(screen, x, baseY, width, f.root, f.maxCount, 0, f.direction, f)

	// Set initial focus if none
	if f.focused == nil && len(f.frames) > 0 {
		f.focused = f.frames[0]
		f.currentIdx = 0
	}

	// Draw focus highlight
	if f.focused != nil {
		style := tcell.StyleDefault.Background(tcell.ColorWhite).Foreground(tcell.ColorBlack)
		for i := 0; i < f.focused.width; i++ {
			screen.SetContent(f.focused.x+i, f.focused.y, ' ', nil, style)
		}
		// Center and draw the function name inside the rectangle
		name := f.focused.frame.Name
		startX := f.focused.x
		if len(name) < f.focused.width {
			startX += (f.focused.width - len(name)) / 2
		}
		for i, ch := range name {
			if i+startX < f.focused.x+f.focused.width {
				screen.SetContent(i+startX, f.focused.y, ch, nil, style)
			}
		}
	}
}

// drawFrame recursively draws each frame's rectangle with color based on count.
// direction determines whether to draw children above or below the parent.
func drawFrame(screen tcell.Screen, x, y, width int, frame *Frame, maxCount int, depth int, direction Direction, f *FlameView) int {
	if len(frame.Children) == 0 {
		return x
	}
	currentX := x
	// Calculate total count for proportional widths.
	total := 0
	for _, child := range frame.Children {
		total += child.Count
	}
	for _, child := range frame.Children {
		// Determine the width allocated for this child.
		childWidth := int(float64(width) * float64(child.Count) / float64(total))
		if childWidth < 1 {
			childWidth = 1
		}

		// Calculate relative ratio within this level for more accurate coloring
		relativeRatio := float64(child.Count) / float64(total)

		// Determine color based on the child's count and relative ratio.
		color := colorForCount(child.Count, maxCount, relativeRatio)
		var rectY int
		if direction == DirectionTopDown {
			rectY = y + depth
		} else {
			rectY = y - depth
		}
		// Fill the rectangle with the background color.
		for i := 0; i < childWidth; i++ {
			style := tcell.StyleDefault.Background(color).Foreground(tcell.ColorBlack)
			screen.SetContent(currentX+i, rectY, ' ', nil, style)
		}
		// Center and draw the function name inside the rectangle.
		name := child.Name
		startX := currentX
		if len(name) < childWidth {
			startX += (childWidth - len(name)) / 2
		}
		for i, ch := range name {
			if i+startX < currentX+childWidth {
				style := tcell.StyleDefault.Background(color).Foreground(tcell.ColorBlack)
				screen.SetContent(i+startX, rectY, ch, nil, style)
			}
		}
		// Store frame info for navigation
		f.frames = append(f.frames, &FocusedFrame{
			frame: child,
			x:     currentX,
			y:     rectY,
			width: childWidth,
		})

		// Recursively draw the child's children.
		drawFrame(screen, currentX, y, childWidth, child, maxCount, depth+1, direction, f)
		currentX += childWidth
	}
	return currentX
}

// handleInput processes keyboard input for navigation
func (f *FlameView) handleInput(event *tcell.EventKey) *tcell.EventKey {
	if len(f.frames) == 0 {
		return event
	}

	current := f.frames[f.currentIdx]
	currentY := current.y

	// Let Ctrl+C pass through to allow program termination
	if event.Key() == tcell.KeyCtrlC {
		return event
	}
	if event.Key() == tcell.KeyRune && event.Rune() == '\uFFFD' {
		return nil
	}

	// Handle ESC key to return to source page or flamegraph view
	if event.Key() == tcell.KeyEscape {
		if f.pageSwitcher != nil {
			// If we're showing stacktrace, go back to flamegraph
			if strings.HasSuffix(f.sourcePage, "stacktrace") {
				f.pageSwitcher("flamegraph")
			} else if f.sourcePage != "" {
				// Return to source page based on where the flamegraph was called from
				if f.sourcePage == "heatmap" {
					f.pageSwitcher("heatmap")
				} else {
					f.pageSwitcher("flamegraph_form")
				}
			}
			return nil
		}
	}

	switch event.Key() {
	case tcell.KeyRight:
		// Find next frame at same level
		for i := f.currentIdx + 1; i < len(f.frames); i++ {
			if f.frames[i].y == currentY {
				f.currentIdx = i
				break
			}
		}
	case tcell.KeyLeft:
		// Find previous frame at same level
		for i := f.currentIdx - 1; i >= 0; i-- {
			if f.frames[i].y == currentY {
				f.currentIdx = i
				break
			}
		}
	case tcell.KeyUp:
		// Find parent frame using Parent pointer
		if current.frame.Parent != nil {
			// Find the parent in frames list
			for i, frame := range f.frames {
				if frame.frame == current.frame.Parent {
					f.currentIdx = i
					break
				}
			}
		}
	case tcell.KeyDown:
		// Find first child frame
		for i, frame := range f.frames {
			if frame.y > currentY && frame.x <= current.x && frame.x+frame.width > current.x {
				f.currentIdx = i
				break
			}
		}
	case tcell.KeyEnter:
		if f.handler != nil {
			stack, count := f.getCurrentStack()
			f.handler(stack, count)
		}
		return nil
	default:
		return event
	}

	f.focused = f.frames[f.currentIdx]
	return nil
}

// handleMouse processes mouse input for navigation and selection
func (f *FlameView) handleMouse(action tview.MouseAction, event *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse) {
	if len(f.frames) == 0 {
		return action, event
	}

	x, y := event.Position()

	// Check for left mouse button click
	if action == tview.MouseLeftClick {
		now := time.Now()
		isDoubleClick := !f.lastClick.IsZero() && now.Sub(f.lastClick) < 500*time.Millisecond
		f.lastClick = now

		// Find clicked frame
		for i, frame := range f.frames {
			if y == frame.y && x >= frame.x && x < frame.x+frame.width {
				f.currentIdx = i
				f.focused = frame
				if isDoubleClick && f.handler != nil {
					stack, count := f.getCurrentStack()
					f.handler(stack, count)
				}
				return action, nil
			}
		}
	}

	return action, event
}

// getCurrentStack returns the full stack trace from root to current frame and the count
func (f *FlameView) getCurrentStack() ([]string, int) {
	if f.focused == nil {
		return nil, 0
	}

	var stack []string
	current := f.focused.frame
	count := current.Count

	// Walk up the tree to build the stack
	for current != nil {
		stack = append([]string{current.Name}, stack...)
		current = current.Parent
	}

	return stack, count
}

func (f *FlameView) GetTotalCount() int {
	return f.root.Count
}
