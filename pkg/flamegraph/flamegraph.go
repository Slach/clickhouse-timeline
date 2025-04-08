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
	frameMap     map[*Frame]*FocusedFrame // Direct mapping from Frame to FocusedFrame
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
		frameMap:  make(map[*Frame]*FocusedFrame),
	}
	f.SetInputCapture(f.handleInput)
	f.SetMouseCapture(func(action tview.MouseAction, event *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse) {
		return f.handleMouse(action, event)
	})
	return f
}

// handleInput processes keyboard input for navigation
func (f *FlameView) handleInput(event *tcell.EventKey) *tcell.EventKey {
	if len(f.frames) == 0 {
		return event
	}

	current := f.frames[f.currentIdx]

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
		if current.frame.Right != nil {
			if focused, exists := f.frameMap[current.frame.Right]; exists {
				f.currentIdx = focused.index
				f.focused = focused
			}
		}
	case tcell.KeyLeft:
		if current.frame.Left != nil {
			if focused, exists := f.frameMap[current.frame.Left]; exists {
				f.currentIdx = focused.index
				f.focused = focused
			}
		}
	case tcell.KeyUp:
		if current.frame.Parent != nil {
			if focused, exists := f.frameMap[current.frame.Parent]; exists {
				f.currentIdx = focused.index
				f.focused = focused
			}
		}
	case tcell.KeyDown:
		if len(current.frame.Children) > 0 {
			if focused, exists := f.frameMap[current.frame.Children[0]]; exists {
				f.currentIdx = focused.index
				f.focused = focused
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

	return nil
}

// FocusedFrame represents a frame that currently has focus.
type FocusedFrame struct {
	frame *Frame
	x     int
	y     int
	width int
	index int // Index in the frames slice
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
	f.frameMap = make(map[*Frame]*FocusedFrame)

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
		focused := &FocusedFrame{
			frame: child,
			x:     currentX,
			y:     rectY,
			width: childWidth,
			index: len(f.frames),
		}
		f.frames = append(f.frames, focused)
		f.frameMap[child] = focused

		// Recursively draw the child's children.
		drawFrame(screen, currentX, y, childWidth, child, maxCount, depth+1, direction, f)
		currentX += childWidth
	}
	return currentX
}
