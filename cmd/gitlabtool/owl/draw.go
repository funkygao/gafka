package main

import (
	"fmt"

	"github.com/funkygao/golib/bjtime"
	"github.com/nsf/termbox-go"
)

var (
	w, h        int
	page        int
	pageSize    int
	selectedRow = 0
	detailView  = false
)

const (
	coldef = termbox.ColorDefault
)

func refreshSize() {
	w, h = termbox.Size()
	pageSize = h - 1 // footer=1
}

func redrawAll() {
	termbox.Clear(coldef, coldef)
	refreshSize()

	x, y := 0, 0
	lock.Lock()
	evts := reorderEvents()
	for i := page * pageSize; i < len(evts) && i < (page+1)*pageSize; i++ {
		evt := evts[i]
		drawEvent(x, y, evt)
		y++
	}

	drawFooter()
	lock.Unlock()

	termbox.Flush()
}

func drawDetail() {
	evts := reorderEvents()
	evt := evts[selectedRow]
	if _, ok := evt.(*Webhook); !ok {
		return
	}

	termbox.Clear(coldef, coldef)

	hook := evt.(*Webhook)

	y := 1
	fg, bg := coldef, coldef
	row := fmt.Sprintf("%7s: %s (%s)",
		"Repo",
		hook.Repository.Name, hook.Repository.Description)
	drawRow(row, y, fg, bg)
	y++

	row = fmt.Sprintf("%7s: %s",
		"Ref",
		hook.Ref)
	drawRow(row, y, fg, bg)
	y++

	row = fmt.Sprintf("%7s: %d",
		"Commits",
		hook.Total_commits_count)
	drawRow(row, y, fg, bg)
	y += 2

	for i := len(hook.Commits) - 1; i >= 0; i-- {
		c := hook.Commits[i]
		row = fmt.Sprintf("%14s %20s %s", since(c.Timestamp), c.Author.Email, c.Message)
		drawRow(row, y, termbox.ColorGreen, bg)
		y++

		row = fmt.Sprintf("%s", c.Url)
		drawRow(row, y, fg, bg)
		y += 2
	}

	termbox.Flush()
}

func drawRow(row string, y int, fg, bg termbox.Attribute) {
	for i, c := range row {
		termbox.SetCell(1+i, y, c, fg, bg)
	}
}

func isSelectedRow(y int) bool {
	return y == selectedRow-(pageSize*page)
}

func drawEvent(x, y int, evt interface{}) {
	fg_col, bg_col := coldef, coldef
	if isSelectedRow(y) {
		fg_col = termbox.ColorBlack
		bg_col = termbox.ColorYellow
	}

	var row string
	switch hook := evt.(type) {
	case *Webhook:
		if len(hook.Commits) == 0 {
			row = fmt.Sprintf("%14s %s %-25s",
				bjtime.TimeToString(hook.ctime),
				wideStr(hook.User_name, 20),
				hook.Repository.Name)
		} else {
			commit := hook.Commits[len(hook.Commits)-1] // the most recent commit
			row = fmt.Sprintf("%14s %s %-25s %s",
				since(commit.Timestamp),
				wideStr(hook.User_name, 20),
				hook.Repository.Name,
				commit.Message)
		}

	case *SystemHookProjectCreate:
		fg_col = termbox.ColorRed
		row = fmt.Sprintf("%14s %20s created project(%s)",
			since(hook.Created_at),
			hook.Owner_name,
			hook.Name)

	case *SystemHookProjectDestroy:
		fg_col = termbox.ColorRed
		row = fmt.Sprintf("%14s %20s destroy project(%s)",
			since(hook.Created_at),
			hook.Owner_name,
			hook.Path_with_namespace)

	case *SystemHookGroupCreate:
		fg_col = termbox.ColorRed
		row = fmt.Sprintf("%14s %20s created group(%s)",
			since(hook.Created_at),
			hook.Owner_name,
			hook.Name)

	case *SystemHookUserCreate:
		fg_col = termbox.ColorRed
		row = fmt.Sprintf("%14s %20s %s signup",
			since(hook.Created_at),
			hook.Name,
			hook.Email)

	case *SystemHookUserAddToGroup:
		fg_col = termbox.ColorRed
		row = fmt.Sprintf("%14s %20s join group(%s)",
			since(hook.Created_at),
			hook.User_name,
			hook.Group_name)

	case *SystemHookUserAddToTeam:
		fg_col = termbox.ColorRed
		row = fmt.Sprintf("%14s %20s join project(%s)",
			since(hook.Created_at),
			hook.User_name,
			hook.Project_name)

	case *SystemHookUnknown:
		fg_col = termbox.ColorMagenta
		row = fmt.Sprintf("%s", hook.Evt)
	}

	drawRow(row, y, fg_col, bg_col)
	if isSelectedRow(y) {
		for i := len(row); i < w; i++ {
			termbox.SetCell(1+i, y, ' ', fg_col, bg_col)
		}
	}
}

func drawSplash() {
	refreshSize()
	row := "loading gitlab events..."
	x, y := w/2-len(row)/2, h/2+1
	for i, c := range row {
		termbox.SetCell(x+i, y, c, termbox.ColorGreen, coldef)
	}
	termbox.Flush()
}

func drawNotify() {
	refreshSize()
	termbox.Clear(coldef, coldef)
	row := "Got a new event from gitlab!"
	x, y := w/2-len(row)/2, h/2+1
	for i, c := range row {
		termbox.SetCell(x+i, y, c, termbox.ColorGreen, coldef)
	}
	termbox.Flush()
}

func drawFooter() {
	s := calculateStats()
	help := "q:Close d:Detail j:Next k:Previous Space:PageDown b:PageUp /:Find"
	stats := fmt.Sprintf("[events:%d/%d-%d page:%d repo:%d staff:%d commit:%d]",
		selectedRow,
		loadedN,
		s.eventN,
		page,
		s.repoN,
		s.staffN,
		s.commitN)
	footerText := help
	for i := 0; i < w-len(help)-len(stats); i++ {
		footerText += " "
	}
	footerText += stats
	for i := 0; i < w; i++ {
		termbox.SetCell(i, h-1, ' ', coldef, termbox.ColorBlue)
	}
	for i, c := range footerText {
		termbox.SetCell(i, h-1, c, coldef, termbox.ColorBlue)
	}
}
