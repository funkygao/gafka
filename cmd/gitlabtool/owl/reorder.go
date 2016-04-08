package main

func reorderEvents() {
	evts := make([]interface{}, 0, len(events))
	for i := len(events) - 1; i >= 0; i-- {
		evts = append(evts, events[i])
	}
	events = evts
}
