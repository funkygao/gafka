package main

type eventStats struct {
	eventN  int
	repoN   int
	staffN  int
	commitN int
}

func calculateStats() eventStats {
	commitN := 0
	userMap := make(map[string]struct{})
	repoMap := make(map[string]struct{})
	for _, e := range events {
		switch hook := e.(type) {
		case *Webhook:
			commitN += hook.Total_commits_count
			repoMap[hook.Repository.Name] = struct{}{}
			for _, c := range hook.Commits {
				userMap[c.Author.Name] = struct{}{}
			}
		}
	}

	return eventStats{
		eventN:  len(events),
		repoN:   len(repoMap),
		staffN:  len(userMap),
		commitN: commitN,
	}

}
