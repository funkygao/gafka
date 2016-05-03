package command

import (
	"sort"
)

func swalllow(err error) {
	if err != nil {
		panic(err)
	}
}

func sortBackendByName(all []Backend) []Backend {
	m := make(map[string]Backend, len(all))
	sortedNames := make([]string, 0, len(all))
	for _, b := range all {
		m[b.Name] = b
		sortedNames = append(sortedNames, b.Name)
	}
	sort.Strings(sortedNames)

	r := make([]Backend, 0, len(all))
	for _, name := range sortedNames {
		r = append(r, m[name])
	}

	return r
}
