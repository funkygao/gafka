package command

func ensureZoneValid(zone string) {
	if _, present := cf.Zones[zone]; !present {
		panic("invalid zone: " + zone)
	}
}
