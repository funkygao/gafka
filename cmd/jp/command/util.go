package command

func swallow(err error) {
	if err != nil {
		panic(err)
	}
}
