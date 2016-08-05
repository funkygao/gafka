package controller

type Controller interface {
}

type controller struct {
}

func New() Controller {
	return &controller{}
}
