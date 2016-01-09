package main

type Consumer struct {
	MyAppId  string `json:"myapp"`
	HisAppId string `json:"hisapp"`
	Topic    string `json:"topic"`
	Ver      string `json:"ver"`
	Group    string `json:"group"`
}
