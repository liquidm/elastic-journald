package main

import (
	e2j "github.com/liquidm/elastic_journald"
)

func main() {
	service := e2j.NewService()
	service.Init()
	go service.Run()
	service.Wait(service.Shutdown)
}
