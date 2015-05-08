package main

import (
	"flag"

	e2j "github.com/liquidm/elastic_journald"
)

func main() {
	flag.Parse()
	service := e2j.NewService()
	service.Run()
}
