package main

import (
	"fmt"
	e2j "github.com/liquidm/elastic_journald"
)

func main() {
	fmt.Println("Hello World")
	service := e2j.NewService()
	service.Run()
}
