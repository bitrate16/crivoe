package main

import (
	"crivoe/config"
	"fmt"
)

func main() {
	args := config.NewConfig()

	fmt.Printf("%v", args)
}
