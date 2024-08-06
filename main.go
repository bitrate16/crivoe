package main

import (
	"crivoe/config"
	"fmt"
)

func main() {
	args := config.GetConfig()

	fmt.Printf("%v", args)
}
