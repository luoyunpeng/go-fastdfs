package main

import (
	"fmt"
	"sync"

	units "github.com/docker/go-units"
)

func main() {
	fmt.Println(units.HumanSize(187350))

	m := sync.Map{}

	m.Store("zhansan", 25)
	m.Store("lisi", "hello")

	fmt.Println(m.Load("zhansan"))
}
