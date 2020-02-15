package main

import (
	"fmt"
	"sync"

	"github.com/docker/go-units"
	"github.com/luoyunpeng/go-fastdfs/pkg"
)

func main() {

	fmt.Println(units.BytesSize(82854982))
	fmt.Println(pkg.HumanSize(82854982))
	m := sync.Map{}

	m.Store("zhansan", 25)
	m.Store("lisi", "hello")

	fmt.Println(m.Load("zhansan"))
}
