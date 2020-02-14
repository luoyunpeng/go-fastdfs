package main

import (
	"fmt"
	"os"

	"github.com/luoyunpeng/go-fastdfs/pkg"
)

func main() {
	f1, err := os.Open("")
	if err != nil {
		panic(err)
	}
	defer f1.Close()

	f2, err := os.Open("")
	if err != nil {
		panic(err)
	}
	defer f1.Close()

	fmt.Println(pkg.GetFileMd5(f1))
	fmt.Println(pkg.GetFileMd5(f2))
}
