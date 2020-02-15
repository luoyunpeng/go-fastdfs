package main

import (
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/luoyunpeng/go-fastdfs/pkg"
)

func main() {
	fmt.Println("today: ", pkg.Today())
	hour := pkg.FormatTimeByHour(time.Now())
	fmt.Println("now:", hour, "***", len(hour))

	dir := "svg/"
	paht := "files/hub"
	dir = strings.Split(dir, paht)[0]
	fmt.Println(dir)
	fmt.Println(path.Join("http://demo/opt/demo/file", "/opt/demo"))

	peer := "123"
	var peers []string

	for _, v := range peers {
		if v == peer {

		}
	}
}

func app() {
	nameCH := make(chan string)
	go func() {
		for i := 0; i < 5; i++ {
			nameCH <- "name"
		}
		close(nameCH)
	}()
	time.Sleep(time.Millisecond * 88)
	for v := range nameCH {
		fmt.Println("v: ", v)
	}
}
