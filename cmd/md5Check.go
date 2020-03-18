package main

import (
	"fmt"
	"io/ioutil"
	"strconv"
)

const (
	one = iota
	two
	three
)

type Demo struct {
	name string
	age  byte
}

func main() {
	age := []int{1, 5, 2, 8, 4, 6}

	res := make([]int, 3)
	copy(res, age[3:6])

	fmt.Println(one, two)

	fmt.Println(strconv.Atoi("21a34"))
	info, _ := ioutil.ReadDir("")
}
