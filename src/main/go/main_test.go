package main

import (
	"fmt"
	"os"
	"testing"
)

// go test -bench=. -run -benchtime=5x -count=5 -cpuprofile cpu.prof -blockprofile goblock.prof
func BenchmarkMain(b *testing.B) {
	fmt.Println("---- BenchmarkMain ----")

	os.Args[1] = "./../../../measurements.txt"
	os.Args[2] = "15"
	for n := 0; n < b.N; n++ {
		fmt.Printf("---- BenchmarkMain %d ----", 15)
		main()
	}
}

// suppose to be called like this
// go test -- ../../../measurements.txt
func TestMain(t *testing.T) {
	fmt.Println("---- TestMain ----")

	if len(os.Args) == 5 {
		os.Args[1] = os.Args[4]
	} else {
		os.Args[1] = "./../../../measurements.txt"
	}

	main()
}
