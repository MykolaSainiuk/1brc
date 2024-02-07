package main

import (
	"os"
	"testing"
)

// suppose to be called like this
// go test -- ../../../measurements.txt
func TestMain(t *testing.T) {
	if len(os.Args) == 5 {
		os.Args[1] = os.Args[4]
	} else {
		os.Args[1] = "./../../../measurements.txt"
	}

	main()
}
