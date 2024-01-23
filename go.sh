#!/bin/sh

# time ./src/main/go/onebrc ./measurements.txt
time go run ./src/main/go/main.go ./measurements.txt
# time go run ./src/main/go/main.go $1

