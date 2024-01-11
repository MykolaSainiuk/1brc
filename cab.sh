#!/bin/sh

time node --initial-heap-size=10000 --max-old-space-size=20000 --max-semi-space-size=2000 ./src/main/node/run.mjs ./measurements.txt

# make it worse
# --no-gc-global --gc-interval=10000000 --gc-stats=0 --no-fuzzer-gc-analysis
# --initial-heap-size=10000 --max-old-space-size=20000 --max-semi-space-size=2000
