package main

import (
	"bufio"
	"errors"
	"fmt"

	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

const (
	// CHUNK_SIZE_IN_BYTES int64 = int64(float32(0.45 * 1024 * 1024 * 1024)) // 450MB -> works ugly
	CHUNK_SIZE_IN_BYTES int = 16 * 1024 * 1024 // 16MB
	// NUM_OF_WORKERS      int   = 160
)

var pln = fmt.Println

type MapElem struct {
	min   float32
	max   float32
	sum   float32
	count int
}

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		log.Fatal("Please provide a filename")
		return
	}
	var filename string = args[0]
	pln("filename: ", filename)

	// read file metadata
	file, size, err := openFile(filename)
	if err != nil {
		log.Fatal(err)
		return
	}

	var numberOfWorkers int = int(int(size)/CHUNK_SIZE_IN_BYTES) + 1
	var chunkSize int = int(CHUNK_SIZE_IN_BYTES)
	// var numberOfWorkers int = NUM_OF_WORKERS
	// var chunkSize int = int(size / int64(numberOfWorkers))

	pln("numberOfWorkers:", numberOfWorkers)
	// a channel of maps of partial parse results
	var channel chan map[string]MapElem = make(chan map[string]MapElem, numberOfWorkers)
	var firstErrChan chan error = make(chan error, 1)

	var offset int64
	for i := 0; i < numberOfWorkers; i++ {
		offset = int64(i) * int64(chunkSize)

		go parseFile(file, offset, chunkSize, channel, firstErrChan)
	}

	var mergedMap map[string]MapElem = make(map[string]MapElem)
	var i int = 0
	for i < numberOfWorkers {
		select {
		case subMap := <-channel:
			for k, v := range subMap {
				el, ok := mergedMap[k]
				if ok {
					el.min = min(el.min, v.min)
					el.max = max(el.max, v.max)
					el.sum += v.sum
					el.count += v.count
				} else {
					mergedMap[k] = v
				}
			}
		case err := <-firstErrChan:
			file.Close()
			log.Fatal(err)
			return
		}
		i++
	}

	close(channel)
	file.Close()

	// get sorted sortedKeys
	var sortedKeys []string = make([]string, 0, len(mergedMap))
	for k := range mergedMap {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	// print results
	for _, k := range sortedKeys {
		v := mergedMap[k]
		fmt.Printf("%s=%.1f/%.1f/%.1f, ", k, v.min, v.sum/float32(v.count), v.max)
	}
	pln()
}

func openFile(path string) (*os.File, int64, error) {
	var err error
	var file *os.File
	if file, err = os.OpenFile(path, os.O_RDONLY, 0444); err != nil {
		pln(err)
		return nil, 0, errorCannotOpenFileToRead
	}
	var fileInfo os.FileInfo
	if fileInfo, err = file.Stat(); err != nil {
		pln(err)
		return nil, 0, errorCannotFetchFileMetadata
	}
	pln("file size (bytes): ", fileInfo.Size())
	return file, fileInfo.Size(), nil
}

func parseFile(file *os.File, offset int64, chunkSize int, channel chan map[string]MapElem, errChan chan error) {
	// var content []byte = buffer[:bytesRead]
	var reader *bufio.Reader = bufio.NewReaderSize(file, int(CHUNK_SIZE_IN_BYTES))
	reader.Discard(int(offset))
	var fileScanner *bufio.Scanner = bufio.NewScanner(reader)
	// fileScanner.Buffer(buffer[:bytesRead])
	fileScanner.Split(bufio.ScanLines)

	var subMap map[string]MapElem = make(map[string]MapElem)
	var line string

	for i := 0; fileScanner.Scan(); i++ {
		line = fileScanner.Text()

		var lineParts []string = strings.Split(line, ";")
		if len(lineParts) != 2 || lineParts[0] == "" || lineParts[1] == "" {
			continue
		}
		// else if i == 0 {
		// 	println("lost content 1: ", line)
		// }

		processLine(subMap, lineParts[0], lineParts[1])
	}

	var lineParts []string = strings.Split(line, ";")
	if len(lineParts) == 2 && lineParts[0] != "" && lineParts[1] != "" {
		processLine(subMap, lineParts[0], lineParts[1])
	}

	channel <- subMap
}

func processLine(subMap map[string]MapElem, key string, value string) {
	var err error
	var val float64
	val, err = strconv.ParseFloat(value, 32)
	if err != nil {
		// println("lost content 2: ", value)
		return
	}
	var fv float32 = float32(val)

	el, ok := subMap[key]
	if ok {
		el.min = min(el.min, fv)
		el.max = max(el.max, fv)
		el.sum += fv
		el.count++
	} else {
		subMap[key] = MapElem{
			min:   fv,
			max:   fv,
			sum:   fv,
			count: 1,
		}
	}
}

var (
	errorCannotOpenFileToRead    = errors.New("cannot open file to read")
	errorCannotFetchFileMetadata = errors.New("cannot read file metadata")
)
