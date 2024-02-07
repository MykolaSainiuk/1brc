package main

import (
	"bufio"
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"onebrc/gopool"
)

const (
	// CHUNK_SIZE_IN_BYTES int64 = int64(float32(0.45 * 1024 * 1024 * 1024)) // 450MB -> works ugly
	CHUNK_SIZE_IN_BYTES     int = 16 * 1024 * 1024 // 16MB
	GOROUTINES_LOAD_PERCENT int = 20
	// NUM_OF_WORKERS      int   = 160
)

var COMMA_DOT_SEP []byte = []byte(";")

var pln = fmt.Println

var mergedMap map[string]MapElem = make(map[string]MapElem)

type MapElem struct {
	min   float32
	max   float32
	sum   float32
	count int
}

// var MapElemsPool = sync.Pool{
// 	New: func() interface{} {
// 		return &MapElem{}
// 	},
// }
// var ChunkBufferPool = sync.Pool{
// 	New: func() interface{} {
// 		s := make([]byte, CHUNK_SIZE_IN_BYTES)
// 		return &s
// 	},
// }

func main() {
	pln("logical processors: ", runtime.NumCPU())
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
	defer func() {
		close(channel)
		file.Close()
	}()

	var offsetChan chan int64 = make(chan int64, numberOfWorkers)
	var offset int64
	for i := 0; i < numberOfWorkers; i++ {
		offset = int64(i) * int64(chunkSize)
		offsetChan <- offset
	}

	perIterN := int(float32(numberOfWorkers)*float32(0.1)) + 1 // 10% by default
	if len(args) > 1 {
		ov := perIterN
		perIterN, err = strconv.Atoi(args[1])
		if perIterN == 0 || err != nil {
			perIterN = ov
		}
	}
	fmt.Printf("running %d goroutines each lap\n", perIterN)

	gp := gopool.NewPool(numberOfWorkers, perIterN)

	gp.Run(parseFile2, mergeMaps, file, offsetChan, chunkSize, channel)

	gp.Await()

	pln("mergedMap size: ", len(mergedMap))
	// get sorted sortedKeys
	var sortedKeys []string = make([]string, 0, len(mergedMap))
	for k := range mergedMap {
		// sortedKeys = append(sortedKeys, k)
		sortedKeys = Insert(sortedKeys, k)
	}
	// sort.Strings(sortedKeys)

	// print results
	var sb strings.Builder
	for _, k := range sortedKeys {
		sb.Write(fmt.Appendf(nil, "%s=%.1f/%.1f/%.1f, ", k, mergedMap[k].min, mergedMap[k].sum/float32(mergedMap[k].count), mergedMap[k].max))
	}
	pln(sb.String())
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

func parseFile2(params ...any) {
	file := params[0].(*os.File)
	offsetChan := params[1].(chan int64)
	chunkSize := params[2].(int)
	channel := params[3].(chan map[string]MapElem)

	offset := <-offsetChan
	var shifterBackOffset int64 = 0
	if offset > 0 {
		shifterBackOffset = offset - 30*4 // 30 runes is enough to cover the longest prev row
		// with shifterBackOffset 30 runes back we're safe to skip first invalid line if there is such
	}

	var separateSectionReader *io.SectionReader = io.NewSectionReader(file, shifterBackOffset, int64(chunkSize))
	var fileScanner *bufio.Scanner = bufio.NewScanner(separateSectionReader)
	fileScanner.Split(bufio.ScanLines)

	subMap := make(map[string]MapElem)
	for i := 0; fileScanner.Scan(); i++ {
		lineParts := bytes.Split(fileScanner.Bytes(), COMMA_DOT_SEP)

		if len(lineParts) == 2 {
			r, _ := utf8.DecodeRune(lineParts[0])
			if unicode.IsUpper(r) {
				processLine(subMap, string(lineParts[0]), string(lineParts[1]))
			}
		}
	}

	channel <- subMap
}

func mergeMaps(subMapsToIter int, params ...any) {
	channel := params[3].(chan map[string]MapElem)

	for i := 0; i < subMapsToIter; i++ {
		select {
		case subMap := <-channel:
			for k := range subMap {
				el, ok := mergedMap[k]
				if ok {
					el.min = min(el.min, subMap[k].min)
					el.max = max(el.max, subMap[k].max)
					el.sum += subMap[k].sum
					el.count += subMap[k].count
					mergedMap[k] = el
				} else {
					mergedMap[k] = subMap[k]
				}
			}
		default:
			log.Fatal("no subMap to merge but expected")
		}
	}
}

func Insert[T cmp.Ordered](ts []T, t T) []T {
	i, _ := slices.BinarySearch(ts, t) // find slot
	return slices.Insert(ts, i, t)
}

func processLine(subMap map[string]MapElem, key string, value string) {
	var val float64
	val, err := strconv.ParseFloat(value, 32)
	if err != nil {
		// pln("[ZYskipped str] error parsing float: ", err)
		return
	}
	var fv float32 = float32(val)

	el, ok := subMap[key]
	if ok {
		el.min = min(el.min, fv)
		el.max = max(el.max, fv)
		el.sum += fv
		el.count++
		subMap[key] = el
	} else {
		subMap[key] = MapElem{min: fv, max: fv, sum: fv, count: 1}
		// memPool := MapElemsPool.Get().(*MapElem)
		// memPool.min = fv
		// memPool.max = fv
		// memPool.sum = fv
		// memPool.count = 1

		// subMap[key] = *memPool

		// MapElemsPool.Put(memPool)
	}
}

var (
	errorCannotOpenFileToRead    = errors.New("cannot open file to read")
	errorCannotFetchFileMetadata = errors.New("cannot read file metadata")
	errNothingToRead             = errors.New("nothing left to read")
)
