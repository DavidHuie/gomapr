// Runs MapReduce on a file, returning counts for the contents
// of a grouped regular expression.

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"runtime"
	"sort"

	"github.com/DavidHuie/gomapr"
)

type LogCount struct {
	scanner *bufio.Scanner
	re      *regexp.Regexp
}

func NewLogCount(path string, re *regexp.Regexp) (*LogCount, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(file)
	return &LogCount{scanner, re}, nil
}

func (l *LogCount) Emit() (gomapr.Event, error) {
	if !l.scanner.Scan() {
		if err := l.scanner.Err(); err != nil && err != io.EOF {
			log.Printf("Error scanning log: %v", err)
		}
		return l.scanner.Text(), gomapr.EndOfEmit
	}
	return l.scanner.Text(), nil
}

func (l *LogCount) Map(e gomapr.Event) (gomapr.ReduceKey, gomapr.Partial) {
	submatches := l.re.FindSubmatch([]byte(e.(string)))

	if submatches != nil {
		return string(submatches[1]), int64(1)
	}

	return "(none)", int64(1)
}

func (l *LogCount) Reduce(k gomapr.ReduceKey, p []gomapr.Partial) (gomapr.ReduceKey, gomapr.Partial) {
	sum := int64(0)
	for _, partial := range p {
		sum += partial.(int64)
	}
	return k, sum
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	path := flag.String("path", "", "A path to a log file")
	re := flag.String("re", "()", "A regular expression with a single group")
	limit := flag.Int("limit", 100, "The number of results to display")
	flag.Parse()

	regex := regexp.MustCompile(*re)

	lc, err := NewLogCount(*path, regex)
	if err != nil {
		panic(err)
	}

	runner := gomapr.NewRunner(lc, runtime.NumCPU(), 1)
	runner.Run()
	results := runner.Results()
	sort.Sort(results)

	resultsPrinted := 0
	for _, result := range results {
		fmt.Printf("Key: '%v'\n", result.Key)
		fmt.Printf("Value: %v\n", result.Value)

		resultsPrinted += 1
		if resultsPrinted == *limit {
			return
		}

		fmt.Println()
	}
}
