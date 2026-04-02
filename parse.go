package csv_cleaner

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/mackerelio/go-osstat/memory"
)

func ParseFileToStrings(fileName string) ([][]string, error) {
	// Parses csv into [][]string ignoring types.
	// This is done in 100mb chunks.
	// The ENTIRE FILE IS LOADED INTO MEMORY
	records := make([][]string, 0)

	path := fileName

	fileInfo, err := os.Stat(path)
	if err != nil {
		return records, fmt.Errorf("could not read file metadata, does it exist? error: %s", err.Error())
	}
	fileSizeBytes := fileInfo.Size()

	canFit, err := fileCanFitInMemory(fileSizeBytes)
	if !canFit || err != nil {
		return records, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return records, fmt.Errorf("Failed to read file name: %s, error: %s", path, err.Error())
	}

	numBytes := len(data)
	chunkSize := 1024 * 1024 * 100 // 100MB chunk size
	cursor := 0

	prevTail := []byte{}
	endOfFile := false
	for cursor < numBytes {
		curChunk := []byte{}
		if cursor+chunkSize < numBytes {
			curChunk = data[cursor : cursor+chunkSize]
		} else {
			curChunk = data[cursor:]
			endOfFile = true
		}
		cursor += chunkSize

		curChunk = slices.Concat(prevTail, curChunk)

		newRecords, tail, err := parseChunk(curChunk, endOfFile)
		if err != nil {
			return records, fmt.Errorf("csv parsing error in file: %s -> %s", fileName, err.Error())
		}

		if endOfFile && len(tail) > 0 {
			return records, fmt.Errorf("a programming error occured when parsing file. tail was not nil at EOF")
		}

		prevTail = []byte{}
		prevTail = tail
		records = slices.Concat(records, newRecords)
	}

	return records, nil
}

func parseChunk(chunk []byte, endOfFile bool) ([][]string, []byte, error) {
	// Returns:
	// - Records from chunk
	// - Tail (all bytes which cannot be confirmed to be a complete line)
	// - Optional Error

	s := string(chunk)
	lines := strings.Split(s, "\n")

	// if no newline chars are present and this is not the final chunk, this chunk is less than one complete line
	// this is unlikely but possible.
	// if this is the last chunk, then it must be assumed that the entire chunk is just one line
	if len(lines) <= 1 && !endOfFile {
		return [][]string{}, chunk, nil
	}

	records := [][]string{}
	tail := []byte{}

	numLines := len(lines)
	for idx, line := range lines {
		if line == "" || line == "\n" {
			// new line only, or blank lines are skipped on purpose
			continue
		}
		if (idx == (numLines - 1)) && !endOfFile {
			// final line may or may not be a complete row, so it must be passed on
			tail = []byte(line)
			continue
		}
		if strings.Contains(line, "\\,") {
			return records, tail, fmt.Errorf("an invalid string is in the file: '\\,'")
		}
		fields := strings.Split(line, ",")
		records = append(records, fields)
	}
	return records, tail, nil
}

func fileCanFitInMemory(fileSizeBytes int64) (bool, error) {
	stats, _ := memory.Get()
	systemMemoryTotalBytes := int64(stats.Total)
	systemMemoryAvailableBytes := int64(stats.Available)

	expectedMemoryUsageOfProcess := int64(float64(fileSizeBytes) * 1.25)

	if expectedMemoryUsageOfProcess > systemMemoryTotalBytes {
		return false, fmt.Errorf("file was not able to be openend because there is not enough memory on this machine. file size in bytes: %v, expected memory footprint: %v, total memory: %v", fileSizeBytes, int64(float64(fileSizeBytes)*1.25), systemMemoryTotalBytes)
	}
	if expectedMemoryUsageOfProcess > systemMemoryAvailableBytes {
		return false, fmt.Errorf("file was not able to be openend because there was not enough free memory. file size in bytes: %v, expected memory footprint: %v, free memory: %v", fileSizeBytes, int64(float64(fileSizeBytes)*1.25), systemMemoryAvailableBytes)
	}
	return true, nil
}
