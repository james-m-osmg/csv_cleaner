package csv_cleaner

import (
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func TestParseFileToStrings_WithTestingCSV(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to determine test file path")
	}

	fileName := filepath.Join(filepath.Dir(thisFile), "Testing.csv")

	got, err := ParseFileToStrings(fileName)
	if err != nil {
		t.Fatalf("ParseFileToStrings returned error: %v", err)
	}

	want := [][]string{
		{"id", "name", "city", "score", "notes"},
		{"1", "Alice", "New York", "98", "top performer"},
		{"2", "Bob", "Chicago", "87", "has empty middle field next row"},
		{"3", "Carol", "", "91", "missing city"},
		{"4", "Dan", "San Francisco", "100", "contains spaces"},
		{"5", "Eve", "Boston", "0", ""},
		{"6", "Frank", "Miami", "72", "C:\\\\temp\\\\report.txt"},
		{"7", "Grace", "Seattle", "88", "plain ascii only"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseFileToStrings mismatch\nwant: %#v\ngot:  %#v", want, got)
	}
}
