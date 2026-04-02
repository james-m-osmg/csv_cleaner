package csv_cleaner

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestWriteStringsToCSV_WritesReadableCSV(t *testing.T) {
	records := [][]string{
		{"id", "name", "city", "score", "notes"},
		{"1", "Alice", "New York", "98", "top performer"},
		{"2", "Bob", "Chicago", "87", "contains,comma"},
		{"3", "Carol", "", "91", "missing city"},
	}

	outputPath := filepath.Join(t.TempDir(), "output.csv")

	if err := WriteStringsToCSV(records, outputPath); err != nil {
		t.Fatalf("WriteStringsToCSV returned error: %v", err)
	}

	file, err := os.Open(outputPath)
	if err != nil {
		t.Fatalf("failed to open written csv: %v", err)
	}
	defer file.Close()

	got, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatalf("failed to read written csv: %v", err)
	}

	if !reflect.DeepEqual(got, records) {
		t.Fatalf("written csv mismatch\nwant: %#v\ngot:  %#v", records, got)
	}
}
