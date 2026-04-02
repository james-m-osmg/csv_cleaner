package csv_cleaner

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestAddColumns_AppendsGeneratedColumns(t *testing.T) {
	records := [][]string{
		{"id", "name", "city"},
		{"1", "Alice", "New York"},
		{"2", "Bob", "Chicago"},
	}

	newColumns := []NewColumn{
		{
			Name: "name_city",
			DetermineValue: func(record RecordMap) (string, error) {
				return record["name"] + "-" + record["city"], nil
			},
		},
		{
			Name: "name_len",
			DetermineValue: func(record RecordMap) (string, error) {
				return string(rune(len(record["name"]) + '0')), nil
			},
		},
	}

	got, err := AddColumns(records, newColumns)
	if err != nil {
		t.Fatalf("AddColumns returned error: %v", err)
	}

	want := [][]string{
		{"id", "name", "city", "name_city", "name_len"},
		{"1", "Alice", "New York", "Alice-New York", "5"},
		{"2", "Bob", "Chicago", "Bob-Chicago", "3"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("AddColumns mismatch\nwant: %#v\ngot:  %#v", want, got)
	}
}

func TestAddColumns_ReturnsErrorWhenGeneratorFails(t *testing.T) {
	records := [][]string{
		{"id", "name"},
		{"1", "Alice"},
	}

	_, err := AddColumns(records, []NewColumn{
		{
			Name: "broken",
			DetermineValue: func(record RecordMap) (string, error) {
				return "", errors.New("boom")
			},
		},
	})
	if err == nil {
		t.Fatal("expected AddColumns to return an error")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected generator error to be wrapped, got: %v", err)
	}
}

func TestRenameColumns_RenamesHeaderOnly(t *testing.T) {
	records := [][]string{
		{"id", "name", "city"},
		{"1", "Alice", "New York"},
		{"2", "Bob", "Chicago"},
	}

	got := RenameColumns(records, map[string]string{
		"name": "full_name",
		"city": "location",
	})

	want := [][]string{
		{"id", "full_name", "location"},
		{"1", "Alice", "New York"},
		{"2", "Bob", "Chicago"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RenameColumns mismatch\nwant: %#v\ngot:  %#v", want, got)
	}
}

func TestRemoveColumns_RemovesRequestedColumnsFromAllRecords(t *testing.T) {
	records := [][]string{
		{"id", "name", "city", "score"},
		{"1", "Alice", "New York", "98"},
		{"2", "Bob", "Chicago", "87"},
	}

	got := RemoveColumns(records, []string{"city", "score"})

	want := [][]string{
		{"id", "name"},
		{"1", "Alice"},
		{"2", "Bob"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RemoveColumns mismatch\nwant: %#v\ngot:  %#v", want, got)
	}
}
