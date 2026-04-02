package csv_cleaner

import (
	"fmt"
	"slices"
)

func RenameColumns(records [][]string, renames map[string]string) [][]string {
	if len(records) == 0 {
		return records
	}
	columns := records[0]
	for i, col := range columns {
		if newName, ok := renames[col]; ok {
			columns[i] = newName
		}
	}
	records[0] = columns
	return records
}

func RemoveColumns(records [][]string, columnsToRemove []string) [][]string {
	if len(records) == 0 {
		return records
	}

	columns := records[0]

	columnIdxToRemove := []int{}
	for idx, col := range columns {
		if slices.Contains(columnsToRemove, col) {
			columnIdxToRemove = append(columnIdxToRemove, idx)
		}
	}

	filteredRecords := [][]string{}

	for _, record := range records {
		newRecord := []string{}
		for i, field := range record {
			if !slices.Contains(columnIdxToRemove, i) {
				newRecord = append(newRecord, field)
			}
		}
		filteredRecords = append(filteredRecords, newRecord)
	}
	return filteredRecords
}

type RecordMap map[string]string // column[]

type NewColumn struct {
	// generator will be passed the entire row
	// as a map of column names and their field values
	Name           string
	DetermineValue func(RecordMap) (string, error)
}

func AddColumns(records [][]string, newColumns []NewColumn) ([][]string, error) {

	if len(records) == 0 {
		return records, nil
	}

	columns := records[0]

	newRecords := [][]string{}

	for idx, record := range records {
		if idx == 0 {
			newColumnRecord := record
			for _, n := range newColumns {
				newColumnRecord = append(newColumnRecord, n.Name)
			}
			newRecords = append(newRecords, newColumnRecord)
			continue
		}

		recMap, err := generateRecordMap(columns, record)
		if err != nil {
			return [][]string{}, fmt.Errorf("could not generate record map for record at index: %v error: %s", idx, error.Error)
		}

		newRecord := record
		for _, newCol := range newColumns {
			v, err := newCol.DetermineValue(recMap)
			if err != nil {
				return [][]string{}, fmt.Errorf("could not generate value for record with generator. error: %s", err)
			}
			newRecord = append(newRecord, v)
		}
		newRecords = append(newRecords, newRecord)
	}
	return newRecords, nil
}

func generateRecordMap(columns []string, record []string) (RecordMap, error) {

	if len(columns) != len(record) {
		return RecordMap{}, fmt.Errorf("column length did not match row length")
	}

	recMap := RecordMap{}

	for idx, col := range columns {
		recMap[col] = record[idx]
	}

	return recMap, nil
}
