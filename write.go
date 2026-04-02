package csv_cleaner

import (
	"encoding/csv"
	"os"
)

func WriteStringsToCSV(records [][]string, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, record := range records {
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	return nil
}
