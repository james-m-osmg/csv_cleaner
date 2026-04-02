package csv_cleaner

import (
	"fmt"
	"strings"
)

type ColumnValueType int

const (
	String ColumnValueType = iota
	Float
	Integer
)

type ColumnDetail struct {
	Name      string
	ValueType ColumnValueType
	Default   any
}

type ParsedCSV[T any] struct {
	FileName string
	Columns  []ColumnDetail
	Rows     []T
}

var typeName = map[ColumnValueType]string{
	String:  "string",
	Float:   "float",
	Integer: "integer",
}

type CleaningDetails struct {
	ColumnRenames   map[string]string
	ColumnSchema    []ColumnDetail
	ColumnsToRemove []string
	ExpectedColumns []string
	ColumnAdditions []ColumnDetail
}

func CleanFile(fileName string, details CleaningDetails) error {

	records, err := ParseFileToStrings(fileName)

	if err != nil {
		fmt.Println("Error:", err)
	}

	newFileName := strings.Replace(fileName, "circana_csv", "circana_csv_testing", 1)
	err = WriteStringsToCSV(records, newFileName)
	if err != nil {
		return fmt.Errorf("write records to csv: %w", err)
	}

	// This file is a work in progress. Below is a bunch of generated slop.

	return nil
	/*
		inputFile, err := os.Open(fileName)
		if err != nil {
			return fmt.Errorf("open %s: %w", fileName, err)
		}
		defer inputFile.Close()

			records, err = ParseFileToRecords(fileName)

			// manual checkbox
			records, err = normalizeHeaders(records, details.ColumnRenames)
			if err != nil {
				return fmt.Errorf("normalize headers: %w", err)
			}

			records, err = dropColumns(records, details.ColumnsToRemove)
			if err != nil {
				return fmt.Errorf("drop columns: %w", err)
			}

			records, err = addColumns(records, details.ColumnAdditions)
			if err != nil {
				return fmt.Errorf("add columns: %w", err)
			}

			records, err = maybeAddEdibleColumn(records, details)
			if err != nil {
				return fmt.Errorf("apply edible column: %w", err)
			}

			if err := validateExpectedColumns(records[0], details.ExpectedColumns); err != nil {
				return err
			}

			finalDF, err := buildDataFrame(records, details.ColumnSchema)
			if err != nil {
				return err
			}

			if len(details.ExpectedColumns) > 0 {
				finalDF = finalDF.Select(details.ExpectedColumns)
				if finalDF.Err != nil {
					return fmt.Errorf("reorder expected columns: %w", finalDF.Err)
				}
			}

			return writeCSVAtomically(fileName, finalDF)
	*/
}

/*
func normalizeHeaders(records [][]string, renames map[string]string) ([][]string, error) {
	if len(records) == 0 {
		return records, nil
	}

	renameLookup := make(map[string]string, len(renames))
	for current, next := range renames {
		renameLookup[normalizeLookupKey(current)] = next
	}

	headers := make([]string, len(records[0]))
	seen := make(map[string]string, len(headers))
	for idx, rawHeader := range records[0] {
		header := strings.TrimSpace(rawHeader)
		if renamed, ok := renameLookup[normalizeLookupKey(header)]; ok {
			header = renamed
		} else {
			header = prettifyHeader(header)
		}

		if header == "" {
			return nil, fmt.Errorf("column %d has an empty name after normalization", idx)
		}
		if existing, ok := seen[header]; ok {
			return nil, fmt.Errorf("duplicate column name %q produced from %q and %q", header, existing, rawHeader)
		}

		seen[header] = rawHeader
		headers[idx] = header
	}

	out := slices.Clone(records)
	out[0] = headers
	return out, nil
}

func dropColumns(records [][]string, columnsToRemove []string) ([][]string, error) {
	if len(columnsToRemove) == 0 {
		return records, nil
	}

	removeSet := make(map[string]struct{}, len(columnsToRemove))
	for _, name := range columnsToRemove {
		removeSet[normalizeLookupKey(name)] = struct{}{}
	}

	keepIndexes := make([]int, 0, len(records[0]))
	for idx, name := range records[0] {
		if _, shouldRemove := removeSet[normalizeLookupKey(name)]; !shouldRemove {
			keepIndexes = append(keepIndexes, idx)
		}
	}

	if len(keepIndexes) == len(records[0]) {
		return records, nil
	}
	if len(keepIndexes) == 0 {
		return nil, fmt.Errorf("cannot remove every column")
	}

	out := make([][]string, len(records))
	for rowIdx, row := range records {
		nextRow := make([]string, len(keepIndexes))
		for i, colIdx := range keepIndexes {
			if colIdx >= len(row) {
				return nil, fmt.Errorf("row %d is missing column %d", rowIdx, colIdx)
			}
			nextRow[i] = row[colIdx]
		}
		out[rowIdx] = nextRow
	}

	return out, nil
}

func addColumns(records [][]string, additions []ColumnWithDefault) ([][]string, error) {
	if len(additions) == 0 {
		return records, nil
	}

	headers := slices.Clone(records[0])
	indexByName := make(map[string]int, len(headers))
	for idx, name := range headers {
		indexByName[normalizeLookupKey(name)] = idx
	}

	out := make([][]string, len(records))
	out[0] = headers
	for rowIdx := 1; rowIdx < len(records); rowIdx++ {
		out[rowIdx] = slices.Clone(records[rowIdx])
	}

	for _, addition := range additions {
		name := strings.TrimSpace(addition.Name)
		if name == "" {
			return nil, fmt.Errorf("column addition has an empty name")
		}

		if _, exists := indexByName[normalizeLookupKey(name)]; exists {
			continue
		}

		out[0] = append(out[0], name)
		value := stringifyDefault(addition.Default)
		for rowIdx := 1; rowIdx < len(out); rowIdx++ {
			out[rowIdx] = append(out[rowIdx], value)
		}
		indexByName[normalizeLookupKey(name)] = len(out[0]) - 1
	}

	return out, nil
}

func maybeAddEdibleColumn(records [][]string, details CleaningDetails) ([][]string, error) {
	const edibleColumn = "Edible or Non Edible"

	if hasColumn(records[0], edibleColumn) {
		return records, nil
	}
	if !requiresColumn(edibleColumn, details.ExpectedColumns, details.ColumnSchema, details.ColumnAdditions) {
		return records, nil
	}

	value := "Non Edible"
	if details.Edible {
		value = "Edible"
	}

	return addColumns(records, []ColumnWithDefault{{Name: edibleColumn, Default: value}})
}

func validateExpectedColumns(headers []string, expected []string) error {
	if len(expected) == 0 {
		return nil
	}

	actualSet := make(map[string]string, len(headers))
	for _, name := range headers {
		actualSet[normalizeLookupKey(name)] = name
	}

	var missing []string
	for _, name := range expected {
		if _, ok := actualSet[normalizeLookupKey(name)]; !ok {
			missing = append(missing, name)
		}
	}

	expectedSet := make(map[string]struct{}, len(expected))
	for _, name := range expected {
		expectedSet[normalizeLookupKey(name)] = struct{}{}
	}

	var unexpected []string
	for _, name := range headers {
		if _, ok := expectedSet[normalizeLookupKey(name)]; !ok {
			unexpected = append(unexpected, name)
		}
	}

	if len(missing) == 0 && len(unexpected) == 0 {
		return nil
	}

	return fmt.Errorf("expected columns mismatch: missing=%v unexpected=%v", missing, unexpected)
}

func buildDataFrame(records [][]string, schema []Column) (dataframe.DataFrame, error) {
	headers := records[0]
	schemaByName := make(map[string]Column, len(schema))
	for _, column := range schema {
		schemaByName[normalizeLookupKey(column.Name)] = column
	}

	if len(schemaByName) > 0 {
		for _, column := range schema {
			if !hasColumn(headers, column.Name) {
				return dataframe.DataFrame{}, fmt.Errorf("schema column %q is missing", column.Name)
			}
		}
	}

	rowCount := len(records) - 1
	cols := make([]series.Series, 0, len(headers))
	for colIdx, header := range headers {
		if spec, ok := schemaByName[normalizeLookupKey(header)]; ok {
			typedSeries, err := buildTypedSeries(records, colIdx, header, spec, rowCount)
			if err != nil {
				return dataframe.DataFrame{}, err
			}
			cols = append(cols, typedSeries)
			continue
		}

		values := make([]string, rowCount)
		for rowIdx := 1; rowIdx < len(records); rowIdx++ {
			values[rowIdx-1] = records[rowIdx][colIdx]
		}
		cols = append(cols, series.New(values, series.String, header))
	}

	df := dataframe.New(cols...)
	if df.Err != nil {
		return dataframe.DataFrame{}, fmt.Errorf("build dataframe: %w", df.Err)
	}
	return df, nil
}

func buildTypedSeries(records [][]string, colIdx int, header string, spec Column, rowCount int) (series.Series, error) {
	switch spec.ValueType {
	case String:
		values := make([]string, rowCount)
		for rowIdx := 1; rowIdx < len(records); rowIdx++ {
			values[rowIdx-1] = records[rowIdx][colIdx]
		}
		return series.New(values, series.String, header), nil
	case Float:
		values := make([]float64, rowCount)
		for rowIdx := 1; rowIdx < len(records); rowIdx++ {
			value := strings.TrimSpace(records[rowIdx][colIdx])
			if value == "" {
				continue
			}
			parsed, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return series.Series{}, fmt.Errorf("column %q row %d: expected %s, got %q", header, rowIdx+1, typeName[spec.ValueType], records[rowIdx][colIdx])
			}
			values[rowIdx-1] = parsed
		}
		return series.New(values, series.Float, header), nil
	case Integer:
		values := make([]int, rowCount)
		for rowIdx := 1; rowIdx < len(records); rowIdx++ {
			value := strings.TrimSpace(records[rowIdx][colIdx])
			if value == "" {
				continue
			}
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return series.Series{}, fmt.Errorf("column %q row %d: expected %s, got %q", header, rowIdx+1, typeName[spec.ValueType], records[rowIdx][colIdx])
			}
			values[rowIdx-1] = parsed
		}
		return series.New(values, series.Int, header), nil
	default:
		return series.Series{}, fmt.Errorf("column %q uses unsupported value type %d", header, spec.ValueType)
	}
}

func writeCSVAtomically(fileName string, df dataframe.DataFrame) error {
	dir := filepath.Dir(fileName)
	tempFile, err := os.CreateTemp(dir, filepath.Base(fileName)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	tempName := tempFile.Name()
	success := false
	defer func() {
		tempFile.Close()
		if !success {
			_ = os.Remove(tempName)
		}
	}()

	if err := df.WriteCSV(tempFile); err != nil {
		return fmt.Errorf("write cleaned csv: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Rename(tempName, fileName); err != nil {
		return fmt.Errorf("replace csv: %w", err)
	}

	success = true
	return nil
}

func hasColumn(headers []string, target string) bool {
	needle := normalizeLookupKey(target)
	for _, header := range headers {
		if normalizeLookupKey(header) == needle {
			return true
		}
	}
	return false
}

func requiresColumn(name string, expected []string, schema []Column, additions []ColumnWithDefault) bool {
	if slices.ContainsFunc(expected, func(candidate string) bool {
		return normalizeLookupKey(candidate) == normalizeLookupKey(name)
	}) {
		return true
	}
	if slices.ContainsFunc(schema, func(candidate Column) bool {
		return normalizeLookupKey(candidate.Name) == normalizeLookupKey(name)
	}) {
		return true
	}
	return slices.ContainsFunc(additions, func(candidate ColumnWithDefault) bool {
		return normalizeLookupKey(candidate.Name) == normalizeLookupKey(name)
	})
}

func stringifyDefault(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func normalizeLookupKey(value string) string {
	var b strings.Builder
	b.Grow(len(value))
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

func prettifyHeader(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, "_", " ")
	return strings.Join(strings.Fields(value), " ")
}
*/
