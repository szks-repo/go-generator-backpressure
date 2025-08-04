package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

func main() {
	ctx := context.Background()
	readFileAndProcessHeavyWithBackPressureN(ctx, 0)
	readFileAndProcessHeavyWithBackPressureN(ctx, 10)
}

func readFileAndProcessHeavyWithBackPressureN(ctx context.Context, n int) {
	n = max(n, 0)
	stream := make(chan RecordsDto, n)
	defer close(stream)

	readCsvAsync(ctx, createTestCsv(10000), stream)

	for s := range stream {
		if len(s.Records) > 0 {
			processHeavy(ctx, s.Records)
		}
		if s.End {
			break
		}
	}
}

func processHeavy(ctx context.Context, records [][]string) {
	slog.Info("processHeavy start")

	time.Sleep(time.Second * 2)

	slog.Info("processHeavy end")
}

type RecordsDto struct {
	Records [][]string
	End     bool
}

func readCsvAsync(
	ctx context.Context,
	file io.Reader,
	ch chan<- RecordsDto,
) {

	go func() {
		var numLoop int
		sr := StreamReader{r: csv.NewReader(file)}
		for {
			numLoop++
			records, hasMore, err := sr.ReadBatch(500)
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					ch <- RecordsDto{End: true}
				}
			}

			select {
			case <-ctx.Done():
				return
			default:
				slog.Info(fmt.Sprintf("readCsvAsync complete: %d", numLoop))
				ch <- RecordsDto{
					Records: records,
					End:     !hasMore,
				}
			}
		}
	}()
}

type StreamReader struct {
	r *csv.Reader
}

func (rs *StreamReader) ReadBatch(n int) ([][]string, bool, error) {
	records := make([][]string, 0, n)
	for {
		record, err := rs.r.Read()
		if err != nil && errors.Is(err, io.EOF) {
			return records, false, nil
		} else if err != nil {
			return nil, false, err
		}

		records = append(records, record)
		if len(records) == n {
			return records, true, nil
		}
	}
}

func createTestCsv(numRecord int) io.Reader {
	buf := new(bytes.Buffer)
	w := csv.NewWriter(buf)

	numRecord = max(numRecord, 1)
	w.Write([]string{"Line", "ID", "Text", "Today"})
	for i := range numRecord {
		w.Write([]string{
			strconv.Itoa(i),
			uuid.Must(uuid.NewV7()).String(),
			strings.Repeat("Hello World", 20),
			time.Now().Format(time.DateTime),
		})
	}
	w.Flush()

	return buf
}
