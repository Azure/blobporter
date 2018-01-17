package transfer

import (
	"fmt"
	"time"

	"github.com/Azure/blobporter/util"
)

//Stats statistics from the transfer
type Stats struct {
	workers int
	readers int
	values  []StatInfo
}

//StatInfo holds calculated statistics from a transfer
type StatInfo struct {
	NumberOfFiles       int
	Duration            time.Duration
	TargetRetries       int32
	TotalNumberOfBlocks int
	TotalSize           uint64
	CumWriteDuration    time.Duration
}

//NewStats creates a new instance of the
func NewStats(numberOfWorkers int, numberOfReaders int) *Stats {
	return &Stats{workers: numberOfWorkers, readers: numberOfReaders, values: make([]StatInfo, 0)}
}

//AddTransferInfo adds final stastistics info the list
func (t *Stats) AddTransferInfo(info *StatInfo) {
	t.values = append(t.values, (*info))
}

func (t *Stats) calcAggretagedValues() *StatInfo {
	agg := StatInfo{}

	for _, r := range t.values {
		agg.NumberOfFiles = agg.NumberOfFiles + r.NumberOfFiles
		agg.TargetRetries = agg.TargetRetries + r.TargetRetries
		agg.TotalNumberOfBlocks = agg.TotalNumberOfBlocks + r.TotalNumberOfBlocks
		agg.TotalSize = agg.TotalSize + r.TotalSize
		agg.Duration = time.Duration(agg.Duration.Nanoseconds() + r.Duration.Nanoseconds())
		agg.CumWriteDuration = time.Duration(agg.CumWriteDuration.Nanoseconds() + r.CumWriteDuration.Nanoseconds())
	}

	return &agg
}

//DisplaySummary displays the final statistics for the transfer
func (t *Stats) DisplaySummary() {

	agg := t.calcAggretagedValues()

	var netMB float64 = 1000000
	fmt.Printf("\nThe data transfer took %v to run.\n", agg.Duration)
	MBs := float64(agg.TotalSize) / netMB / agg.Duration.Seconds()
	fmt.Printf("Throughput: %1.2f MB/s (%1.2f Mb/s) \n", MBs, MBs*8)
	fmt.Printf("Configuration: R=%d, W=%d, DataSize=%s, Blocks=%d\n",
		t.readers, t.workers, util.PrintSize(agg.TotalSize), agg.TotalNumberOfBlocks)
	fmt.Printf("Cumulative Writes Duration: Total=%v, Avg Per Worker=%v\n",
		agg.CumWriteDuration, time.Duration(agg.CumWriteDuration.Nanoseconds()/int64(t.workers)))
	fmt.Printf("Retries: Avg=%v Total=%v\n", float32(agg.TargetRetries)/float32(agg.TotalNumberOfBlocks), agg.TargetRetries)
}
