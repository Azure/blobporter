package main

import (
	"fmt"
	"math"
	"net/url"
	"os"
	"time"

	"github.com/Azure/blobporter/internal"
	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/transfer"
	"github.com/Azure/blobporter/util"
)

type progressState struct {
	quietMode      bool
	numOfReaders   int
	numOfWorkers   int
	bufferLevel    int
	committedCount int
	processedData  float64
	global         globalState
}

type globalState struct {
	startTime         time.Time
	cumWriteDuration  time.Duration
	cumDataSize       float64
	cumCommittedCount int
	cumNumOfBlocks    int64
}

var progress = &progressState{}

func newProgressState(quietMode bool, numOfReaders int, numOfWorkers int) *progressState {
	return &progressState{quietMode: quietMode,
		numOfReaders: numOfReaders,
		numOfWorkers: numOfWorkers,
		global: globalState{
			startTime: time.Now(),
		},
	}
}
func (p *progressState) reset() {
	p.bufferLevel = 0
	p.committedCount = 0
	p.processedData = 0
}

func (p *progressState) resetTransferState() {
	p.bufferLevel = 0
	p.committedCount = 0
	p.processedData = 0
}

func (p *progressState) newTransfer(totalSize float64, sourcesInfo []pipeline.SourceInfo, transferType transfer.Definition) {

	//init event sink
	//only reset the transfer counters and keep the globalones
	p.resetTransferState()
	p.addEndOfTransferDelegate()
	p.displayFilesToTransfer(sourcesInfo, transferType)
	if p.quietMode {
		return
	}

	p.addProgressBarDelegate(totalSize)
}

func (p *progressState) addEndOfTransferDelegate() {
	endTransDelegate := func(e internal.EventItem, a internal.EventItemAggregate) {
		switch e.Name {
		case transfer.DataWrittenEvent:
			p.global.cumDataSize += a.Value
		case transfer.WrittenPartEvent:
			p.global.cumNumOfBlocks += int64(a.Value)
		case transfer.WriteDurationEvent:
			p.global.cumWriteDuration += time.Duration(int64(a.Value))
		case transfer.CommitEvent:
			p.global.cumCommittedCount += int(a.Value)
		default:
			return
		}
	}

	internal.EventSink.AddSubscription(internal.CommitListHandler, internal.OnDone, endTransDelegate)
	internal.EventSink.AddSubscription(internal.Worker, internal.OnDone, endTransDelegate)
}
func (p *progressState) displayFilesToTransfer(sourcesInfo []pipeline.SourceInfo, transferType transfer.Definition) {
	fmt.Printf("\nFiles to Transfer (%v) :\n", transferType)
	var totalSize uint64
	summary := ""

	for _, source := range sourcesInfo {
		//if the source is URL, remove the QS
		display := source.SourceName
		if u, err := url.Parse(source.SourceName); err == nil {
			display = fmt.Sprintf("%v%v", u.Hostname(), u.Path)
		}
		summary = summary + fmt.Sprintf("Source: %v Size:%v \n", display, source.Size)
		totalSize = totalSize + source.Size
	}

	if len(sourcesInfo) < 10 {
		fmt.Printf(summary)
		return
	}

	fmt.Printf("%v files. Total size:%v\n", len(sourcesInfo), totalSize)

	return
}
func (p *progressState) displayGlobalSummary() {
	var netMB float64 = 1000000
	duration := time.Now().Sub(p.global.startTime)
	fmt.Printf("\nThe data transfer took %v to run.\n", duration)
	MBs := float64(p.global.cumDataSize) / netMB / duration.Seconds()
	fmt.Printf("Throughput: %1.2f MB/s (%1.2f Mb/s) \n", MBs, MBs*8)
	fmt.Printf("Configuration: R=%d, W=%d, DataSize=%s, Blocks=%d\n",
		p.numOfReaders, p.numOfWorkers, util.PrintSize(uint64(p.global.cumDataSize)), p.global.cumNumOfBlocks)
	fmt.Printf("Cumulative Writes Duration: Total=%v, Avg Per Worker=%v\n",
		p.global.cumWriteDuration, time.Duration(p.global.cumWriteDuration.Nanoseconds()/int64(p.numOfWorkers)))
}
func (p *progressState) addProgressBarDelegate(totalSize float64) {

	progressDelegate := func(e internal.EventItem, a internal.EventItemAggregate) {
		var pp int
		switch e.Name {
		case transfer.BufferEvent:
			p.bufferLevel = e.Data[0].Value.(int)
		case transfer.DataWrittenEvent:
			p.processedData = a.Value
		case transfer.CommitEvent:
			p.committedCount = int(a.Value)
		default:
			return
		}
		pp = int(math.Ceil((p.processedData / totalSize) * 100))
		var ind string
		var pchar string
		for i := 0; i < 25; i++ {
			if i+1 > pp/4 {
				pchar = "."
			} else {
				pchar = "|"
			}

			ind = ind + pchar
		}
		if !util.Verbose {
			fmt.Fprintf(os.Stdout, "\r --> %3d %% [%v] Committed Count: %v Buffer Level: %03d%%", pp, ind, p.committedCount, p.bufferLevel)
		}
	}

	//Buffer and WrittenPart event are signaled by the worker. Commit event by the comitter
	internal.EventSink.AddSubscription(internal.CommitListHandler, internal.RealTime, progressDelegate)
	internal.EventSink.AddSubscription(internal.Worker, internal.RealTime, progressDelegate)
}
