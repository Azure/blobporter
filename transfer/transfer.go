package transfer

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/blobporter/internal"

	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/util"
)

// Worker represents a worker routine that transfers data to a target
type Worker struct {
	WorkerID    int
	WorkerQueue chan pipeline.Part
	Result      chan pipeline.WorkerResult
	Wg          *sync.WaitGroup
	dupeLevel   DupeCheckLevel
}

// DupeCheckLevel -- degree to which we'll try to check for duplicate blocks
type DupeCheckLevel int

// The different levels for duplicate blocks
const (
	None     DupeCheckLevel = iota // Don't bother to check
	ZeroOnly                       // Only check for blocks with all zero bytes
	Full                           // Compute MD5s and look for matches on that
)

//DupeCheckLevelStr list of duplicate blob check strategies
const DupeCheckLevelStr = "None, ZeroOnly, Full" //TODO: package this better so it can stay in sync w/declaration

// ToString printable representation of duplicate level values.
// For now, handle error as fatal, shouldn't be possible
func (d *DupeCheckLevel) ToString() string {
	var res = "None"
	if *d == ZeroOnly {
		res = "ZeroOnly"
	} else if *d == Full {
		res = "Full"
	} else if *d == None {
		res = "None"
	} else {
		log.Fatalf("Bad value for dupeCheckLevel: %v", *d)
	}
	return res
}

// ParseDupeCheckLevel converts string to DupeCheckLevel
func ParseDupeCheckLevel(str string) (res DupeCheckLevel, err error) {
	var s = strings.ToLower(str)

	if s == "none" {
		res = None
	} else if s == "zeroonly" {
		res = ZeroOnly
	} else if s == "full" {
		res = Full
	} else {
		err = errors.New("Error: DupeCheckLevel must be one of: " + DupeCheckLevelStr)
	}
	return res, err
}

//Definition represents the supported source and target combinations
type Definition string

// The different transfers types
const (
	FileToBlock Definition = "file-blockblob"
	HTTPToBlock            = "http-blockblob"
	BlobToFile             = "blob-file"
	FileToPage             = "file-pageblob"
	HTTPToPage             = "http-pageblob"
	HTTPToFile             = "http-file"
	BlobToBlock            = "blob-blockblob"
	BlobToPage             = "blob-pageblob"
	S3ToBlock              = "s3-blockblob"
	S3ToPage               = "s3-pageblob"
	PerfToBlock            = "perf-blockblob"
	PerfToPage             = "perf-pageblob"
	BlobToPerf             = "blob-perf"
	none                   = "none"
)

//TransferSegment source and target types
type TransferSegment string

//valid transfer segments
const (
	File      TransferSegment = "file"
	HTTP                      = "http"
	BlockBlob                 = "blockblob"
	PageBlob                  = "pageblob"
	S3                        = "s3"
	Perf                      = "perf"
	Blob                      = "blob"
	NA                        = "na"
)

//ParseTransferSegment TODO
func ParseTransferSegment(def Definition) (TransferSegment, TransferSegment) {
	//defstr := string(def)

	if def == none || def == "" {
		return NA, NA
	}

	ref := reflect.ValueOf(def)

	defstr := ref.String()
	segments := strings.Split(defstr, "-")

	source := TransferSegment(segments[0])
	target := TransferSegment(segments[1])

	return source, target
}

//ParseTransferDefinition parses a Definition from a string.
func ParseTransferDefinition(str string) (Definition, error) {
	val := strings.ToLower(str)
	switch val {
	case "file-blob":
		return FileToBlock, nil
	case "file-blockblob":
		return FileToBlock, nil
	case "http-blob":
		return HTTPToBlock, nil
	case "http-blockblob":
		return HTTPToBlock, nil
	case "blob-file":
		return BlobToFile, nil
	case "pageblob-file":
		return BlobToFile, nil
	case "blockblob-file":
		return BlobToFile, nil
	case "http-file":
		return HTTPToFile, nil
	case "file-pageblob":
		return FileToPage, nil
	case "http-pageblob":
		return HTTPToPage, nil
	case "blob-blockblob":
		return BlobToBlock, nil
	case "blob-pageblob":
		return BlobToPage, nil
	case "blob-blob":
		return BlobToBlock, nil
	case "s3-blockblob":
		return S3ToBlock, nil
	case "s3-pageblob":
		return S3ToPage, nil
	case "perf-blockblob":
		return PerfToBlock, nil
	case "perf-pageblob":
		return PerfToPage, nil
	case "blob-perf":
		return BlobToPerf, nil
	default:
		return none, fmt.Errorf("%v is not a valid transfer definition value.\n Valid values: file-blockblob, http-blockblob,file-pageblob, http-pageblob, pageblob-file, blockblob-file, http-file", str)
	}
}

///////////////////////////////////////////////////////////////////
//  Checking for duplicate blocks
///////////////////////////////////////////////////////////////////

var zeroChunkOrdinal int32 = -1 // Remember the first all-zero chunk (-1 == none yet)

// checkForDuplicateChunk - dupeLevel determines how strident we will be in
// ... looking for duplicate chunks.  Set current.DuplicateOfBlockOrdinal to -1 for
// ... no duplicate found, else set it to the ordinal of the equivalent block.
func checkForDuplicateChunk(current *pipeline.Part, dupeLevel DupeCheckLevel) {
	current.DuplicateOfBlockOrdinal = -1

	if current.Data == nil { // can't check because data is not yet present
		return
	}

	if dupeLevel == None {
		return
	}

	if dupeLevel == ZeroOnly { // check for all-zero block
		isZero := true
		for i := 0; i < len(current.Data); i++ { //TODO: confidence in this instead using the hash?  Should also check length
			if (current.Data)[i] != 0 {
				isZero = false
				break
			}
		}

		if !isZero { // non-zero data block, so give up
			return
		}

		if zeroChunkOrdinal < 0 {
			// remember this one as the first zero block
			atomic.CompareAndSwapInt32(&zeroChunkOrdinal, 0, int32(current.Ordinal))
		} else {
			// mark this one as a duplicate of the zero block previously found
			current.DuplicateOfBlockOrdinal = int(zeroChunkOrdinal)
		}
	} else {
		// otherwise it's level == Full, so run a full MD5 and remember the value & ordinal
		var ordinalOfDuplicate = current.LookupMD5DupeOrdinal()
		current.DuplicateOfBlockOrdinal = ordinalOfDuplicate
	}

	// Optimization: if this is a copy of another block, we can discard this data early to save memory
	if current.DuplicateOfBlockOrdinal >= 0 {
		current.Data = nil
	}
}

// newWorker creates a new instance of upload Worker
func newWorker(workerID int, workerQueue chan pipeline.Part,
	resultQueue chan pipeline.WorkerResult, wg *sync.WaitGroup, d DupeCheckLevel) Worker {
	return Worker{
		//Info:        sti,
		WorkerQueue: workerQueue,
		WorkerID:    workerID,
		Wg:          wg,
		Result:      resultQueue,
		dupeLevel:   d}

}

// recordStatus -- record the upload status for a completed block to the results channel.
// ... Can be executed sync or async.
func (w *Worker) recordStatus(tb *pipeline.Part, startTime time.Time, d time.Duration, status string, retries int) {

	var wStats = pipeline.WorkerResultStats{
		StartTime: startTime,
		Duration:  d,
		Retries:   retries}

	tb.Data = nil
	tb.BufferQ = nil

	w.Result <- pipeline.WorkerResult{
		Stats:                   &wStats,
		BlockSize:               int(tb.BytesToRead),
		Result:                  status,
		WorkerID:                w.WorkerID,
		ItemID:                  tb.BlockID,
		DuplicateOfBlockOrdinal: tb.DuplicateOfBlockOrdinal,
		Ordinal:                 tb.Ordinal,
		Offset:                  tb.Offset,
		SourceURI:               tb.SourceURI,
		NumberOfBlocks:          tb.NumberOfBlocks,
		TargetName:              tb.TargetAlias}

}

//ProgressUpdate called whenever a worker completes successfully
type ProgressUpdate func(results pipeline.WorkerResult, committedCount int, bufferSize int)

//Transfer top data structure holding the state of the transfer.
type Transfer struct {
	SourcePipeline       pipeline.SourcePipeline
	TargetPipeline       pipeline.TargetPipeline
	NumOfReaders         int
	NumOfWorkers         int
	TotalNumOfBlocks     int
	TotalSize            uint64
	ThreadTarget         int
	SyncWaitGroups       *WaitGroups
	ControlChannels      *Channels
	TimeStats            *TimeStatsInfo
	tracker              *internal.TransferTracker
	readPartsBufferSize  int
	blockSize            uint64
	totalNumberOfRetries int32
}

//TimeStatsInfo contains transfer statistics, used at the end of the transfer
type TimeStatsInfo struct {
	StartTime        time.Time
	Duration         time.Duration
	CumWriteDuration time.Duration
}

//WaitGroups holds all wait groups involved in the transfer.
type WaitGroups struct {
	Readers sync.WaitGroup
	Workers sync.WaitGroup
	Commits sync.WaitGroup
}

//Channels represents all the control channels in the transfer.
type Channels struct {
	ReadParts  chan pipeline.Part
	Parts      chan pipeline.Part
	Results    chan pipeline.WorkerResult
	Partitions chan pipeline.PartsPartition
}

const workerDelayStarTime = 300 * time.Millisecond
const maxMemReadPartsBufferSize = 500 * util.MB
const extraThreadTarget = 4

//NewTransfer creates a new Transfer, this will adjust the thread target
//and initialize the channels and the wait groups for the writers, readers and the committers
func NewTransfer(source pipeline.SourcePipeline, target pipeline.TargetPipeline, readers int, workers int, blockSize uint64) *Transfer {
	threadTarget := readers + workers + extraThreadTarget
	runtime.GOMAXPROCS(runtime.NumCPU())

	wg := WaitGroups{}
	channels := Channels{}
	t := Transfer{ThreadTarget: threadTarget, NumOfReaders: readers, NumOfWorkers: workers, SourcePipeline: source,
		TargetPipeline: target, SyncWaitGroups: &wg, ControlChannels: &channels,
		TimeStats: &TimeStatsInfo{},
		blockSize: blockSize}

	//validate that all sourceURIs are unique
	sources := source.GetSourcesInfo()

	if s := getFirstDuplicateSource(sources); s != nil {
		log.Fatalf("Invalid input. You can't start a transfer with duplicate sources.\nFirst duplicate detected:%v\n", s)
	}
	//Setup the wait groups
	t.SyncWaitGroups.Readers.Add(readers)
	t.SyncWaitGroups.Workers.Add(workers)
	t.SyncWaitGroups.Commits.Add(1)

	//Create buffered channels
	channels.Partitions, channels.Parts, t.TotalNumOfBlocks, t.TotalSize = source.ConstructBlockInfoQueue(blockSize)
	readParts := make(chan pipeline.Part, t.getReadPartsBufferSize())
	results := make(chan pipeline.WorkerResult, t.TotalNumOfBlocks)

	channels.ReadParts = readParts
	channels.Results = results

	return &t
}
func getFirstDuplicateSource(sources []pipeline.SourceInfo) *pipeline.SourceInfo {
	countMap := make(map[string]int, len(sources))

	for _, item := range sources {
		x := countMap[item.SourceName]
		x++
		if x == 2 {
			return &item
		}
		countMap[item.SourceName] = x
	}

	return nil
}
func (t *Transfer) getReadPartsBufferSize() int {
	if t.readPartsBufferSize == 0 {
		t.readPartsBufferSize = t.NumOfReaders
		maxReadBufferSize := int(maxMemReadPartsBufferSize / t.blockSize)
		if t.readPartsBufferSize > maxReadBufferSize {
			t.readPartsBufferSize = maxReadBufferSize
		}
	}
	return t.readPartsBufferSize
}

//StartTransfer starts the readers and readers. Process and commits the results of the write operations.
//The duration timer is started.
func (t *Transfer) StartTransfer(dupeLevel DupeCheckLevel, progressBarDelegate ProgressUpdate) {
	t.preprocessSources()

	t.TimeStats.StartTime = time.Now()

	t.startReaders(t.ControlChannels.Partitions, t.ControlChannels.Parts, t.ControlChannels.ReadParts, t.NumOfReaders, &t.SyncWaitGroups.Readers, t.SourcePipeline)

	t.startWorkers(t.ControlChannels.ReadParts, t.ControlChannels.Results, t.NumOfWorkers, &t.SyncWaitGroups.Workers, dupeLevel, t.TargetPipeline)

	go t.processAndCommitResults(t.ControlChannels.Results, progressBarDelegate, t.TargetPipeline, &t.SyncWaitGroups.Commits)
}

//Concurrely calls the PreProcessSourceInfo implementation of the target pipeline for each source in the transfer.
func (t *Transfer) preprocessSources() {
	sourcesInfo := t.SourcePipeline.GetSourcesInfo()
	var wg sync.WaitGroup
	wg.Add(len(sourcesInfo))

	for i := 0; i < len(sourcesInfo); i++ {
		go func(s *pipeline.SourceInfo, b uint64) {
			defer wg.Done()
			if err := t.TargetPipeline.PreProcessSourceInfo(s, b); err != nil {
				log.Fatal(err)
			}
		}(&sourcesInfo[i], t.blockSize)
	}

	wg.Wait()
}

//WaitForCompletion blocks until the readers, writers and commit operations complete.
//Duration property is set and returned.
func (t *Transfer) WaitForCompletion() (time.Duration, time.Duration) {

	t.SyncWaitGroups.Readers.Wait()
	close(t.ControlChannels.ReadParts)
	t.SyncWaitGroups.Workers.Wait() // Ensure all upload workers complete
	close(t.ControlChannels.Results)
	t.SyncWaitGroups.Commits.Wait() // Ensure all commits complete
	t.TimeStats.Duration = time.Now().Sub(t.TimeStats.StartTime)
	return t.TimeStats.Duration, t.TimeStats.CumWriteDuration
}

//GetStats returns the statistics of the transfer.
func (t *Transfer) GetStats() *StatInfo {
	return &StatInfo{
		NumberOfFiles:       len(t.SourcePipeline.GetSourcesInfo()),
		Duration:            t.TimeStats.Duration,
		CumWriteDuration:    t.TimeStats.CumWriteDuration,
		TotalNumberOfBlocks: t.TotalNumOfBlocks,
		TargetRetries:       t.totalNumberOfRetries,
		TotalSize:           t.TotalSize}
}

// StartWorkers creates and starts the set of Workers to send data blocks
// from the to the target. Workers are started after waiting workerDelayStarTime.
func (t *Transfer) startWorkers(workerQ chan pipeline.Part, resultQ chan pipeline.WorkerResult, numOfWorkers int, wg *sync.WaitGroup, d DupeCheckLevel, target pipeline.TargetPipeline) {
	for w := 0; w < numOfWorkers; w++ {
		worker := newWorker(w, workerQ, resultQ, wg, d)
		go worker.startWorker(target)
	}
}

//ProcessAndCommitResults reads from the results channel and calls the target's implementations of the ProcessWrittenPart
// and CommitList, if the last part is received. The update delegate is called as well.
func (t *Transfer) processAndCommitResults(resultQ chan pipeline.WorkerResult, update ProgressUpdate, target pipeline.TargetPipeline, commitWg *sync.WaitGroup) {

	lists := make(map[string]pipeline.TargetCommittedListInfo)
	blocksProcessed := make(map[string]int)
	committedCount := 0
	var list pipeline.TargetCommittedListInfo
	var numOfBlocks int
	var requeue bool
	var err error
	var workerBufferLevel int
	defer commitWg.Done()

	for {
		result, ok := <-resultQ

		if !ok {
			break
		}

		list = lists[result.SourceURI]
		numOfBlocks = blocksProcessed[result.SourceURI]

		if requeue, err = target.ProcessWrittenPart(&result, &list); err == nil {
			lists[result.SourceURI] = list
			if requeue {
				resultQ <- result
			} else {
				numOfBlocks++
				t.TimeStats.CumWriteDuration = t.TimeStats.CumWriteDuration + result.Stats.Duration
				t.totalNumberOfRetries = t.totalNumberOfRetries + int32(result.Stats.Retries)
				blocksProcessed[result.SourceURI] = numOfBlocks
			}
		} else {
			log.Fatal(err)
		}

		if numOfBlocks == result.NumberOfBlocks {
			if _, err := target.CommitList(&list, numOfBlocks, result.TargetName); err != nil {
				log.Fatal(err)
			}
			committedCount++
			if t.tracker != nil {
				if err := t.tracker.TrackFileTransferComplete(result.TargetName); err != nil {
					log.Fatal(err)
				}
			}
		}

		workerBufferLevel = int(float64(len(t.ControlChannels.ReadParts)) / float64(t.getReadPartsBufferSize()) * 100)
		// call update delegate
		update(result, committedCount, workerBufferLevel)
	}
}

//SetTransferTracker TODO
func (t *Transfer) SetTransferTracker(tracker *internal.TransferTracker) {
	t.tracker = tracker
}

// StartReaders starts 'n' readers. Each reader is a routine that executes the source's implementations of ExecuteReader.
func (t *Transfer) startReaders(partitionsQ chan pipeline.PartsPartition, partsQ chan pipeline.Part, workerQ chan pipeline.Part, numberOfReaders int, wg *sync.WaitGroup, pipeline pipeline.SourcePipeline) {
	for i := 0; i < numberOfReaders; i++ {
		go pipeline.ExecuteReader(partitionsQ, partsQ, workerQ, i, wg)
	}
}

// startWorker starts a worker that reads from the worker queue channel. Which contains the read parts from the source.
// Calls the target's WritePart implementation and sends the result to the results channel.
func (w *Worker) startWorker(target pipeline.TargetPipeline) {
	var tb pipeline.Part
	var ok bool
	var duration time.Duration
	var startTime time.Time
	var retries int
	var err error
	var t = target
	defer w.Wg.Done()
	for {
		tb, ok = <-w.WorkerQueue

		if !ok { // Work queue has been closed, so done.
			return
		}

		checkForDuplicateChunk(&tb, w.dupeLevel)

		if tb.DuplicateOfBlockOrdinal >= 0 {
			// This block is a duplicate of another, so don't upload it.
			// Instead, just reflect it (with it's "duplicate" status)
			// onwards in the completion channel
			w.recordStatus(&tb, time.Now(), 0, "Success", 0)
			continue
		}
		if duration, startTime, retries, err = t.WritePart(&tb); err == nil {
			tb.ReturnBuffer()
			w.recordStatus(&tb, startTime, duration, "Success", retries)
		} else {
			log.Fatal(err)
		}
	}

}
