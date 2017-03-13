package transfer

import (
	"errors"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fmt"

	"github.com/Azure/blobporter/pipeline"
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
	FileToBlob Definition = "file-blob"
	HTTPToBlob            = "http-blob"
	BlobToFile            = "blob-file"
	HTTPToFile            = "http-file"
	none                  = "none"
)

//ToString converts the enum value to string
func (d *Definition) ToString() string {
	switch *d {
	case FileToBlob:
		return "file-blob"
	case HTTPToBlob:
		return "http-blob"
	case BlobToFile:
		return "blob-file"
	case HTTPToFile:
		return "http-file"
	default:
		return "none"
	}
}

//ParseTransferDefinition parses a Definition from a string.
func ParseTransferDefinition(str string) (Definition, error) {
	val := strings.ToLower(str)
	switch val {
	case "file-blob":
		return FileToBlob, nil
	case "http-blob":
		return HTTPToBlob, nil
	case "blob-file":
		return BlobToFile, nil
	case "http-file":
		return HTTPToFile, nil
	default:
		return none, fmt.Errorf("%v is not a valid transfer definition value.\n Valid values: file-blob, http-blob, blob-file, http-file", str)
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

//Transfer TODO
type Transfer struct {
	SourcePipeline      *pipeline.SourcePipeline
	TargetPipeline      *pipeline.TargetPipeline
	NumOfReaders        int
	NumOfWorkers        int
	TotalNumOfBlocks    int
	TotalSize           uint64
	ThreadTarget        int
	SyncWaitGroups      *WaitGroups
	ControlChannels     *Channels
	Stats               *StatsInfo
	readPartsBufferSize int
}

//StatsInfo TODO
type StatsInfo struct {
	StartTime        time.Time
	Duration         time.Duration
	CumWriteDuration time.Duration
}

//WaitGroups  TODO
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
const extraWorkerBufferSlots = 5
const maxReadPartsBufferSize = 30
const extraThreadTarget = 4

//NewTransfer creates a new Transfer, this will adjust the thread target
//and initialize the channels and the wait groups for the writers, readers and the committers
func NewTransfer(source *pipeline.SourcePipeline, target *pipeline.TargetPipeline, readers int, workers int, blockSize uint64) *Transfer {
	threadTarget := readers + workers + extraThreadTarget
	runtime.GOMAXPROCS(runtime.NumCPU())

	wg := WaitGroups{}
	channels := Channels{}
	t := Transfer{ThreadTarget: threadTarget, NumOfReaders: readers, NumOfWorkers: workers, SourcePipeline: source,
		TargetPipeline: target, SyncWaitGroups: &wg, ControlChannels: &channels, Stats: &StatsInfo{}}

	//Setup the wait groups
	t.SyncWaitGroups.Readers.Add(readers)
	t.SyncWaitGroups.Workers.Add(workers)
	t.SyncWaitGroups.Commits.Add(len((*source).GetSourcesInfo()))

	//Create buffered chanels
	channels.Partitions, channels.Parts, t.TotalNumOfBlocks, t.TotalSize = (*source).ConstructBlockInfoQueue(blockSize)
	readParts := make(chan pipeline.Part, t.getReadPartsBufferSize())
	results := make(chan pipeline.WorkerResult, t.TotalNumOfBlocks)

	channels.ReadParts = readParts
	channels.Results = results

	return &t
}

func (t *Transfer) getReadPartsBufferSize() int {
	if t.readPartsBufferSize == 0 {
		t.readPartsBufferSize = t.NumOfReaders
		if t.readPartsBufferSize > maxReadPartsBufferSize {
			t.readPartsBufferSize = maxReadPartsBufferSize
		}
	}

	return t.readPartsBufferSize
}

//StartTransfer starts the readers and readers. Process and commits the results of the write operations.
//The duration timer is started.
func (t *Transfer) StartTransfer(dupeLevel DupeCheckLevel, progressBarDelegate ProgressUpdate) {

	t.Stats.StartTime = time.Now()

	t.startReaders(t.ControlChannels.Partitions, t.ControlChannels.Parts, t.ControlChannels.ReadParts, t.NumOfReaders, &t.SyncWaitGroups.Readers, t.SourcePipeline)

	t.startWorkers(t.ControlChannels.ReadParts, t.ControlChannels.Results, t.NumOfWorkers, &t.SyncWaitGroups.Workers, dupeLevel, t.TargetPipeline)

	go t.processAndCommitResults(t.ControlChannels.Results, progressBarDelegate, t.TargetPipeline, &t.SyncWaitGroups.Commits)

}

//WaitForCompletion blocks until the readers, writers and commit operations complete.
//Duration property is set and returned.
func (t *Transfer) WaitForCompletion() (time.Duration, time.Duration) {

	t.SyncWaitGroups.Readers.Wait()
	cc := t.ControlChannels
	close((*cc).ReadParts)
	t.SyncWaitGroups.Workers.Wait() // Ensure all upload workers complete
	t.SyncWaitGroups.Commits.Wait() // Ensure all commits complete
	t.Stats.Duration = time.Now().Sub(t.Stats.StartTime)

	return t.Stats.Duration, t.Stats.CumWriteDuration
}

// StartWorkers creates and starts the set of Workers to send data blocks
// from the to the target. Workers are started after waiting workerDelayStarTime.
func (t *Transfer) startWorkers(workerQ chan pipeline.Part, resultQ chan pipeline.WorkerResult, numOfWorkers int, wg *sync.WaitGroup, d DupeCheckLevel, target *pipeline.TargetPipeline) {
	for w := 0; w < numOfWorkers; w++ {
		worker := newWorker(w, workerQ, resultQ, wg, d)
		go worker.startWorker(target)
	}
}

//ProcessAndCommitResults reads from the results channel and calls the target's implementations of the ProcessWrittenPart
// and CommitList, if the last part is received. The update delegate is called as well.
func (t *Transfer) processAndCommitResults(resultQ chan pipeline.WorkerResult, update ProgressUpdate, target *pipeline.TargetPipeline, commitWg *sync.WaitGroup) {

	lists := make(map[string]pipeline.TargetCommittedListInfo)
	blocksProcessed := make(map[string]int)
	committedCount := 0
	var list pipeline.TargetCommittedListInfo
	var numOfBlocks int
	var requeue bool
	var err error
	var workerBufferLevel int

	for {
		result, ok := <-resultQ

		if !ok {
			break
		}

		list = lists[result.SourceURI]
		numOfBlocks = blocksProcessed[result.SourceURI]

		if requeue, err = (*target).ProcessWrittenPart(&result, &list); err == nil {
			lists[result.SourceURI] = list
			if requeue {
				resultQ <- result
			} else {
				numOfBlocks++
				t.Stats.CumWriteDuration = t.Stats.CumWriteDuration + result.Stats.Duration
				blocksProcessed[result.SourceURI] = numOfBlocks
			}
		} else {
			log.Fatal(err)
		}

		if numOfBlocks == result.NumberOfBlocks {
			if _, err := (*target).CommitList(&list, numOfBlocks, result.TargetName); err != nil {
				log.Fatal(err)
			}
			committedCount++
			commitWg.Done()
		}

		workerBufferLevel = int(float64(len(t.ControlChannels.ReadParts)) / float64(t.getReadPartsBufferSize()) * 100)

		// call update delegate
		update(result, committedCount, workerBufferLevel)

	}
}

// StartReaders starts 'n' readers. Each reader is a routine that executes the source's implementations of ExecuteReader.
func (t *Transfer) startReaders(partitionsQ chan pipeline.PartsPartition, partsQ chan pipeline.Part, workerQ chan pipeline.Part, numberOfReaders int, wg *sync.WaitGroup, pipeline *pipeline.SourcePipeline) {
	for i := 0; i < numberOfReaders; i++ {
		go (*pipeline).ExecuteReader(partitionsQ, partsQ, workerQ, i, wg)
	}
}

// startWorker starts a worker that reads from the worker queue channel. Which contains the read parts from the source.
// Calls the target's WritePart implementation and sends the result to the results channel.
func (w *Worker) startWorker(target *pipeline.TargetPipeline) {

	var tb pipeline.Part
	var ok bool
	var duration time.Duration
	var startTime time.Time
	var retries int
	var err error
	var t = (*target)

	for {
		tb, ok = <-w.WorkerQueue

		if !ok { // Work queue has been closed, so done.
			w.Wg.Done()
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
			panic(err)
		}
	}

}
