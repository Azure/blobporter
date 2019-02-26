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

//ProgressUpdate called whenever a worker completes successfully
//type ProgressUpdate func(results pipeline.WorkerResult, committedCount int, bufferSize int)

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
	tracker              *internal.TransferTracker
	readPartsBufferSize  int
	blockSize            uint64
	totalNumberOfRetries int32
	commitListHandler    *commitListHandler
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
	Partitions chan pipeline.PartsPartition
}

const workerDelayStarTime = 300 * time.Millisecond
const maxMemReadPartsBufferSize = 500 * util.MB
const extraThreadTarget = 4
const numberOfCommitters = 4

//NewTransfer creates a new Transfer, this will adjust the thread target
//and initialize the channels and the wait groups for the writers, readers and the committers
func NewTransfer(source pipeline.SourcePipeline, target pipeline.TargetPipeline, readers int, workers int, blockSize uint64) *Transfer {
	threadTarget := readers + workers + extraThreadTarget
	runtime.GOMAXPROCS(runtime.NumCPU())

	wg := WaitGroups{}
	channels := Channels{}
	t := Transfer{ThreadTarget: threadTarget,
		NumOfReaders:    readers,
		NumOfWorkers:    workers,
		SourcePipeline:  source,
		TargetPipeline:  target,
		SyncWaitGroups:  &wg,
		ControlChannels: &channels,
		blockSize:       blockSize,
	}

	//validate that all sourceURIs are unique
	sources := source.GetSourcesInfo()

	if s := getFirstDuplicateSource(sources); s != nil {
		log.Fatalf("Invalid input. You can't start a transfer with duplicate sources.\nFirst duplicate detected:%v\n", s)
	}
	//Setup the wait groups
	t.SyncWaitGroups.Readers.Add(readers)
	t.SyncWaitGroups.Workers.Add(workers)
	t.SyncWaitGroups.Commits.Add(numberOfCommitters)

	//Create buffered channels
	channels.Partitions, channels.Parts, t.TotalNumOfBlocks, t.TotalSize = source.ConstructBlockInfoQueue(blockSize)
	readParts := make(chan pipeline.Part, t.getReadPartsBufferSize())

	channels.ReadParts = readParts

	internal.EventSink.Reset()

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
func (t *Transfer) StartTransfer(dupeLevel DupeCheckLevel) {
	t.preprocessSources()

	t.startReaders(t.ControlChannels.Partitions, t.ControlChannels.Parts, t.ControlChannels.ReadParts, t.NumOfReaders, &t.SyncWaitGroups.Readers, t.SourcePipeline)

	t.startWorkersAndCommitters(dupeLevel)

	return

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
func (t *Transfer) WaitForCompletion() time.Time {

	t.SyncWaitGroups.Readers.Wait()
	close(t.ControlChannels.ReadParts)
	t.SyncWaitGroups.Workers.Wait() // Ensure all upload workers complete
	t.commitListHandler.setDone()
	t.commitListHandler.Wait()
	t.SyncWaitGroups.Commits.Wait()
	done := time.Now()
	internal.EventSink.FlushAndWait()
	return done
}

// startWorkersAndCommitters creates and starts the Workers (writers) and the committers
func (t *Transfer) startWorkersAndCommitters(d DupeCheckLevel) {
	t.commitListHandler = newCommitListHandler(t.tracker, t.TargetPipeline)
	for w := 0; w < t.NumOfWorkers; w++ {
		worker := newWorker(w,
			t.ControlChannels.ReadParts,
			t.commitListHandler,
			&t.SyncWaitGroups.Workers,
			d)
		go worker.startWorker(t.TargetPipeline)
	}

	for c := 0; c < numberOfCommitters; c++ {
		committer := newCommitter(t.commitListHandler, &t.SyncWaitGroups.Commits)
		go committer.startCommitter(t.TargetPipeline)
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
