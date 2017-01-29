package transfer

// blobporter Tool
//
// Copyright (c) Microsoft Corporation
//
// All rights reserved.
//
// MIT License
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.
//

import (
	"errors"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/blobporter/pipeline"
)

const (
	scriptVersion = "2016.11.09.A"
)

// BlockWorker represents a worker routine that transfer data
type BlockWorker struct {
	//Info        *SourceAndTargetInfo
	WorkerID    int
	WorkerQueue *chan pipeline.Part
	Result      *chan pipeline.WorkerResult
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

//SourceTypeStr list of source types
//const SourceTypeStr = "File, HTTP, MFILE (Multifile, input could be a pattern e.g. /data/*.fastq)" //TODO: enum??

// ToString - printable representation of duplicate level values
// ... For now, handle error as fatal, shouldn't be possible
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

// ParseDupeCheckLevel - convert string to DupeCheckLevel
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

///////////////////////////////////////////////////////////////////
//  Readers
///////////////////////////////////////////////////////////////////

// StartReaders - Start 'n' reader "threads"
func StartReaders(partsQ *chan pipeline.Part, workerQ *chan pipeline.Part, numberOfReaders int, wg *sync.WaitGroup, pipeline *pipeline.SourcePipeline) {
	for i := 0; i < numberOfReaders; i++ {
		//go executeReader(blockQ, workerQ, i, wg)
		go (*pipeline).ExecuteReader(partsQ, workerQ, i, wg)
	}
}

///////////////////////////////////////////////////////////////////
//  Writers
///////////////////////////////////////////////////////////////////

// StartWorkers - Create and start the set of Workers to send data blocks
// ... from the worker queue to Azure Storage.
func StartWorkers(workerQ *chan pipeline.Part, resultQ *chan pipeline.WorkerResult, numOfWorkers int, wg *sync.WaitGroup, d DupeCheckLevel, target *pipeline.TargetPipeline) {
	for w := 0; w < numOfWorkers; w++ {
		worker := newWorker(w, workerQ, resultQ, wg, d)
		worker.startWorker(target)
	}
}

// newWorker - creates a new instance of upload Worker
func newWorker(workerID int, workerQueue *chan pipeline.Part,
	resultQueue *chan pipeline.WorkerResult, wg *sync.WaitGroup, d DupeCheckLevel) BlockWorker {
	return BlockWorker{
		//Info:        sti,
		WorkerQueue: workerQueue,
		WorkerID:    workerID,
		Wg:          wg,
		Result:      resultQueue,
		dupeLevel:   d,
	}
}

// startWorker - Helper to start a single upload worker function which will read each
// ... block from WorkerQueue channel and reflect results to the ResultQ channel.
func (w *BlockWorker) startWorker(target *pipeline.TargetPipeline) {
	//var bc storage.BlobStorageClient
	var t = (*target)
	//var blocksHandled = 0

	go func() {
		var tb pipeline.Part
		var ok bool
		for {
			//	fmt.Printf(" Uploader[%d,A] top of loop\n", w.WorkerID)
			//	t0 := time.Now()  //TODO: fix?
			//tb, ok := <-*w.WorkerQueue
			tb, ok = <-*w.WorkerQueue

			if !ok { // Work queue has been closed, so done.
				//	fmt.Printf(" Uploader[%d,D] Done, blocks=%d\n", w.WorkerID, blocksHandled)
				w.Wg.Done()
				return
			}
			//		fmt.Printf(" Uploader[%d,C] BlockID=%v\n", w.WorkerID, tb.BlockId)

			checkForDuplicateChunk(&tb, w.dupeLevel)

			if tb.DuplicateOfBlockOrdinal >= 0 {
				// This block is a duplicate of another, so don't upload it.
				// Instead, just reflect it (with it's "duplicate" status)
				// onwards in the completion channel
				w.recordStatus(&tb, time.Now(), 0, "Success", 0)
				continue
			}

			if duration, startTime, retries, err := t.WritePart(&tb); err == nil {

				w.recordStatus(&tb, startTime, duration, "Success", retries)
			} else {
				panic(err)
			}

		}
	}()
}

// recordStatus -- record the upload status for a completed block to the results Queue.
// ... Can be executed sync or async.
func (w *BlockWorker) recordStatus(tb *pipeline.Part, startTime time.Time, d time.Duration, status string, retries int) {

	*w.Result <- pipeline.WorkerResult{
		BlockSize:               int((*tb).BytesToRead),
		Result:                  status,
		WorkerID:                w.WorkerID,
		ItemID:                  (*tb).BlockID,
		DuplicateOfBlockOrdinal: (*tb).DuplicateOfBlockOrdinal,
		Ordinal:                 (*tb).Ordinal,
		Offset:                  (*tb).Offset,
		StartTime:               startTime,
		Duration:                d,
		Retries:                 retries,
		SourceURI:               (*tb).SourceURI,
		NumberOfBlocks:          (*tb).NumberOfBlocks,
		TargetName:              tb.SourceName}
}

//ProgressUpdate called whenever a worker completes successfully
type ProgressUpdate func(results pipeline.WorkerResult, committedCount int)

//ProcessAndCommitResults TODO
func ProcessAndCommitResults(resultQ *chan pipeline.WorkerResult, update ProgressUpdate, target *pipeline.TargetPipeline, commitWg *sync.WaitGroup) {
	t := (*target)

	go func() {

		lists := make(map[string]pipeline.TargetCommittedListInfo)
		blocksProcessed := make(map[string]int)
		committedCount := 0
		for {
			result, ok := <-(*resultQ)

			if !ok {
				// empty, end of input
				//commitWg.Add(len(lists))
				break
			}

			list := lists[result.SourceURI]
			numOfBlocks := blocksProcessed[result.SourceURI]

			if requeue, err := t.ProcessWrittenPart(&result, &list); err == nil {
				lists[result.SourceURI] = list
				if requeue {
					(*resultQ) <- result
				} else {
					numOfBlocks++
					blocksProcessed[result.SourceURI] = numOfBlocks
				}
			} else {
				log.Fatal(err)
			}

			if numOfBlocks == result.NumberOfBlocks {

				if _, err := t.CommitList(&list, numOfBlocks, result.TargetName); err != nil {
					log.Fatal(err)
				}
				committedCount++
				commitWg.Done()
			}

			// call update delegate
			update(result, committedCount)

		}
	}()
}

//Transfer TODO
type Transfer struct {
	SourcePipeline   *pipeline.SourcePipeline
	TargetPipeline   *pipeline.TargetPipeline
	NumOfReaders     int
	NumOfWorkers     int
	TotalNumOfBlocks int
	TotalSize        uint64
	ThreadTarget     int
	SyncWaitGroups   *WaitGroups
	ControlChannels  *Channels
	StartTime        time.Time
	Duration         time.Duration
}

//WaitGroups  TODO
type WaitGroups struct {
	Readers sync.WaitGroup
	Workers sync.WaitGroup
	Commits sync.WaitGroup
}

//Channels TODO
type Channels struct {
	ReadParts *chan pipeline.Part
	Parts     *chan pipeline.Part
	Results   *chan pipeline.WorkerResult
}

const (
	extraWorkerBufferSlots = 5
)

//NewTransfer Creates a new Transfer, this will adjust the thread target
//and initialize the channels and the wait groups for the writers, readers and the committers
func NewTransfer(source *pipeline.SourcePipeline, target *pipeline.TargetPipeline, readers int, workers int, blockSize uint64) *Transfer {
	threadTarget := readers + workers + 4
	runtime.GOMAXPROCS(threadTarget)

	wg := WaitGroups{}
	channels := Channels{}
	t := Transfer{ThreadTarget: threadTarget, NumOfReaders: readers, NumOfWorkers: workers, SourcePipeline: source,
		TargetPipeline: target, SyncWaitGroups: &wg, ControlChannels: &channels}

	//Setup the wait groups
	t.SyncWaitGroups.Readers.Add(readers)
	t.SyncWaitGroups.Workers.Add(workers)
	t.SyncWaitGroups.Commits.Add(len((*source).GetSourcesInfo()))

	//Create buffered chanels
	channels.Parts, t.TotalNumOfBlocks, t.TotalSize = (*source).ConstructBlockInfoQueue(blockSize)
	readParts := make(chan pipeline.Part, workers+extraWorkerBufferSlots)
	results := make(chan pipeline.WorkerResult, t.TotalNumOfBlocks)

	channels.ReadParts = &readParts
	channels.Results = &results

	return &t
}

//StartTransfer Starts the readers and readers. Process and commits the results of the write operations.
//The duration counter is started.
func (t *Transfer) StartTransfer(dupeLevel DupeCheckLevel, progressBarDelegate ProgressUpdate) {

	t.StartTime = time.Now()

	go StartReaders(t.ControlChannels.Parts, t.ControlChannels.ReadParts, t.NumOfReaders, &t.SyncWaitGroups.Readers, t.SourcePipeline)

	go StartWorkers(t.ControlChannels.ReadParts, t.ControlChannels.Results, t.NumOfWorkers, &t.SyncWaitGroups.Workers, dupeLevel, t.TargetPipeline)

	//no blocking the operation
	ProcessAndCommitResults(t.ControlChannels.Results, progressBarDelegate, t.TargetPipeline, &t.SyncWaitGroups.Commits)

}

//WaitForCompletion  Blocks until the readers, writers and commit operations complete.
//Duration property is set and returned.
func (t *Transfer) WaitForCompletion() time.Duration {
	t.SyncWaitGroups.Readers.Wait()
	close((*(*t.ControlChannels).ReadParts))
	(*t.SyncWaitGroups).Workers.Wait() // Ensure all upload workers complete
	t.SyncWaitGroups.Commits.Wait()    // Ensure all commits complete
	t.Duration = time.Now().Sub(t.StartTime)
	return t.Duration
}
