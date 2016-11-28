package blocktransfer

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
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/util"
)

const (
	BlockSizeMax  = 4 * util.MB // no block blob blocks larger than this
	scriptVersion = "2016.11.09.A"
)

//SourceAndTargetInfo -
type SourceAndTargetInfo struct {
	SourceURI           string
	TargetContainerName string
	TargetBlobName      string
	TargetCredentials   *StorageAccountCredentials
	IdealBlockSize      uint32 // Azure storage block norm, usually 4MB in bytes
}

// StorageAccountCredentials - a central location for account info.
type StorageAccountCredentials struct {
	AccountName string // short name of the storage account.  e.g., mystore
	AccountKey  string // Base64-encoded storage account key
}

// BlockWorker represents a worker routine that transfer data
type BlockWorker struct {
	Info        *SourceAndTargetInfo
	WorkerID    int
	WorkerQueue *chan pipeline.Part
	Result      *chan WorkerResult
	Wg          *sync.WaitGroup
	dupeLevel   DupeCheckLevel
}

// WorkerResult represents the result of a single block upload
type WorkerResult struct {
	BlockSize               int
	Result                  string //TODO: could cut down on the number of members
	WorkerID                int
	Duration                time.Duration
	StartTime               time.Time
	ItemID                  string
	DuplicateOfBlockOrdinal int
	Ordinal                 int
	Offset                  uint64
}

// getBlobStorageClient - internal utility function to get a properly-primed
// ... Azure Storage SDK Blob Client.
func getBlobStorageClient(creds *StorageAccountCredentials) storage.BlobStorageClient {
	var bc storage.BlobStorageClient
	var client storage.Client
	var err error
	var accountName string
	var accountKey string

	if creds != nil {
		accountName = creds.AccountName
		accountKey = creds.AccountKey
	} else {
		accountName = os.Getenv("ACCOUNT_NAME") //TODO: clean up.. should always be in creds at this point
		accountKey = os.Getenv("ACCOUNT_KEY")
	}

	if accountName == "" || accountKey == "" {
		log.Fatal("Storage account and/or key not specified via options or in environment variables ACCOUNT_NAME and ACCOUNT_KEY")
	}

	if client, err = storage.NewBasicClient(accountName, accountKey); err != nil {
		log.Fatal(err)
	}

	bc = client.GetBlobService()
	return bc
}

// DupeCheckLevel -- degree to which we'll try to check for duplicate blocks
type DupeCheckLevel int

// The different levels for duplicate blocks
const (
	None     DupeCheckLevel = iota // Don't bother to check
	ZeroOnly                       // Only check for blocks with all zero bytes
	Full                           // Compute MD5s and look for matches on that
)
const DupeCheckLevelStr = "None, ZeroOnly, Full" //TODO: package this better so it can stay in sync w/declaration

const SourceTypeStr = "File, HTTP" //TODO: enum??

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
			if current.Data[i] != 0 {
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
//  Input File Reading/Processing
///////////////////////////////////////////////////////////////////

// StartReaders - Start 'n' reader "threads"
func StartReaders(partsQ *chan pipeline.Part, workerQ *chan pipeline.Part, numberOfReaders int, wg *sync.WaitGroup, pipeline *pipeline.SourcePipeline) {
	for i := 0; i < numberOfReaders; i++ {
		//go executeReader(blockQ, workerQ, i, wg)
		go (*pipeline).ExecuteReader(partsQ, workerQ, i, wg)
	}
}

///////////////////////////////////////////////////////////////////
//  Block Uploading to Azure Storage
///////////////////////////////////////////////////////////////////

// StartWorkers - Create and start the set of Workers to send data blocks
// ... from the worker queue to Azure Storage.
func StartWorkers(workerQ *chan pipeline.Part, resultQ *chan WorkerResult, numOfWorkers int, wg *sync.WaitGroup, d DupeCheckLevel, sti *SourceAndTargetInfo) {
	for w := 0; w < numOfWorkers; w++ {
		worker := newWorker(w, workerQ, resultQ, wg, d, sti)
		worker.startWorker()
	}
}

// newWorker - creates a new instance of upload Worker
func newWorker(workerID int, workerQueue *chan pipeline.Part,
	resultQueue *chan WorkerResult, wg *sync.WaitGroup, d DupeCheckLevel,
	sti *SourceAndTargetInfo) BlockWorker {
	return BlockWorker{
		Info:        sti,
		WorkerQueue: workerQueue,
		WorkerID:    workerID,
		Wg:          wg,
		Result:      resultQueue,
		dupeLevel:   d,
	}
}

// startWorker - Helper to start a single upload worker function which will read each
// ... block from WorkerQueue channel and reflect results to the ResultQ channel.
func (w *BlockWorker) startWorker() {
	var bc storage.BlobStorageClient
	var blocksHandled = 0

	go func() {
		for {
			//	fmt.Printf(" Uploader[%d,A] top of loop\n", w.WorkerID)
			//	t0 := time.Now()  //TODO: fix?
			tb, ok := <-*w.WorkerQueue

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
				w.recordStatus(tb, time.Now(), 0, "Success")
				continue
			}

			// otherwise, actually kick off an upload of the block.
			bc = getBlobStorageClient(w.Info.TargetCredentials)

			util.RetriableOperation(func() error {
				t0 := time.Now()
				if err := bc.PutBlock(w.Info.TargetContainerName, w.Info.TargetBlobName, tb.BlockID, tb.Data); err != nil {
					if util.Verbose {
						data, _ := base64.StdEncoding.DecodeString(tb.BlockID)
						fmt.Println(string(data))
						fmt.Print(err.Error())
					}
					bc = getBlobStorageClient(w.Info.TargetCredentials) // reset to a fresh client for the retry
					return err
				}
				t1 := time.Now()
				blocksHandled++
				w.recordStatus(tb, t0, t1.Sub(t0), "Success")
				return nil
			})
		}
	}()
}

// recordStatus -- record the upload status for a completed block to the results Queue.
// ... Can be executed sync or async.
func (w *BlockWorker) recordStatus(tb pipeline.Part, startTime time.Time, d time.Duration, status string) {
	*w.Result <- WorkerResult{
		BlockSize:               int(tb.BytesToRead),
		Result:                  status,
		WorkerID:                w.WorkerID,
		ItemID:                  tb.BlockID,
		DuplicateOfBlockOrdinal: tb.DuplicateOfBlockOrdinal,
		Ordinal:                 tb.Ordinal,
		Offset:                  tb.Offset,
		StartTime:               startTime,
		Duration:                d,
	}
}

///////////////////////////////////////////////////////////////////
// Commit the list of blocks for the blob
///////////////////////////////////////////////////////////////////

// PutBlobBlockList - commit the block list for the target blob
// ... the block list is read in from the resultsQ channel.
func PutBlobBlockList(blobInfo *SourceAndTargetInfo, resultsQ *chan WorkerResult, numberOfBlocks int, blobSize uint64) {

	var blockList = make([]storage.Block, numberOfBlocks)
	durations := make([]time.Duration, numberOfBlocks)
	starts := make([]time.Time, numberOfBlocks)

	// do one pass, with potential for requeues to handle out-of-order entries.  The requeues
	// occur when a block is tagged as a duplicate of a block not yet processed.
	blocksToGo := numberOfBlocks

	var duplicateBlocksSkipped = 0
	var sizeSkipped uint64

	for blocksToGo > 0 {
		wr, _ := <-*resultsQ
		blocksToGo--
		// fmt.Printf("List ordinal %d being processed, dup of %d, offset=%d\n", wr.Ordinal, wr.DuplicateOfBlockOrdinal, wr.Offset)
		if wr.DuplicateOfBlockOrdinal >= 0 { // this block is a duplicate of another.
			if blockList[wr.DuplicateOfBlockOrdinal].ID != "" {
				// ... that we've already recorded, so just dup it
				//fmt.Printf("Block %d is a duplicate of %d\n", wr.Ordinal, wr.DuplicateOfBlockOrdinal)
				blockList[wr.Ordinal] = blockList[wr.DuplicateOfBlockOrdinal]
				duplicateBlocksSkipped++
				sizeSkipped += uint64(wr.BlockSize)
			} else { // we haven't yet see the block of which this is a dup, so requeue this one
				*resultsQ <- wr
				blocksToGo++ //TODO: should include infinite loop protection
			}
		} else {
			blockList[wr.Ordinal].ID = wr.ItemID
			blockList[wr.Ordinal].Status = "Uncommitted"
			durations[wr.Ordinal] = wr.Duration
			starts[wr.Ordinal] = wr.StartTime
		}
	}

	if false { //TODO: should add option to turn on this level of tracing
		fmt.Printf("Final BlockList:\n")
		for j := 0; j < numberOfBlocks; j++ {
			fmt.Printf("   [%2d]: ID=%s, Status=%s, Start=%v, Duration=%v\n",
				j, blockList[j].ID, blockList[j].Status, starts[j], durations[j])
		}
	}

	util.RetriableOperation(func() error {
		var bc = getBlobStorageClient(blobInfo.TargetCredentials)

		t0 := time.Now()

		if err := bc.PutBlockList(blobInfo.TargetContainerName, blobInfo.TargetBlobName, blockList); err != nil {
			return err
		}

		t1 := time.Now()
		fmt.Printf("\rBlocks Committed: %d, Commit time = %v, Duplicate blocks detected = %d, Saving = %v (%d%%)",
			len(blockList), t1.Sub(t0), duplicateBlocksSkipped, sizeSkipped, uint64(sizeSkipped)*100/blobSize)
		return nil
	})
}

///////////////////////////////////////////////////////////////////
// Miscellaneous Helpers
///////////////////////////////////////////////////////////////////

//ProgressUpdate called whenever a worker completes successfully
type ProgressUpdate func(results WorkerResult)

// ProcessProgress  - processes progress typically used to update UI
// ... called for each block processed to run the update function.
func ProcessProgress(resultQ *chan WorkerResult, update ProgressUpdate, finalCommitQueue *chan WorkerResult) {
	go func() {
		for {
			result, ok := <-(*resultQ)
			if !ok {
				// empty, end of input
				return
			}
			// otherwise, call the user function.
			update(result)

			//send to the final queue
			(*finalCommitQueue) <- result
		}
	}()
}

///////////////////////////////////////////////////////////////////
