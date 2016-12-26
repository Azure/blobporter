package main

// BlobPorter Tool
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
	"flag"
	"fmt"
	"log"
	"math"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"sync/atomic"

	"github.com/Azure/blobporter/blocktransfer"
	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/sources"
	"github.com/Azure/blobporter/util"
)

var containerName string
var blobName string
var sourceURI string
var numberOfWorkers int

var blockSize uint64
var blockSizeStr string

var numberOfReaders int
var extraWorkerBufferSlots int // allocate this many extra slots above the # of workers to allow I/O to work ahead
var dedupeLevel blocktransfer.DupeCheckLevel = blocktransfer.None
var sourceType string
var dedupeLevelOptStr = dedupeLevel.ToString()
var storageAccountName string
var storageAccountKey string
var profile bool

const (
	// User can use environment variables to specify storage account information
	storageAccountNameEnvVar = "ACCOUNT_NAME"
	storageAccountKeyEnvVar  = "ACCOUNT_KEY"
	profiledataFile          = "blobporterprof"
	MiByte                   = 1048576
	programVersion           = "0.1.02" // version number to show in help
)

func init() {

	// shape the default parallelism based on number of CPUs
	var defaultNumberOfWorkers = int(float32(runtime.NumCPU()) * 12.5)
	var defaultNumberOfReaders = int(float32(runtime.NumCPU()) * 1.5)
	blockSizeStr = "4MB" // default size for blob blocks
	var defaultSourceType = "File"
	const (
		dblockSize             = 4 * MiByte
		extraWorkerBufferSlots = 5
	)

	util.StringVarAlias(&sourceURI, "f", "source_file", "", "source file to upload")
	util.StringVarAlias(&blobName, "n", "blob_name", "", "blob name (e.g. myblob.txt)")
	util.StringVarAlias(&containerName, "c", "container_name", "", "container name (e.g. mycontainer)")
	util.IntVarAlias(&numberOfWorkers, "g", "concurrent_workers", defaultNumberOfWorkers, ""+
		"number of threads for parallel upload")
	util.IntVarAlias(&numberOfReaders, "r", "concurrent_readers", defaultNumberOfReaders, ""+
		"number of threads for parallel reading of the input file")
	util.StringVarAlias(&blockSizeStr, "b", "block_size", blockSizeStr, "desired size of each blob block. "+
		"Can be specified an integer byte count or integer suffixed with B, KB, MB, or GB. ")
	util.BoolVarAlias(&util.Verbose, "v", "verbose", false, "display verbose output (Version="+programVersion+")")
	util.BoolVarAlias(&profile, "p", "profile", false, "enables CPU profiling.")
	util.StringVarAlias(&storageAccountName, "a", "account_name", "", ""+
		"storage account name (e.g. mystorage). "+
		"Can also be specified via the "+storageAccountNameEnvVar+" environment variable.")
	util.StringVarAlias(&storageAccountKey, "k", "account_key", "", ""+
		"storage account key string (e.g. "+
		"4Rr8CpUM9Y/3k/SqGSr/oZcLo3zNU6aIo32NVzda4EJj0hjS2Jp7NVLAD3sFp7C67z/i7Rfbrpu5VHgcmOShTg==). "+
		"Can also be specified via the "+storageAccountKeyEnvVar+" environment variable.")
	util.StringVarAlias(&dedupeLevelOptStr, "d", "dup_check_level", dedupeLevelOptStr, ""+
		"Desired level of effort to detect duplicate data blocks to minimize upload size. "+
		"Must be one of "+blocktransfer.DupeCheckLevelStr)
	util.StringVarAlias(&sourceType, "s", "source_type", defaultSourceType, ""+
		"Transport protocol to access the source data. "+
		"Must be one of "+blocktransfer.SourceTypeStr)
}

var dataTransfered int64
var targetRetries int32

func main() {

	parseAndValidate()

	//TODO: add flag to enable profiling...
	if profile {
		f, err := os.Create("profiledata")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	t0 := time.Now()
	var t1 time.Time
	var fileSize uint64
	var threadTarget int

	sti := newSourceAndTargetInfo()

	// creates a source pipeline
	pipelineImpl := getSourcePipeline()
	blockQ, numOfBlocks, fileSize := pipelineImpl.ConstructBlockInfoQueue(blockSize)

	// tone down expected input concurrency if there are fewer blocks than expected readers
	if numberOfReaders > numOfBlocks {
		numberOfReaders = numOfBlocks
	}

	fmt.Printf("Task: source %s to blob: %s/%s/%s, Size=%s, Blocks=%d\n",
		sti.SourceURI, storageAccountName, sti.TargetContainerName, sti.TargetBlobName, util.PrintSize(fileSize), numOfBlocks)
	fmt.Printf("Configuration: R=%d, W=%d, MP=%d, MaxBlockSize=%d bytes, DuplicateDetection=%s, Version=%s\n",
		numberOfReaders, numberOfWorkers, threadTarget, sti.IdealBlockSize, dedupeLevel.ToString(), programVersion)

	defer func() { // final wrap-up summary
		fmt.Printf("\nThe process took %v to run.\n", t1.Sub(t0))
		fmt.Printf("Retry avg: %v Retries %d\n", float32(targetRetries)/float32(numOfBlocks), targetRetries)
		MBs := float64(fileSize) / MiByte / t1.Sub(t0).Seconds()
		fmt.Printf("Throughput: %1.2f MB/s (%1.2f Mb/s) \n", MBs, MBs*8)
		fmt.Printf("Configuration: R=%d, W=%d, MP=%d DataSize=%s, Blocks=%d\n",
			numberOfReaders, numberOfWorkers, threadTarget, util.PrintSize(fileSize), numOfBlocks)

	}()

	// Boost thread count up from GO default of one thread per core
	threadTarget = numberOfReaders + numberOfWorkers + 4
	runtime.GOMAXPROCS(threadTarget)

	// Create the queue to connect the file readers to the upload workers.
	// Note that both the input and output from the readers uses FileChunks.  The difference
	// is that output file chunks have the data block populated.  Input chunks have nil for
	// this field.
	workerQ := make(chan pipeline.Part, numberOfWorkers+extraWorkerBufferSlots)

	var wgReaders sync.WaitGroup
	wgReaders.Add(numberOfReaders)
	go blocktransfer.StartReaders(blockQ, &workerQ, numberOfReaders, &wgReaders, &pipelineImpl)

	// Start up upload workers to send file data to Azure Storage as blocks.  These take input
	// from the output of the file Readers (the workerQ channel) and produce another channel of
	// individual upload results (resultsQ).

	var wgWorkers sync.WaitGroup
	wgWorkers.Add(numberOfWorkers) // completion control for upload workers

	// Channel for summmary info about upload of each block.  Buffered since we're
	// explicitly waiting on upload workers to finish anyway.
	resultsQ := make(chan blocktransfer.WorkerResult, numOfBlocks)

	go blocktransfer.StartWorkers(&workerQ, &resultsQ, numberOfWorkers, &wgWorkers, dedupeLevel, sti)

	var commitChannel = make(chan blocktransfer.WorkerResult, numOfBlocks)

	createProgressDelegation(&resultsQ, &commitChannel, fileSize)

	wgReaders.Wait()
	close(workerQ)

	wgWorkers.Wait() // Ensure all upload workers complete

	// Commit the block list on the blob
	blocktransfer.PutBlobBlockList(sti, &commitChannel, numOfBlocks, fileSize)
	t1 = time.Now()
}

func newSourceAndTargetInfo() *blocktransfer.SourceAndTargetInfo {
	cred := blocktransfer.StorageAccountCredentials{
		AccountName: storageAccountName,
		AccountKey:  storageAccountKey}

	sti := blocktransfer.SourceAndTargetInfo{
		TargetCredentials:   &cred,
		SourceURI:           sourceURI,
		TargetContainerName: containerName,
		TargetBlobName:      blobName,
		IdealBlockSize:      uint32(blockSize)}

	return &sti
}

func getSourcePipeline() pipeline.SourcePipeline {

	if strings.ToLower(sourceType) == "file" {
		return sources.NewFilePipeline(sourceURI)
	}

	if strings.ToLower(sourceType) == "http" {
		return sources.NewHTTPPipeline(sourceURI)
	}

	log.Fatal("Invalid source type.")

	//compiler requieres a return, although not reachable
	return nil

}

// parseAndValidate - extra checks on command line arguments
func parseAndValidate() {
	flag.Parse()

	if sourceURI == "" {
		log.Fatal("source filename not specified")
	}

	if containerName == "" {
		log.Fatal("container name not specified ")
	}

	if blobName == "" {
		//TODO: should do more vetting of the blob name
		basename := strings.ToLower(filepath.Base(sourceURI))
		blobName = basename
	}

	// wasn't specified, try the environment variable
	if storageAccountName == "" {
		envVal := os.Getenv(storageAccountNameEnvVar)
		if envVal == "" {
			log.Fatal("storage account name not specified or found in environment variable " + storageAccountNameEnvVar)
		}
		storageAccountName = envVal
	}

	// wasn't specified, try the environment variable
	if storageAccountKey == "" {
		envVal := os.Getenv(storageAccountKeyEnvVar)
		if envVal == "" {
			log.Fatal("storage account key not specified or found in environment variable " + storageAccountKeyEnvVar)
		}
		storageAccountKey = envVal
	}

	var errx error
	blockSize, errx = util.ByteCountFromSizeString(blockSizeStr)
	if errx != nil {
		log.Fatal("Invalid block size specified: " + blockSizeStr)
	}

	blockSizeMax := blocktransfer.LargeBlockSizeMax

	if blockSize > blockSizeMax {
		log.Fatal("Block size specified (" + blockSizeStr + ") exceeds maximum of " + util.PrintSize(blockSizeMax))
	}

	dedupeLevel, errx = blocktransfer.ParseDupeCheckLevel(dedupeLevelOptStr)
	if errx != nil {
		log.Fatalf("Duplicate detection level is invalid.  Found '%s', must be one of %s", dedupeLevelOptStr, blocktransfer.DupeCheckLevelStr)
	}

	//TODO validate source type as well... relying on getSourcePipeline to check if the input is valid
}

// createProgressDelegation - create rolling status text for the console.
// ... Note: this function must be used as it is also responsible for moving result objects from the resultQ
// ... to the finalCommitQueue, which is the channel read by code to commit the final blockList.
func createProgressDelegation(resultQ *chan blocktransfer.WorkerResult,
	// callback for each block uploaded
	finalCommitQueue *chan blocktransfer.WorkerResult, fileSize uint64) {
	blocktransfer.ProcessProgress(resultQ, func(r blocktransfer.WorkerResult) {

		atomic.AddInt32(&targetRetries, int32(r.Retries))
		if false && util.Verbose {
			blockID, _ := base64.StdEncoding.DecodeString(r.ItemID)
			fmt.Fprintf(os.Stdout, "|%v|%v duration: %v \n", r.WorkerID, string(blockID), r.Duration)
		} else {
			dataTransfered = dataTransfered + int64(r.BlockSize)
			p := int(math.Ceil((float64(dataTransfered) / float64(fileSize)) * 100))
			var ind string
			var pchar string
			for i := 0; i < 25; i++ {
				if i+1 > p/4 {
					pchar = "."
				} else {
					pchar = "|"
				}

				ind = ind + pchar
			}
			fmt.Fprintf(os.Stdout, "\r --> %3d %% [%v]", p, ind)
		}
		// add result to final result queue for use by blob block commit
		//*finalCommitQueue <- r
	}, finalCommitQueue)
}
