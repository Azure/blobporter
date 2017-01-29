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
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strings"
	"time"

	"sync/atomic"

	"runtime/debug"

	"net/url"

	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/sources"
	"github.com/Azure/blobporter/targets"
	"github.com/Azure/blobporter/transfer"
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
var dedupeLevel transfer.DupeCheckLevel = transfer.None
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
	programVersion           = "0.2.00" // version number to show in help
)

func init() {

	//Show blobporter banner
	fmt.Printf("BlobPorter \nCopyright (c) Microsoft Corporation. \nVersion: %v\n---------------\n", programVersion)

	// shape the default parallelism based on number of CPUs
	var defaultNumberOfWorkers = int(float32(runtime.NumCPU()) * 12.5)
	var defaultNumberOfReaders = int(float32(runtime.NumCPU()) * 1.5)
	blockSizeStr = "4MB" // default size for blob blocks
	var defaultSourceType = "file"
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
	util.BoolVarAlias(&profile, "p", "profile", false, "enables profiling.")
	util.StringVarAlias(&storageAccountName, "a", "account_name", "", ""+
		"storage account name (e.g. mystorage). "+
		"Can also be specified via the "+storageAccountNameEnvVar+" environment variable.")
	util.StringVarAlias(&storageAccountKey, "k", "account_key", "", ""+
		"storage account key string (e.g. "+
		"4Rr8CpUM9Y/3k/SqGSr/oZcLo3zNU6aIo32NVzda4EJj0hjS2Jp7NVLAD3sFp7C67z/i7Rfbrpu5VHgcmOShTg==). "+
		"Can also be specified via the "+storageAccountKeyEnvVar+" environment variable.")
	util.StringVarAlias(&dedupeLevelOptStr, "d", "dup_check_level", dedupeLevelOptStr, ""+
		"Desired level of effort to detect duplicate data blocks to minimize upload size. "+
		"Must be one of "+transfer.DupeCheckLevelStr)
	//Deprecated.
	util.StringVarAlias(&sourceType, "s", "source_type", defaultSourceType, ""+
		"Transport protocol to access the source data. (deprecated)")

	//Profiling...
	if profile {
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()

	}

	if runtime.GOOS == "windows" {
		go func() {
			for {
				time.Sleep(time.Minute * 5)
				debug.FreeOSMemory()
			}
		}()
	}

}

var dataTransfered int64
var targetRetries int32

func displayFilesToTransfer(sourcesInfo []string) {
	fmt.Printf("Files to Transfer:\n")
	for _, source := range sourcesInfo {
		fmt.Printf("%v\n", source)
	}
}

func displayFinalWrapUpSummary(duration time.Duration, targetRetries int32, threadTarget int, totalNumberOfBlocks int, totalSize uint64) {
	fmt.Printf("\nThe process took %v to run.\n", duration)
	fmt.Printf("Retry avg: %v Retries %d\n", float32(targetRetries)/float32(totalNumberOfBlocks), targetRetries)
	MBs := float64(totalSize) / MiByte / duration.Seconds()
	fmt.Printf("Throughput: %1.2f MB/s (%1.2f Mb/s) \n", MBs, MBs*8)
	fmt.Printf("Configuration: R=%d, W=%d, MP=%d DataSize=%s, Blocks=%d\n",
		numberOfReaders, numberOfWorkers, threadTarget, util.PrintSize(totalSize), totalNumberOfBlocks)

}
func main() {

	parseAndValidate()

	// Create pipelines
	sourcePipeline := getSourcePipeline()
	targetPipeline := getTargetPipeline()

	sourcesInfo := sourcePipeline.GetSourcesInfo()

	tfer := transfer.NewTransfer(&sourcePipeline, &targetPipeline, numberOfReaders, numberOfWorkers, blockSize)

	displayFilesToTransfer(sourcesInfo)
	pb := getProgressBarDelegate(tfer.TotalSize)

	tfer.StartTransfer(dedupeLevel, pb)

	tfer.WaitForCompletion()

	displayFinalWrapUpSummary(tfer.Duration, targetRetries, tfer.ThreadTarget, tfer.TotalNumOfBlocks, tfer.TotalSize)
}

func getTargetPipeline() pipeline.TargetPipeline {
	cred := pipeline.StorageAccountCredentials{
		AccountName: storageAccountName,
		AccountKey:  storageAccountKey}
	return targets.NewAzureBlock(&cred, containerName)
}

func isSourceHTTP() bool {

	url, err := url.Parse(sourceURI)

	return err != nil || strings.ToLower(url.Scheme) == "http" || strings.ToLower(url.Scheme) == "https"
}

func getSourcePipeline() pipeline.SourcePipeline {

	//try to derive source from the source
	if isSourceHTTP() {

		var name = sourceURI
		if blobName != "" {
			name = blobName
		}

		return sources.NewHTTPPipeline(sourceURI, name)
	}

	if strings.ToLower(sourceType) == "file_legacy" {
		return sources.NewFilePipeline(sourceURI, blobName)
	}

	return sources.NewMultiFilePipeline(sourceURI, blockSize)

}

// parseAndValidate - extra checks on command line arguments
func parseAndValidate() {

	flag.Parse()

	if sourceURI == "" {
		log.Fatal("The source is missing. Must be a file, URL or file pattern (e.g. /data/*.fastq) ")
	}

	if containerName == "" {
		log.Fatal("container name not specified ")
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

	blockSizeMax := util.LargeBlockSizeMax

	if blockSize > blockSizeMax {
		log.Fatal("Block size specified (" + blockSizeStr + ") exceeds maximum of " + util.PrintSize(blockSizeMax))
	}

	dedupeLevel, errx = transfer.ParseDupeCheckLevel(dedupeLevelOptStr)
	if errx != nil {
		log.Fatalf("Duplicate detection level is invalid.  Found '%s', must be one of %s", dedupeLevelOptStr, transfer.DupeCheckLevelStr)
	}

	//TODO validate source type as well... relying on getSourcePipeline to check if the input is valid
}

func getProgressBarDelegate(totalSize uint64) func(r pipeline.WorkerResult, committedCount int) {
	delegate := func(r pipeline.WorkerResult, committedCount int) {

		atomic.AddInt32(&targetRetries, int32(r.Retries))

		dataTransfered = dataTransfered + int64(r.BlockSize)
		p := int(math.Ceil((float64(dataTransfered) / float64(totalSize)) * 100))
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
		if !util.Verbose {
			fmt.Fprintf(os.Stdout, "\r --> %3d %% [%v] Committed Count: %v", p, ind, committedCount)
		}
	}

	return delegate
}
