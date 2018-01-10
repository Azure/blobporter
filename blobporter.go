package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/transfer"
	"github.com/Azure/blobporter/util"
)

var containerName string
var blobNames util.ListFlag
var sourceURIs util.ListFlag
var numberOfWorkers int

var blockSize uint64
var blockSizeStr string

var numberOfReaders int
var dedupeLevel = transfer.None
var dedupeLevelOptStr = dedupeLevel.ToString()
var transferDef transfer.Definition
var transferDefStr string
var defaultTransferDef = transfer.FileToBlock
var storageAccountName string
var storageAccountKey string
var storageClientHTTPTimeout int

var sourceKeyInfo map[string]string
var sourceAuthorization string

var quietMode bool
var calculateMD5 bool
var exactNameMatch bool
var keepDirStructure bool
var numberOfHandlesPerFile = defaultNumberOfHandlesPerFile
var numberOfFilesInBatch = defaultNumberOfFilesInBatch

const (
	// User can use environment variables to specify storage account information
	storageAccountNameEnvVar  = "ACCOUNT_NAME"
	storageAccountKeyEnvVar   = "ACCOUNT_KEY"
	sourceAuthorizationEnvVar = "SOURCE_AUTH"
	programVersion            = "0.5.21" // version number to show in help
)

const numOfWorkersFactor = 8
const numOfReadersFactor = 5
const defaultNumberOfFilesInBatch = 200
const defaultNumberOfHandlesPerFile = 2

func init() {

	//Show blobporter banner
	fmt.Printf("BlobPorter \nCopyright (c) Microsoft Corporation. \nVersion: %v\n---------------\n", programVersion)

	// shape the default parallelism based on number of CPUs
	var defaultNumberOfWorkers = runtime.NumCPU() * numOfWorkersFactor
	var defaultNumberOfReaders = runtime.NumCPU() * numOfReadersFactor

	blockSizeStr = "8MB" // default size for blob blocks
	const (
		dblockSize = 8 * util.MB
	)

	const (
		fileMsg                    = "Source URL, file or files (e.g. /data/*.gz) to upload."
		nameMsg                    = "Blob name (e.g. myblob.txt) or prefix for download scenarios."
		containerNameMsg           = "Container name (e.g. mycontainer).\n\tIf the container does not exist, it will be created."
		concurrentWorkersMsg       = "Number of workers for parallel upload."
		concurrentReadersMsg       = "Number of readers for parallel reading of the input file(s)."
		blockSizeMsg               = "Desired size of each blob block or page.\n\tCan be an integer byte count or integer suffixed with B, KB, MB, or GB.\n\tFor page blobs the value must be a multiple of 512 bytes."
		verboseMsg                 = "Diplay verbose output for debugging."
		quietModeMsg               = "Quiet mode, no progress information is written to the stdout.\n\tErrors, warnings and final summary still are written."
		computeBlockMD5Msg         = "Computes the MD5 for the block and includes the value in the block request."
		httpTimeoutMsg             = "HTTP client timeout in seconds."
		accountNameMsg             = "Storage account name (e.g. mystorage).\n\tCan also be specified via the " + storageAccountNameEnvVar + " environment variable."
		accountKeyMsg              = "Storage account key string.\n\tCan also be specified via the " + storageAccountKeyEnvVar + " environment variable."
		dupcheckLevelMsg           = "Desired level of effort to detect duplicate data to minimize upload size.\n\tMust be one of " + transfer.DupeCheckLevelStr
		transferDefMsg             = "Defines the type of source and target in the transfer.\n\tMust be one of:\n\tfile-blockblob, file-pageblob, http-blockblob, http-pageblob, blob-file,\n\tpageblock-file (alias of blob-file), blockblob-file (alias of blob-file)\n\tor http-file."
		exactNameMatchMsg          = "If set or true only blobs that match the name exactly will be downloaded."
		keepDirStructureMsg        = "If set blobs are downloaded or uploaded keeping the directory structure from the source.\n\tNot applicable when the source is a HTTP endpoint."
		numberOfHandlersPerFileMsg = "Number of open handles for concurrent reads and writes per file."
		numberOfFilesInBatchMsg    = "Maximum number of files in a transfer.\n\tIf the number is exceeded new transfers are created"
	)

	flag.Usage = func() {
		util.PrintUsageDefaults("f", "source_file", "", fileMsg)
		util.PrintUsageDefaults("n", "name", "", nameMsg)
		util.PrintUsageDefaults("c", "container_name", "", containerNameMsg)
		util.PrintUsageDefaults("g", "concurrent_workers", strconv.Itoa(defaultNumberOfWorkers), concurrentWorkersMsg)
		util.PrintUsageDefaults("r", "concurrent_readers", strconv.Itoa(defaultNumberOfReaders), concurrentReadersMsg)
		util.PrintUsageDefaults("b", "block_size", blockSizeStr, blockSizeMsg)
		util.PrintUsageDefaults("v", "verbose", "false", verboseMsg)
		util.PrintUsageDefaults("q", "quiet_mode", "false", quietModeMsg)
		util.PrintUsageDefaults("m", "compute_blockmd5", "false", computeBlockMD5Msg)
		util.PrintUsageDefaults("s", "http_timeout", strconv.Itoa(util.HTTPClientTimeout), httpTimeoutMsg)
		util.PrintUsageDefaults("a", "account_name", "", accountNameMsg)
		util.PrintUsageDefaults("k", "account_key", "", accountKeyMsg)
		util.PrintUsageDefaults("d", "dup_check_level", dedupeLevelOptStr, dupcheckLevelMsg)
		util.PrintUsageDefaults("t", "transfer_definition", string(defaultTransferDef), transferDefMsg)
		util.PrintUsageDefaults("e", "exact_name", "false", exactNameMatchMsg)
		util.PrintUsageDefaults("p", "keep_directories", "false", keepDirStructureMsg)
		util.PrintUsageDefaults("h", "handles_per_file", strconv.Itoa(defaultNumberOfHandlesPerFile), numberOfHandlersPerFileMsg)
		util.PrintUsageDefaults("x", "files_per_transfer", strconv.Itoa(defaultNumberOfFilesInBatch), numberOfFilesInBatchMsg)

	}

	util.StringListVarAlias(&sourceURIs, "f", "source_file", "", fileMsg)
	util.StringListVarAlias(&blobNames, "n", "name", "", nameMsg)
	util.StringVarAlias(&containerName, "c", "container_name", "", containerNameMsg)
	util.IntVarAlias(&numberOfWorkers, "g", "concurrent_workers", defaultNumberOfWorkers, concurrentWorkersMsg)
	util.IntVarAlias(&numberOfReaders, "r", "concurrent_readers", defaultNumberOfReaders, concurrentReadersMsg)
	util.StringVarAlias(&blockSizeStr, "b", "block_size", blockSizeStr, blockSizeMsg)
	util.BoolVarAlias(&util.Verbose, "v", "verbose", false, verboseMsg)
	util.BoolVarAlias(&quietMode, "q", "quiet_mode", false, quietModeMsg)
	util.BoolVarAlias(&calculateMD5, "m", "compute_blockmd5", false, computeBlockMD5Msg)
	util.IntVarAlias(&util.HTTPClientTimeout, "s", "http_timeout", util.HTTPClientTimeout, httpTimeoutMsg)
	util.StringVarAlias(&storageAccountName, "a", "account_name", "", accountNameMsg)
	util.StringVarAlias(&storageAccountKey, "k", "account_key", "", accountKeyMsg)
	util.StringVarAlias(&dedupeLevelOptStr, "d", "dup_check_level", dedupeLevelOptStr, dupcheckLevelMsg)
	util.StringVarAlias(&transferDefStr, "t", "transfer_definition", string(defaultTransferDef), transferDefMsg)
	util.BoolVarAlias(&exactNameMatch, "e", "exact_name", false, exactNameMatchMsg)
	util.BoolVarAlias(&keepDirStructure, "p", "keep_directories", false, keepDirStructureMsg)
	util.IntVarAlias(&numberOfHandlesPerFile, "h", "handles_per_file", defaultNumberOfHandlesPerFile, numberOfHandlersPerFileMsg)
	util.IntVarAlias(&numberOfFilesInBatch, "x", "files_per_transfer", defaultNumberOfFilesInBatch, numberOfFilesInBatchMsg)
}

var dataTransferred uint64
var targetRetries int32
var transferType transfer.Definition

func displayFilesToTransfer(sourcesInfo []pipeline.SourceInfo, numOfBatches int, batchNumber int) {
	if numOfBatches == 1 {
		fmt.Printf("Files to Transfer (%v) :\n ", transferDefStr)
		for _, source := range sourcesInfo {
			fmt.Printf("Source: %v Size:%v \n", source.SourceName, source.Size)
		}

		return
	}

	fmt.Printf("\nBatch transfer (%v).\nFiles per Batch: %v.\nBatch: %v of %v\n ", transferDefStr, len(sourcesInfo), batchNumber+1, numOfBatches)
}

func displayFinalWrapUpSummary(duration time.Duration, targetRetries int32, threadTarget int, totalNumberOfBlocks int, totalSize uint64, cumWriteDuration time.Duration) {
	var netMB float64 = 1000000
	fmt.Printf("\nThe transfer took %v to run.\n", duration)
	MBs := float64(totalSize) / netMB / duration.Seconds()
	fmt.Printf("Throughput: %1.2f MB/s (%1.2f Mb/s) \n", MBs, MBs*8)
	fmt.Printf("Configuration: R=%d, W=%d, DataSize=%s, Blocks=%d\n",
		numberOfReaders, numberOfWorkers, util.PrintSize(totalSize), totalNumberOfBlocks)
	fmt.Printf("Cumulative Writes Duration: Total=%v, Avg Per Worker=%v\n",
		cumWriteDuration, time.Duration(cumWriteDuration.Nanoseconds()/int64(numberOfWorkers)))
	fmt.Printf("Retries: Avg=%v Total=%v\n", float32(targetRetries)/float32(totalNumberOfBlocks), targetRetries)
}

func main() {

	flag.Parse()

	// Create pipelines
	sourcePipelines, targetPipeline, err := getPipelines()

	if err != nil {
		log.Fatal(err)
	}

	stats := transfer.NewStats(numberOfWorkers, numberOfReaders)

	for b, sourcePipeline := range sourcePipelines {
		sourcesInfo := sourcePipeline.GetSourcesInfo()

		tfer := transfer.NewTransfer(&sourcePipeline, &targetPipeline, numberOfReaders, numberOfWorkers, blockSize)

		displayFilesToTransfer(sourcesInfo, len(sourcePipelines), b)
		pb := getProgressBarDelegate(tfer.TotalSize, quietMode)

		tfer.StartTransfer(dedupeLevel, pb)

		tfer.WaitForCompletion()

		stats.AddTransferInfo(tfer.GetStats())
	}

	stats.DisplaySummary()

}

func getProgressBarDelegate(totalSize uint64, quietMode bool) func(r pipeline.WorkerResult, committedCount int, bufferLevel int) {
	dataTransferred = 0
	targetRetries = 0
	if quietMode {
		return func(r pipeline.WorkerResult, committedCount int, bufferLevel int) {
			atomic.AddInt32(&targetRetries, int32(r.Stats.Retries))
			dataTransferred = dataTransferred + uint64(r.BlockSize)
		}

	}
	return func(r pipeline.WorkerResult, committedCount int, bufferLevel int) {

		atomic.AddInt32(&targetRetries, int32(r.Stats.Retries))

		dataTransferred = dataTransferred + uint64(r.BlockSize)
		p := int(math.Ceil((float64(dataTransferred) / float64(totalSize)) * 100))
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
			fmt.Fprintf(os.Stdout, "\r --> %3d %% [%v] Committed Count: %v Buffer Level: %03d%%", p, ind, committedCount, bufferLevel)
		}
	}
}
