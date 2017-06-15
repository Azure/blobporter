package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/sources"
	"github.com/Azure/blobporter/targets"
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
var extraWorkerBufferSlots int // allocate this many extra slots above the # of workers to allow I/O to work ahead
var dedupeLevel = transfer.None
var dedupeLevelOptStr = dedupeLevel.ToString()
var transferDef transfer.Definition
var transferDefStr string
var defaultTransferDef = transfer.FileToBlock
var storageAccountName string
var storageAccountKey string
var storageClientHTTPTimeout int
var quietMode bool
var calculateMD5 bool

const (
	// User can use environment variables to specify storage account information
	storageAccountNameEnvVar = "ACCOUNT_NAME"
	storageAccountKeyEnvVar  = "ACCOUNT_KEY"
	programVersion           = "0.5.03" // version number to show in help
)

const numOfWorkersFactor = 9
const numOfReadersFactor = 6

func init() {

	//Show blobporter banner
	fmt.Printf("BlobPorter \nCopyright (c) Microsoft Corporation. \nVersion: %v\n---------------\n", programVersion)

	// shape the default parallelism based on number of CPUs
	var defaultNumberOfWorkers = runtime.NumCPU() * numOfWorkersFactor
	var defaultNumberOfReaders = runtime.NumCPU() * numOfReadersFactor

	blockSizeStr = "4MB" // default size for blob blocks
	const (
		dblockSize             = 4 * util.MB
		extraWorkerBufferSlots = 5
	)

	const (
		fileMsg              = "URL, file or files (e.g. /data/*.gz) to upload.\n\tDestination file for download."
		nameMsg              = "Blob name for upload or download scenarios from Azure Blob Storage.\n\tDestination file name for downloads from a URL."
		containerNameMsg     = "Container name (e.g. mycontainer).\n\tIf the container does not exist, it will be created."
		concurrentWorkersMsg = "Number of workers for parallel upload."
		concurrentReadersMsg = "Number of readers for parallel reading of the input file(s)."
		blockSizeMsg         = "Desired size of each blob block or page.\n\tCan be an integer byte count or integer suffixed with B, KB, MB, or GB.\n\tFor page blobs the value must be a multiple of 512 bytes."
		verboseMsg           = "Diplay verbose output for debugging."
		quietModeMsg         = "Quiet mode, no progress information is written to the stdout.\n\tErrors, warnings and final summary still are written."
		computeBlockMD5Msg   = "Computes the MD5 for the block and includes the value in the block request."
		httpTimeoutMsg       = "HTTP client timeout in seconds."
		accountNameMsg       = "Storage account name (e.g. mystorage).\n\tCan also be specified via the " + storageAccountNameEnvVar + " environment variable."
		accountKeyMsg        = "Storage account key string.\n\tCan also be specified via the " + storageAccountKeyEnvVar + " environment variable."
		dupcheckLevelMsg     = "Desired level of effort to detect duplicate data to minimize upload size.\n\tMust be one of " + transfer.DupeCheckLevelStr
		transferDefMsg       = "Defines the type of source and target in the transfer.\n\tMust be one of:\n\tfile-blockblob, file-pageblob, http-blockblob, http-pageblob, blob-file,\n\tpageblock-file (alias of blob-file), blockblob-file (alias of blob-file)\n\tor http-file."
	)

	flag.Usage = func() {
		util.PrintUsageDefaults("f", "file", "", fileMsg)
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
	}

	util.StringListVarAlias(&sourceURIs, "f", "file", "", fileMsg)
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

}

var dataTransferred uint64
var targetRetries int32

func displayFilesToTransfer(sourcesInfo []pipeline.SourceInfo) {
	fmt.Printf("Transfer Task: %v\n", transferDefStr)
	fmt.Printf("Files to Transfer:\n")
	for _, source := range sourcesInfo {
		fmt.Printf("Source: %v Size:%v \n", source.SourceName, source.Size)
	}
}

func displayFinalWrapUpSummary(duration time.Duration, targetRetries int32, threadTarget int, totalNumberOfBlocks int, totalSize uint64, cumWriteDuration time.Duration) {
	var netMB float64 = 1000000
	fmt.Printf("\nThe process took %v to run.\n", duration)
	MBs := float64(totalSize) / netMB / duration.Seconds()
	fmt.Printf("Throughput: %1.2f MB/s (%1.2f Mb/s) \n", MBs, MBs*8)
	fmt.Printf("Configuration: R=%d, W=%d, DataSize=%s, Blocks=%d\n",
		numberOfReaders, numberOfWorkers, util.PrintSize(totalSize), totalNumberOfBlocks)
	fmt.Printf("Cumulative Writes Duration: Total=%v, Avg Per Worker=%v\n",
		cumWriteDuration, time.Duration(cumWriteDuration.Nanoseconds()/int64(numberOfWorkers)))
	fmt.Printf("Retries: Avg=%v Total=%v\n", float32(targetRetries)/float32(totalNumberOfBlocks), targetRetries)
}

func main() {

	parseAndValidate()

	// Create pipelines
	sourcePipeline, targetPipeline := getPipelines()
	sourcesInfo := sourcePipeline.GetSourcesInfo()

	validateReaders(len(sourcesInfo))

	tfer := transfer.NewTransfer(&sourcePipeline, &targetPipeline, numberOfReaders, numberOfWorkers, blockSize)

	displayFilesToTransfer(sourcesInfo)
	pb := getProgressBarDelegate(tfer.TotalSize, quietMode)

	tfer.StartTransfer(dedupeLevel, pb)

	tfer.WaitForCompletion()

	displayFinalWrapUpSummary(tfer.Stats.Duration, targetRetries, tfer.ThreadTarget, tfer.TotalNumOfBlocks, tfer.TotalSize, tfer.Stats.CumWriteDuration)
}

const openFileLimitForLinux = 1024

//validates if the number of readers needs to be adjusted to accommodate max filehandle limits in Debian systems
func validateReaders(numOfSources int) {

	if runtime.GOOS == "linux" {
		if (numOfSources * numberOfReaders) > openFileLimitForLinux {
			numberOfReaders = openFileLimitForLinux / numOfSources

			if numberOfReaders == 0 {
				log.Fatal("Too many files in the transfer. Reduce the number of files to be transferred")
			}

			fmt.Printf("Warning! The requested number of readers will exceed the allowed limit of files open by the OS.\nThe number will be adjusted to: %v\n", numberOfReaders)

		}
	}

}

func isSourceHTTP() bool {

	url, err := url.Parse(sourceURIs[0])

	return err == nil && (strings.ToLower(url.Scheme) == "http" || strings.ToLower(url.Scheme) == "https")
}

func getPipelines() (pipeline.SourcePipeline, pipeline.TargetPipeline) {

	var source pipeline.SourcePipeline
	var target pipeline.TargetPipeline
	var defValue transfer.Definition
	var err error
	if defValue, err = transfer.ParseTransferDefinition(transferDefStr); err != nil {
		log.Fatal(err)
	}

	//container is required but when is a http to file transfer.
	if defValue != transfer.HTTPToFile {
		if containerName == "" {
			log.Fatal("container name not specified ")
		}
	}

	switch defValue {
	case transfer.FileToPage:
		source = sources.NewMultiFile(sourceURIs, blockSize, blobNames, numberOfReaders, calculateMD5)
		target = targets.NewAzurePage(storageAccountName, storageAccountKey, containerName)
	case transfer.FileToBlock:
		//Since this is default value detect if the source is http
		if isSourceHTTP() {
			source = sources.NewHTTP(sourceURIs, blobNames, calculateMD5)
			transferDefStr = transfer.HTTPToBlock
		} else {
			source = sources.NewMultiFile(sourceURIs, blockSize, blobNames, numberOfReaders, calculateMD5)
		}
		target = targets.NewAzureBlock(storageAccountName, storageAccountKey, containerName)

	case transfer.HTTPToPage:
		source = sources.NewHTTP(sourceURIs, blobNames, calculateMD5)
		target = targets.NewAzurePage(storageAccountName, storageAccountKey, containerName)

	case transfer.HTTPToBlock:
		source = sources.NewHTTP(sourceURIs, blobNames, calculateMD5)
		target = targets.NewAzureBlock(storageAccountName, storageAccountKey, containerName)

	case transfer.BlobToFile:

		if len(blobNames) == 0 {
			//use empty prefix to get the bloblist
			blobNames = []string{""}
		}

		source = sources.NewAzureBlob(containerName, blobNames, storageAccountName, storageAccountKey, calculateMD5)
		target = targets.NewMultiFile(true, numberOfWorkers)

	case transfer.HTTPToFile:
		source = sources.NewHTTP(sourceURIs, blobNames, calculateMD5)

		var targetAliases []string

		if len(blobNames) > 0 {
			targetAliases = blobNames
		} else {
			targetAliases = make([]string, len(sourceURIs))
			for i, src := range sourceURIs {
				targetAliases[i], err = util.GetFileNameFromURL(src)
				if err != nil {
					log.Fatal(err)
				}
			}
		}

		target = targets.NewMultiFile(true, numberOfWorkers)
	}

	return source, target
}

// parseAndValidate - extra checks on command line arguments
func parseAndValidate() {

	flag.Parse()

	var err error
	if util.HTTPClientTimeout < 30 {
		fmt.Printf("Warning! The storage HTTP client timeout is too low (>5). Setting value to 600s \n")
		util.HTTPClientTimeout = 600
	}

	blockSize, err = util.ByteCountFromSizeString(blockSizeStr)
	if err != nil {
		log.Fatal("Invalid block size specified: " + blockSizeStr)
	}

	blockSizeMax := util.LargeBlockSizeMax
	if blockSize > blockSizeMax {
		log.Fatal("Block size specified (" + blockSizeStr + ") exceeds maximum of " + util.PrintSize(blockSizeMax))
	}

	dedupeLevel, err = transfer.ParseDupeCheckLevel(dedupeLevelOptStr)
	if err != nil {
		log.Fatalf("Duplicate detection level is invalid.  Found '%s', must be one of %s", dedupeLevelOptStr, transfer.DupeCheckLevelStr)
	}

	err = validateTransferTypesParams()
	if err != nil {
		log.Fatal(err)
	}
}

//validates command line params considering the type of transfer
func validateTransferTypesParams() error {
	var transt transfer.Definition
	var err error
	if transt, err = transfer.ParseTransferDefinition(transferDefStr); err != nil {
		return err
	}

	if transt == transfer.HTTPToPage || transt == transfer.FileToPage {
		if blockSize%uint64(targets.PageSize) != 0 || blockSize > 4*util.MB {
			return fmt.Errorf("Invalid block size (%v) for a page blob. The size must be a multiple of %v (bytes) and less or equal to %v (4MB)", blockSize, targets.PageSize, 4*util.MB)
		}
	}

	isUpload := transt != transfer.BlobToFile && transt != transfer.HTTPToFile

	if (len(sourceURIs) == 0 || sourceURIs == nil) && isUpload {
		log.Fatal("The file parameter is missing. Must be a file, URL or file pattern (e.g. /data/*.fastq) ")
	}

	if transt != transfer.HTTPToFile {
		// wasn't specified, try the environment variable
		if storageAccountName == "" {
			envVal := os.Getenv(storageAccountNameEnvVar)
			if envVal == "" {
				return errors.New("storage account name not specified or found in environment variable " + storageAccountNameEnvVar)
			}
			storageAccountName = envVal
		}

		// wasn't specified, try the environment variable
		if storageAccountKey == "" {
			envVal := os.Getenv(storageAccountKeyEnvVar)
			if envVal == "" {
				return errors.New("storage account key not specified or found in environment variable " + storageAccountKeyEnvVar)
			}
			storageAccountKey = envVal
		}
	}

	return nil
}

func getProgressBarDelegate(totalSize uint64, quietMode bool) func(r pipeline.WorkerResult, committedCount int, bufferLevel int) {
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
