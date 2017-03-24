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

const (
	// User can use environment variables to specify storage account information
	storageAccountNameEnvVar = "ACCOUNT_NAME"
	storageAccountKeyEnvVar  = "ACCOUNT_KEY"
	programVersion           = "0.4.01" // version number to show in help
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

	util.StringListVarAlias(&sourceURIs, "f", "file", "", "URL, file or files (e.g. /data/*.gz) to upload. \nDestination file for download.")
	util.StringListVarAlias(&blobNames, "n", "name", "", "Blob name to upload or download from Azure Blob Storage. \nDestination file for download from URL")
	util.StringVarAlias(&containerName, "c", "container_name", "", "container name (e.g. mycontainer)")
	util.IntVarAlias(&numberOfWorkers, "g", "concurrent_workers", defaultNumberOfWorkers, ""+
		"number of threads for parallel upload")
	util.IntVarAlias(&numberOfReaders, "r", "concurrent_readers", defaultNumberOfReaders, ""+
		"number of threads for parallel reading of the input file")
	util.StringVarAlias(&blockSizeStr, "b", "block_size", blockSizeStr, "desired size of each blob block. "+
		"Can be specified an integer byte count or integer suffixed with B, KB, MB, or GB. ")
	util.BoolVarAlias(&util.Verbose, "v", "verbose", false, "display verbose output (Version="+programVersion+")")
	util.IntVarAlias(&util.HTTPClientTimeout, "s", "http_timeout", util.HTTPClientTimeout, "sets the HTTP client timeout in seconds. \nDefault value is 30s.")
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
	util.StringVarAlias(&transferDefStr, "t", "transfer_definition", string(defaultTransferDef),
		"Defines the source and target of the transfer. Must be one of file-blockblob, file-pageblob, http-blockblob, http-pageblob, blob-file, pageblock-file (alias of blob-file), blockblob-file (alias of blob-file) or http-file")
}

var dataTransfered uint64
var targetRetries int32

func displayFilesToTransfer(sourcesInfo []pipeline.SourceInfo) {
	fmt.Printf("Transfer Task: %v\n", transferDefStr)
	fmt.Printf("Files to Transfer:\n")
	for _, source := range sourcesInfo {
		fmt.Printf("Source: %v Size:%v \n", source.SourceName, source.Size)
	}
}

func displayFinalWrapUpSummary(duration time.Duration, targetRetries int32, threadTarget int, totalNumberOfBlocks int, totalSize uint64, cumWriteDuration time.Duration) {

	fmt.Printf("\nThe process took %v to run.\n", duration)
	MBs := float64(totalSize) / float64(util.MB) / duration.Seconds()
	fmt.Printf("Throughput: %1.2f MB/s (%1.2f Mb/s) \n", MBs, MBs*8)
	fmt.Printf("Configuration: R=%d, W=%d, MP=%d DataSize=%s, Blocks=%d\n",
		numberOfReaders, numberOfWorkers, threadTarget, util.PrintSize(totalSize), totalNumberOfBlocks)
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
	pb := getProgressBarDelegate(tfer.TotalSize)

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

	return err != nil || strings.ToLower(url.Scheme) == "http" || strings.ToLower(url.Scheme) == "https"
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
		target = targets.NewAzurePage(storageAccountName, storageAccountKey, containerName)
		source = sources.NewMultiFilePipeline(sourceURIs, blockSize, blobNames, numberOfReaders)
	case transfer.FileToBlock:
		target = targets.NewAzureBlock(storageAccountName, storageAccountKey, containerName)
		//Since this is default value detect if the source is http
		if isSourceHTTP() {
			source = sources.NewHTTPPipeline(sourceURIs, blobNames)
			transferDefStr = transfer.HTTPToBlock
		} else {
			source = sources.NewMultiFilePipeline(sourceURIs, blockSize, blobNames, numberOfReaders)
		}
	case transfer.HTTPToPage:
		target = targets.NewAzurePage(storageAccountName, storageAccountKey, containerName)
		source = sources.NewHTTPPipeline(sourceURIs, blobNames)
	case transfer.HTTPToBlock:
		target = targets.NewAzureBlock(storageAccountName, storageAccountKey, containerName)
		source = sources.NewHTTPPipeline(sourceURIs, blobNames)
	case transfer.BlobToFile:
		if len(blobNames) == 0 {
			log.Fatal("Blob name(s) (-n) is required when downloading data from blob")
		}
		target = targets.NewFile(sourceURIs[0], true, numberOfWorkers)
		source = sources.NewHTTPAzureBlockPipeline(containerName, blobNames, storageAccountName, storageAccountKey)
	case transfer.HTTPToFile:
		if len(blobNames) == 0 || blobNames[0] == "" {
			log.Fatal("File name (-n) is required when downloading data from a URL")
		}
		target = targets.NewFile(blobNames[0], true, numberOfWorkers)
		source = sources.NewHTTPPipeline(sourceURIs, blobNames)
	}

	return source, target
}

// parseAndValidate - extra checks on command line arguments
func parseAndValidate() {

	flag.Parse()

	var err error
	if util.HTTPClientTimeout < 5 {
		fmt.Printf("Warning! The storage HTTP client timeout is too low (>5). Setting value to 30s \n")
		util.HTTPClientTimeout = 30
	}

	if len(sourceURIs) == 0 || sourceURIs == nil {
		log.Fatal("The file parameter is missing. Must be a file, URL or file pattern (e.g. /data/*.fastq) ")
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
	storeCredsReq := true
	var err error
	if transt, err = transfer.ParseTransferDefinition(transferDefStr); err != nil {
		return err
	}

	switch {
	case transt == transfer.HTTPToPage || transt == transfer.FileToPage:
		if blockSize%uint64(targets.PageSize) != 0 || blockSize > 4*util.MB {
			return fmt.Errorf("Invalid block size (%v) for a page blob. The size must be a multiple of %v (bytes) and less or equal to %v (4MB)", blockSize, targets.PageSize, 4*util.MB)
		}
	case transt == transfer.BlobToFile:
		if len(blobNames) == 0 {
			return errors.New("Blob name(s) (-n) is required when downloading data from a blob")
		}
	case transt == transfer.HTTPToFile:
		storeCredsReq = false
		if len(blobNames) == 0 || blobNames[0] == "" {
			return errors.New("File name (-n) is required when downloading data from an URL")
		}

	}

	// wasn't specified, try the environment variable
	if storageAccountName == "" && storeCredsReq {
		envVal := os.Getenv(storageAccountNameEnvVar)
		if envVal == "" {
			return errors.New("storage account name not specified or found in environment variable " + storageAccountNameEnvVar)
		}
		storageAccountName = envVal
	}

	// wasn't specified, try the environment variable
	if storageAccountKey == "" && storeCredsReq {
		envVal := os.Getenv(storageAccountKeyEnvVar)
		if envVal == "" {
			return errors.New("storage account key not specified or found in environment variable " + storageAccountKeyEnvVar)
		}
		storageAccountKey = envVal
	}

	return nil
}

func getProgressBarDelegate(totalSize uint64) func(r pipeline.WorkerResult, committedCount int, bufferLevel int) {
	delegate := func(r pipeline.WorkerResult, committedCount int, bufferLevel int) {

		atomic.AddInt32(&targetRetries, int32(r.Stats.Retries))

		dataTransfered = dataTransfered + uint64(r.BlockSize)
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
			fmt.Fprintf(os.Stdout, "\r --> %3d %% [%v] Committed Count: %v Buffer Level: %03d%%", p, ind, committedCount, bufferLevel)
		}
	}

	return delegate
}
