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
	programVersion            = "0.5.20" // version number to show in help
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

	parseAndValidate()

	// Create pipelines
	sourcePipelines, targetPipeline := getPipelines()
	stats := transfer.NewStats(numberOfWorkers, numberOfReaders)

	for b, sourcePipeline := range sourcePipelines {
		sourcesInfo := sourcePipeline.GetSourcesInfo()

		adjustReaders(len(sourcesInfo))
		tfer := transfer.NewTransfer(&sourcePipeline, &targetPipeline, numberOfReaders, numberOfWorkers, blockSize)

		displayFilesToTransfer(sourcesInfo, len(sourcePipelines), b)
		pb := getProgressBarDelegate(tfer.TotalSize, quietMode)

		tfer.StartTransfer(dedupeLevel, pb)

		tfer.WaitForCompletion()

		stats.AddTransferInfo(tfer.GetStats())
	}

	stats.DisplaySummary()

}

const openFileLimitForLinux = 1024

//validates if the number of readers needs to be adjusted to accommodate max filehandle limits in Debian systems
func adjustReaders(numOfSources int) {
	//var source []pipeline.SourcePipeline
	//var target pipeline.TargetPipeline
	if runtime.GOOS != "linux" {
		return
	}

	if transferType == transfer.FileToBlock || transferType == transfer.FileToPage {
		if (numOfSources * numberOfHandlesPerFile) > openFileLimitForLinux {
			numberOfHandlesPerFile = openFileLimitForLinux / (numOfSources * numberOfHandlesPerFile)

			if numberOfHandlesPerFile == 0 {
				log.Fatal("The number of files will cause the process to exceed the limit of open files allowed by the OS. Reduce the number of files to be transferred")
			}

			fmt.Printf("Warning! Adjusting the number of handles per file (-h) to %v\n", numberOfHandlesPerFile)

		}
	}
}

func isSourceHTTP() bool {

	url, err := url.Parse(sourceURIs[0])

	return err == nil && (strings.ToLower(url.Scheme) == "http" || strings.ToLower(url.Scheme) == "https")
}

func getTransferPipelines(pipelinesFactory transferPipelinesFactory, validationRules ...parseAndvalidationRule) ([]pipeline.SourcePipeline, pipeline.TargetPipeline) {
	for i := 0; i < len(validationRules); i++ {
		val := validationRules[i]
		if err := val(); err != nil {
			log.Fatal(err)
		}
	}

	return pipelinesFactory()
}

type transferPipelinesFactory func() ([]pipeline.SourcePipeline, pipeline.TargetPipeline)

func getPipelines() ([]pipeline.SourcePipeline, pipeline.TargetPipeline) {

	switch transferType {
	case transfer.FileToPage:
		return getTransferPipelines(getFileToPagePipelines,
			pvBlobAuthInfoIsReq,
			pvSourceURIISReq,
			pvBlockSizeCheckForPageBlobs,
			pvContainerIsReq)
	case transfer.FileToBlock:
		return getTransferPipelines(getFileToBlockPipelines,
			pvBlobAuthInfoIsReq,
			pvSourceURIISReq,
			pvBlockSizeCheckForBlockBlobs,
			pvContainerIsReq)
	case transfer.HTTPToPage:
		return getTransferPipelines(getHTTPToPagePipelines,
			pvBlobAuthInfoIsReq,
			pvSourceURIISReq,
			pvIsSourceHTTP,
			pvBlockSizeCheckForPageBlobs,
			pvContainerIsReq)
	case transfer.HTTPToBlock:
		return getTransferPipelines(getHTTPToBlockPipelines,
			pvBlobAuthInfoIsReq,
			pvSourceURIISReq,
			pvIsSourceHTTP,
			pvContainerIsReq,
			pvBlockSizeCheckForBlockBlobs)
	case transfer.BlobToFile:
		return getTransferPipelines(getBlobToFilePipelines,
			pvBlobAuthInfoIsReq,
			pvContainerIsReq,
			pvBlockSizeCheckForBlockBlobs)
	case transfer.HTTPToFile:
		return getTransferPipelines(getHTTPToFilePipelines,
			pvSourceURIISReq,
			pvIsSourceHTTP)
	case transfer.BlobToBlock:
		return getTransferPipelines(getBlobToBlockPipelines,
			pvBlobAuthInfoIsReq,
			pvContainerIsReq,
			pvSourceURIISReq,
			pvSourceInfoForBlobIsReq,
			pvBlockSizeCheckForBlockBlobs)
	case transfer.BlobToPage:
		return getTransferPipelines(getBlobToPagePipelines,
			pvBlobAuthInfoIsReq,
			pvContainerIsReq,
			pvSourceURIISReq,
			pvSourceInfoForBlobIsReq,
			pvBlockSizeCheckForPageBlobs)
	}

	log.Fatal(fmt.Errorf("Invalid transfer type: %v ", transferType))

	return nil, nil
}
func getFileToPagePipelines() (source []pipeline.SourcePipeline, target pipeline.TargetPipeline) {

	params := &sources.MultiFileParams{
		SourcePatterns:   sourceURIs,
		BlockSize:        blockSize,
		TargetAliases:    blobNames,
		NumOfPartitions:  numberOfReaders,
		MD5:              calculateMD5,
		KeepDirStructure: keepDirStructure,
		FilesPerPipeline: numberOfFilesInBatch}

	source = sources.NewMultiFile(params)
	target = targets.NewAzurePage(storageAccountName, storageAccountKey, containerName)
	return
}
func getFileToBlockPipelines() (source []pipeline.SourcePipeline, target pipeline.TargetPipeline) {
	//Since this is default value detect if the source is http and change th transfer type
	if isSourceHTTP() {
		source = []pipeline.SourcePipeline{sources.NewHTTP(sourceURIs, blobNames, calculateMD5)}
		transferDefStr = transfer.HTTPToBlock
	} else {
		params := &sources.MultiFileParams{
			SourcePatterns:   sourceURIs,
			BlockSize:        blockSize,
			TargetAliases:    blobNames,
			NumOfPartitions:  numberOfReaders,
			MD5:              calculateMD5,
			KeepDirStructure: keepDirStructure,
			FilesPerPipeline: numberOfFilesInBatch}

		source = sources.NewMultiFile(params)
	}
	target = targets.NewAzureBlock(storageAccountName, storageAccountKey, containerName)
	return
}
func getHTTPToPagePipelines() (source []pipeline.SourcePipeline, target pipeline.TargetPipeline) {
	source = []pipeline.SourcePipeline{sources.NewHTTP(sourceURIs, blobNames, calculateMD5)}
	target = targets.NewAzurePage(storageAccountName, storageAccountKey, containerName)
	return
}
func getHTTPToBlockPipelines() (source []pipeline.SourcePipeline, target pipeline.TargetPipeline) {
	source = []pipeline.SourcePipeline{sources.NewHTTP(sourceURIs, blobNames, calculateMD5)}
	target = targets.NewAzureBlock(storageAccountName, storageAccountKey, containerName)
	return
}
func getBlobToPagePipelines() (source []pipeline.SourcePipeline, target pipeline.TargetPipeline) {

	params := &sources.AzureBlobParams{
		Container:         sourceKeyInfo["CONTAINER"],
		BlobNames:         blobNames,
		AccountName:       sourceKeyInfo["ACCOUNT_NAME"],
		AccountKey:        sourceAuthorization,
		CalculateMD5:      calculateMD5,
		UseExactNameMatch: exactNameMatch,
		FilesPerPipeline:  numberOfFilesInBatch,
		//default to always true so blob names are kept
		KeepDirStructure: true}

	source = sources.NewAzureBlob(params)
	target = targets.NewAzurePage(storageAccountName, storageAccountKey, containerName)
	return
}
func getBlobToBlockPipelines() (source []pipeline.SourcePipeline, target pipeline.TargetPipeline) {

	blobNames = []string{sourceKeyInfo["PREFIX"]}

	params := &sources.AzureBlobParams{
		Container:         sourceKeyInfo["CONTAINER"],
		BlobNames:         blobNames,
		AccountName:       sourceKeyInfo["ACCOUNT_NAME"],
		AccountKey:        sourceAuthorization,
		CalculateMD5:      calculateMD5,
		UseExactNameMatch: exactNameMatch,
		FilesPerPipeline:  numberOfFilesInBatch,
		//default to always true so blob names are kept
		KeepDirStructure: true}

	source = sources.NewAzureBlob(params)
	target = targets.NewAzureBlock(storageAccountName, storageAccountKey, containerName)
	return
}
func getBlobToFilePipelines() (source []pipeline.SourcePipeline, target pipeline.TargetPipeline) {

	if len(blobNames) == 0 {
		//use empty prefix to get the bloblist
		blobNames = []string{""}
	}

	params := &sources.AzureBlobParams{
		Container:         containerName,
		BlobNames:         blobNames,
		AccountName:       storageAccountName,
		AccountKey:        storageAccountKey,
		CalculateMD5:      calculateMD5,
		UseExactNameMatch: exactNameMatch,
		FilesPerPipeline:  numberOfFilesInBatch,
		KeepDirStructure:  keepDirStructure}

	source = sources.NewAzureBlob(params)
	target = targets.NewMultiFile(true, numberOfHandlesPerFile)
	return
}
func getHTTPToFilePipelines() (source []pipeline.SourcePipeline, target pipeline.TargetPipeline) {
	source = []pipeline.SourcePipeline{sources.NewHTTP(sourceURIs, blobNames, calculateMD5)}

	var targetAliases []string
	var err error

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

	target = targets.NewMultiFile(true, numberOfHandlesPerFile)
	return
}

// parseAndValidate - extra checks on command line arguments
func parseAndValidate() {

	flag.Parse()
	var err error
	if transferType, err = transfer.ParseTransferDefinition(transferDefStr); err != nil {
		log.Fatal(err)
	}

	if numberOfFilesInBatch < 1 {
		log.Fatal("Invalid value for option -x, the value must be greater than 1")
	}

	if numberOfHandlesPerFile < 1 {
		log.Fatal("Invalid value for option -h, the value must be greater than 1")
	}

	if util.HTTPClientTimeout < 30 {
		fmt.Printf("Warning! The storage HTTP client timeout is too low (>5). Setting value to 600s \n")
		util.HTTPClientTimeout = 600
	}

	
	dedupeLevel, err = transfer.ParseDupeCheckLevel(dedupeLevelOptStr)
	if err != nil {
		log.Fatalf("Duplicate detection level is invalid.  Found '%s', must be one of %s", dedupeLevelOptStr, transfer.DupeCheckLevelStr)
	}

}

//Validation rules...
type parseAndvalidationRule func() error

func pvBlockSizeCheckForBlockBlobs() error {
	var err error
	blockSize, err = util.ByteCountFromSizeString(blockSizeStr)
	if err != nil {
		return fmt.Errorf("Invalid block size specified: %v. Parse Error: %v ", blockSizeStr, err)
	}

	blockSizeMax := util.LargeBlockSizeMax
	if blockSize > blockSizeMax {
		return fmt.Errorf("Block size specified (%v) exceeds maximum of %v", blockSizeStr, util.PrintSize(blockSizeMax))
	}

	return nil

}

func pvIsSourceHTTP() error {
	if !isSourceHTTP() {
		return fmt.Errorf("The source is an invalid HTTP endpoint")
	}
	return nil
}

func pvBlockSizeCheckForPageBlobs() error {
	if blockSize%uint64(targets.PageSize) != 0 {
		return fmt.Errorf("Invalid block size (%v) for a page blob. The size must be a multiple of %v (bytes) and less or equal to %v (4MB)", blockSize, targets.PageSize, 4*util.MB)
	}

	if blockSize > 4*util.MB {
		//adjust the block size to 4 MB
		blockSize = 4 * util.MB

	}

	return nil
}

func pvSourceURIISReq() error {
	if len(sourceURIs) == 0 || sourceURIs == nil {
		return fmt.Errorf("The source parameter is missing (-f).\nMust be a file, URL, Azure Blob URL or file pattern (e.g. /data/*.fastq) ")
	}
	return nil
}

func pvSourceInfoForBlobIsReq() error {
	burl, err := url.Parse(sourceURIs[0])

	if err != nil {
		return fmt.Errorf("Invalid Blob URL. Parsing error: %v", err)
	}

	host := strings.Split(burl.Hostname(), ".")

	sourceAccountName := host[0]

	if sourceAccountName == "" {
		return fmt.Errorf("Invalid source Azure Blob URL. Account name could be parsed from the domain")
	}

	segments := strings.Split(burl.Path, "/")

	sourceContName := segments[1]

	if sourceContName == "" {
		return fmt.Errorf("Invalid source Azure Blob URL. A container is required")
	}
	sourceBlobName := ""
	if len(segments) > 1 {
		sourceBlobName = strings.Join(segments[2:len(segments)], "/")
	}

	sourceKeyInfo = make(map[string]string)

	sourceKeyInfo["ACCOUNT_NAME"] = sourceAccountName
	sourceKeyInfo["CONTAINER"] = sourceContName
	sourceKeyInfo["PREFIX"] = sourceBlobName

	if sourceAuthorization == "" {
		envVal := os.Getenv(sourceAuthorizationEnvVar)
		if envVal == "" {
			return fmt.Errorf("The source storage acccount key is required for this transfer type")
		}
		sourceAuthorization = envVal
	}

	return nil
}

func pvContainerIsReq() error {
	if containerName == "" {
		return fmt.Errorf("container name not specified ")
	}
	return nil
}

func pvBlobAuthInfoIsReq() error {

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

	return nil
}

//validates command line params considering the type of transfer
func validateTransferTypesParams() error {
	var transt transfer.Definition
	var err error
	if transt, err = transfer.ParseTransferDefinition(transferDefStr); err != nil {
		return err
	}

	if transt == transfer.HTTPToPage || transt == transfer.FileToPage {
		if blockSize%uint64(targets.PageSize) != 0 {
			return fmt.Errorf("Invalid block size (%v) for a page blob. The size must be a multiple of %v (bytes) and less or equal to %v (4MB)", blockSize, targets.PageSize, 4*util.MB)
		}

		if blockSize > 4*util.MB {
			//adjust the block size to 4 MB
			blockSize = 4 * util.MB

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
