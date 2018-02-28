package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"net/url"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/Azure/blobporter/internal"
	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/transfer"
	"github.com/Azure/blobporter/util"
)

var argsUtil paramParserValidator

func init() {

	//Show blobporter banner
	fmt.Printf("BlobPorter \nCopyright (c) Microsoft Corporation. \nVersion: %v\n---------------\n", internal.ProgramVersion)

	argsUtil = newParamParserValidator()

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
		removeDirStructureMsg      = "If set the directory structure from the source is not kept.\n\tNot applicable when the source is a HTTP endpoint."
		numberOfHandlersPerFileMsg = "Number of open handles for concurrent reads and writes per file."
		numberOfFilesInBatchMsg    = "Maximum number of files in a transfer.\n\tIf the number is exceeded new transfers are created"
		readTokenExpMsg            = "Expiration in minutes of the read-only access token that will be generated to read from S3 or Azure Blob sources."
	)

	flag.Usage = func() {
		util.PrintUsageDefaults("f", "source_file", "", fileMsg)
		util.PrintUsageDefaults("n", "name", "", nameMsg)
		util.PrintUsageDefaults("c", "container_name", "", containerNameMsg)
		util.PrintUsageDefaults("g", "concurrent_workers", strconv.Itoa(argsUtil.args.numberOfWorkers), concurrentWorkersMsg)
		util.PrintUsageDefaults("r", "concurrent_readers", strconv.Itoa(argsUtil.args.numberOfReaders), concurrentReadersMsg)
		util.PrintUsageDefaults("b", "block_size", argsUtil.args.blockSizeStr, blockSizeMsg)
		util.PrintUsageDefaults("v", "verbose", "false", verboseMsg)
		util.PrintUsageDefaults("q", "quiet_mode", "false", quietModeMsg)
		util.PrintUsageDefaults("m", "compute_blockmd5", "false", computeBlockMD5Msg)
		util.PrintUsageDefaults("s", "http_timeout", strconv.Itoa(argsUtil.args.hTTPClientTimeout), httpTimeoutMsg)
		util.PrintUsageDefaults("a", "account_name", "", accountNameMsg)
		util.PrintUsageDefaults("k", "account_key", "", accountKeyMsg)
		util.PrintUsageDefaults("d", "dup_check_level", argsUtil.args.dedupeLevelOptStr, dupcheckLevelMsg)
		util.PrintUsageDefaults("t", "transfer_definition", argsUtil.args.transferDefStr, transferDefMsg)
		util.PrintUsageDefaults("e", "exact_name", "false", exactNameMatchMsg)
		util.PrintUsageDefaults("i", "remove_directories", "false", removeDirStructureMsg)
		util.PrintUsageDefaults("h", "handles_per_file", strconv.Itoa(argsUtil.args.numberOfHandlesPerFile), numberOfHandlersPerFileMsg)
		util.PrintUsageDefaults("x", "files_per_transfer", strconv.Itoa(argsUtil.args.numberOfFilesInBatch), numberOfFilesInBatchMsg)
		util.PrintUsageDefaults("o", "read_token_exp", strconv.Itoa(defaultReadTokenExp), readTokenExpMsg)
	}

	util.StringListVarAlias(&argsUtil.args.sourceURIs, "f", "source_file", "", fileMsg)
	util.StringListVarAlias(&argsUtil.args.blobNames, "n", "name", "", nameMsg)
	util.StringVarAlias(&argsUtil.args.containerName, "c", "container_name", "", containerNameMsg)
	util.IntVarAlias(&argsUtil.args.numberOfWorkers, "g", "concurrent_workers", argsUtil.args.numberOfWorkers, concurrentWorkersMsg)
	util.IntVarAlias(&argsUtil.args.numberOfReaders, "r", "concurrent_readers", argsUtil.args.numberOfReaders, concurrentReadersMsg)
	util.StringVarAlias(&argsUtil.args.blockSizeStr, "b", "block_size", argsUtil.args.blockSizeStr, blockSizeMsg)
	util.BoolVarAlias(&util.Verbose, "v", "verbose", false, verboseMsg)
	util.BoolVarAlias(&argsUtil.args.quietMode, "q", "quiet_mode", false, quietModeMsg)
	util.BoolVarAlias(&argsUtil.args.calculateMD5, "m", "compute_blockmd5", false, computeBlockMD5Msg)
	util.IntVarAlias(&argsUtil.args.hTTPClientTimeout, "s", "http_timeout", argsUtil.args.hTTPClientTimeout, httpTimeoutMsg)
	util.StringVarAlias(&argsUtil.args.storageAccountName, "a", "account_name", "", accountNameMsg)
	util.StringVarAlias(&argsUtil.args.storageAccountKey, "k", "account_key", "", accountKeyMsg)
	util.StringVarAlias(&argsUtil.args.dedupeLevelOptStr, "d", "dup_check_level", argsUtil.args.dedupeLevelOptStr, dupcheckLevelMsg)
	util.StringVarAlias(&argsUtil.args.transferDefStr, "t", "transfer_definition", argsUtil.args.transferDefStr, transferDefMsg)
	util.BoolVarAlias(&argsUtil.args.exactNameMatch, "e", "exact_name", false, exactNameMatchMsg)
	util.BoolVarAlias(&argsUtil.args.removeDirStructure, "i", "remove_directories", false, removeDirStructureMsg)
	util.IntVarAlias(&argsUtil.args.numberOfHandlesPerFile, "h", "handles_per_file", defaultNumberOfHandlesPerFile, numberOfHandlersPerFileMsg)
	util.IntVarAlias(&argsUtil.args.numberOfFilesInBatch, "x", "files_per_transfer", defaultNumberOfFilesInBatch, numberOfFilesInBatchMsg)
	util.IntVarAlias(&argsUtil.args.readTokenExp, "o", "read_token_exp", defaultReadTokenExp, readTokenExpMsg)

}

var dataTransferred uint64
var targetRetries int32

func displayFilesToTransfer(sourcesInfo []pipeline.SourceInfo) {
	fmt.Printf("\nFiles to Transfer (%v) :\n", argsUtil.params.transferType)
	var totalSize uint64
	summary := ""

	for _, source := range sourcesInfo {
		//if the source is URL, remove the QS
		display := source.SourceName
		if u, err := url.Parse(source.SourceName); err == nil {
			display = fmt.Sprintf("%v%v", u.Hostname(), u.Path)
		}
		summary = summary + fmt.Sprintf("Source: %v Size:%v \n", display, source.Size)
		totalSize = totalSize + source.Size
	}

	if len(sourcesInfo) < 10 {
		fmt.Printf(summary)
		return
	}

	fmt.Printf("%v files. Total size:%v\n", len(sourcesInfo), totalSize)

	return
}

func main() {

	flag.Parse()

	if err := argsUtil.parseAndValidate(); err != nil {
		log.Fatal(err)
	}

	//Create pipelines
	sourcePipelines, targetPipeline, err := newTransferPipelines(argsUtil.params)

	if err != nil {
		log.Fatal(err)
	}

	stats := transfer.NewStats(argsUtil.params.numberOfWorkers, argsUtil.params.numberOfReaders)

	for sourcePipeline := range sourcePipelines {

		if sourcePipeline.Err != nil {
			log.Fatal(sourcePipeline.Err)
		}

		sourcesInfo := sourcePipeline.Source.GetSourcesInfo()

		tfer := transfer.NewTransfer(&sourcePipeline.Source, &targetPipeline, argsUtil.params.numberOfReaders, argsUtil.params.numberOfWorkers, argsUtil.params.blockSize)

		displayFilesToTransfer(sourcesInfo)
		pb := getProgressBarDelegate(tfer.TotalSize, argsUtil.params.quietMode)

		tfer.StartTransfer(argsUtil.params.dedupeLevel, pb)

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
