package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strings"

	"github.com/Azure/blobporter/internal"
	"github.com/Azure/blobporter/sources"
	"github.com/Azure/blobporter/targets"
	"github.com/Azure/blobporter/transfer"
	"github.com/Azure/blobporter/util"
)

//user env names
const (
	// User can use environment variables to specify storage account information
	storageAccountNameEnvVar  = "ACCOUNT_NAME"
	storageAccountKeyEnvVar   = "ACCOUNT_KEY"
	sourceAuthorizationEnvVar = "SRC_ACCOUNT_KEY"
	//S3 creds information
	s3AccessKeyEnvVar = "S3_ACCESS_KEY"
	s3SecretKeyEnvVar = "S3_SECRET_KEY"
)

//defaults for some of the arguments and parameters...
const defaulPreSignedExpMins = 360
const numOfWorkersFactor = 8
const numOfReadersFactor = 5
const defaultNumberOfFilesInBatch = 500
const defaultNumberOfHandlesPerFile = 2
const defaultHTTPClientTimeout = 60
const defaultBlockSizeStr = "8MB"
const defaultDedupeLevelStr = "None"
const defaultReadTokenExp = 360

//sets the parameters by parsing and validating the arguments
type paramParserValidator struct {
	args          *arguments
	params        *validatedParameters
	sourceSegment transfer.TransferSegment
	targetSegment transfer.TransferSegment
	defaultValues defaults
}

type defaults struct {
	defaultNumberOfReaders int
	defaultNumberOfWorkers int
}

//respresents raw (not validated) the list of options/flags as received from the user
type arguments struct {
	containerName            string
	blobNames                util.ListFlag
	sourceURIs               util.ListFlag
	numberOfWorkers          int
	blockSizeStr             string
	numberOfReaders          int
	dedupeLevelOptStr        string //dedupeLevelOptStr = dedupeLevel.ToString()
	transferDef              transfer.Definition
	transferDefStr           string
	defaultTransferDef       transfer.Definition //defaultTransferDef = transfer.FileToBlock
	storageAccountName       string
	storageAccountKey        string
	storageClientHTTPTimeout int
	baseBlobURL              string
	sourceParameters         map[string]string
	sourceAuthorization      string
	quietMode                bool
	calculateMD5             bool
	exactNameMatch           bool
	removeDirStructure       bool
	hTTPClientTimeout        int
	readTokenExp             int
	numberOfHandlesPerFile   int //numberOfHandlesPerFile = defaultNumberOfHandlesPerFile
	numberOfFilesInBatch     int //numberOfFilesInBatch = defaultNumberOfFilesInBatch
	transferStatusPath       string
}

//represents validated parameters
type validatedParameters struct {
	s3Source
	numberOfWorkers        int
	numberOfReaders        int
	quietMode              bool
	transferType           transfer.Definition
	dedupeLevel            transfer.DupeCheckLevel //dedupeLevel := transfer.None
	sourceURIs             []string
	blockSize              uint64
	targetAliases          []string
	numOfPartitions        int
	calculateMD5           bool
	keepDirStructure       bool
	filesPerPipeline       int
	useExactMatch          bool
	numberOfHandlesPerFile int
	numberOfFilesInBatch   int
	blobSource             blobParams
	blobTarget             blobParams
	perfSourceDefinitions  []sources.SourceDefinition
	tracker                *internal.TransferTracker
	referenceMode          bool
}

type s3Source struct {
	bucket          string
	prefixes        []string
	endpoint        string
	preSignedExpMin int    //preSignedExpMin: defaulPreSignedExpMins,
	accessKey       string //sourceParameters[s3AccessKeyEnvVar],
	secretKey       string //  sourceParameters[s3SecretKeyEnvVar],
}

type blobParams struct {
	accountName   string
	accountKey    string
	container     string
	prefixes      []string
	baseBlobURL   string
	sasExpMin     int
	useServerSide bool
}

func (b blobParams) isSourceAuthAndContainerInfoSet() bool {
	return b.accountName != "" && b.accountKey != "" && b.container != ""
}

//creates a new param parser validator, with default values for some paramater and arguments
func newParamParserValidator() paramParserValidator {

	var defaultNumberOfWorkers = runtime.NumCPU() * numOfWorkersFactor
	var defaultNumberOfReaders = runtime.NumCPU() * numOfReadersFactor

	if defaultNumberOfWorkers > 60 {
		defaultNumberOfWorkers = 60
	}
	if defaultNumberOfReaders > 50 {
		defaultNumberOfReaders = 50
	}

	args := &arguments{
		removeDirStructure:     false,
		numberOfReaders:        defaultNumberOfReaders,
		numberOfWorkers:        defaultNumberOfWorkers,
		blockSizeStr:           defaultBlockSizeStr,
		dedupeLevelOptStr:      defaultDedupeLevelStr,
		readTokenExp:           defaultReadTokenExp,
		transferDefStr:         string(transfer.FileToBlock),
		numberOfHandlesPerFile: defaultNumberOfHandlesPerFile,
		hTTPClientTimeout:      defaultHTTPClientTimeout,
		numberOfFilesInBatch:   defaultNumberOfFilesInBatch,
	}
	params := &validatedParameters{
		s3Source: s3Source{
			preSignedExpMin: defaultReadTokenExp},
		blobSource: blobParams{
			sasExpMin: defaultReadTokenExp},
		blobTarget: blobParams{}}

	p := paramParserValidator{args: args,
		params: params,
		defaultValues: defaults{
			defaultNumberOfReaders: defaultNumberOfReaders,
			defaultNumberOfWorkers: defaultNumberOfWorkers,
		},
	}

	return p
}

func (p *paramParserValidator) parseAndValidate() error {
	//Run global rules.. this will set the transfer type which is needed in some of the global rules.
	err := p.pvgTransferType()
	if err != nil {
		return err
	}
	s, t := transfer.ParseTransferSegment(p.params.transferType)
	p.sourceSegment = s
	p.targetSegment = t
	err = p.runParseAndValidationRules(
		p.pvgSetReferenceModeAndServerSide,
		p.pvgCalculateReadersAndWorkers,
		p.pvgTransferStatusPathIsPresent,
		p.pvgBatchLimits,
		p.pvgHTTPTimeOut,
		p.pvgDupCheck,
		p.pvgParseBlockSize,
		p.pvgQuietMode,
		p.pvgKeepDirectoryStructure,
		p.pvgUseExactMatch)

	if err != nil {
		return err
	}

	//get and run source and target rules...
	var rules []parseAndValidationRule
	if rules, err = p.getSourceRules(); err != nil {
		return err
	}

	if err = p.runParseAndValidationRules(rules...); err != nil {
		return err
	}

	if rules, err = p.getTargetRules(); err != nil {
		return err
	}

	return p.runParseAndValidationRules(rules...)
}
func (p *paramParserValidator) getTargetRules() ([]parseAndValidationRule, error) {

	switch p.targetSegment {
	case transfer.File:
		return []parseAndValidationRule{
			p.pvNumOfHandlesPerFile,
			p.pvKeepDirStructueIsFalseWarning}, nil
	case transfer.PageBlob:
		return []parseAndValidationRule{
			p.pvTargetBaseURL,
			p.pvTargetContainerIsReq,
			p.pvBlockSizeCheckForPageBlobs,
			p.pvTargetBlobAuthInfoIsReq}, nil
	case transfer.BlockBlob:
		return []parseAndValidationRule{
			p.pvTargetBaseURL,
			p.pvTargetContainerIsReq,
			p.pvBlockSizeCheckForBlockBlobs,
			p.pvTargetBlobAuthInfoIsReq}, nil
	case transfer.Perf:
		return []parseAndValidationRule{}, nil
	}

	return nil, fmt.Errorf("Invalid target segment type: %v ", p.targetSegment)
}

func (p *paramParserValidator) getSourceRules() ([]parseAndValidationRule, error) {

	switch p.sourceSegment {
	case transfer.File:
		return []parseAndValidationRule{
			p.pvSourceURIISReq,
			p.pvSetTargetAliases}, nil
	case transfer.HTTP:
		return []parseAndValidationRule{
			p.pvSourceURIISReq,
			p.pvIsSourceHTTP,
			p.pvSetTargetAliases}, nil
	case transfer.S3:
		return []parseAndValidationRule{
			p.pvSourceURIISReq,
			p.pvSourceInfoForS3IsReq}, nil
	case transfer.Blob:
		return []parseAndValidationRule{
			p.pvSourceBaseURL,
			p.pvSourceInfoForBlobIsReq,
			p.pvSetEmptyPrefixIfNone}, nil
	case transfer.Perf:
		return []parseAndValidationRule{
			p.pvSourceURIISReq,
			p.pvPerfSourceIsReq}, nil
	}

	return nil, fmt.Errorf("Invalid source segment type: %v ", p.sourceSegment)

}

//**************************
//Parsing and validation rules...
//These rules validate and prepare the parameters to construct the pipelines
// and are applied for all transfers or for an specific one.
//**************************

//Global rules....
func (p *paramParserValidator) pvgSetReferenceModeAndServerSide() error {
	if p.targetSegment == transfer.BlockBlob {
		if p.sourceSegment == transfer.Blob {
			p.params.blobTarget.useServerSide = true
			p.params.referenceMode = true
		}
	}
	return nil
}

func (p *paramParserValidator) pvgUseExactMatch() error {
	p.params.useExactMatch = p.args.exactNameMatch
	return nil
}

func (p *paramParserValidator) pvgTransferStatusPathIsPresent() error {

	if p.args.transferStatusPath != "" {
		if !p.args.quietMode {
			fmt.Printf("Transfer is resumable. Transfer status file:%v \n", p.args.transferStatusPath)
		}
		tracker, err := internal.NewTransferTracker(p.args.transferStatusPath)

		if err != nil {
			return err
		}

		p.params.tracker = tracker
	}
	return nil
}
func (p *paramParserValidator) pvgKeepDirectoryStructure() error {
	p.params.keepDirStructure = !p.args.removeDirStructure
	return nil
}
func (p *paramParserValidator) pvgQuietMode() error {
	p.params.quietMode = p.args.quietMode
	return nil
}

func (p *paramParserValidator) pvgCalculateReadersAndWorkers() error {

	if p.args.numberOfWorkers <= 0 {
		return fmt.Errorf("Invalid number of workers (-g):%v", p.args.numberOfWorkers)
	}
	if p.args.numberOfReaders <= 0 {
		return fmt.Errorf("Invalid number of readers (-r):%v", p.args.numberOfReaders)
	}

	p.params.numberOfWorkers = p.args.numberOfWorkers
	p.params.numberOfReaders = p.args.numberOfReaders

	//if using the default parameters and the transfer is blob-blockblob change
	// params to maximes the transfer considering that this will be server-side sync
	if p.defaultValues.defaultNumberOfReaders == p.params.numberOfReaders &&
		p.defaultValues.defaultNumberOfWorkers == p.params.numberOfWorkers {

		if p.params.transferType == transfer.BlobToBlock {
			workers := 75 * runtime.NumCPU()

			if workers > 200 {
				workers = 200
			}
			p.params.numberOfWorkers = workers
			p.params.numberOfReaders = 4
		}

	}

	return nil

}
func (p *paramParserValidator) pvgParseBlockSize() error {
	var err error
	p.params.blockSize, err = util.ByteCountFromSizeString(p.args.blockSizeStr)
	if err != nil || p.params.blockSize <= 0 {
		return fmt.Errorf("Invalid block size specified: %v. Parse Error: %v ", p.args.blockSizeStr, err)
	}

	return nil
}
func (p *paramParserValidator) pvgBatchLimits() error {
	if p.args.numberOfFilesInBatch < 1 {
		return fmt.Errorf("Invalid value for option -x, the value must be greater than 1")
	}
	p.params.numberOfFilesInBatch = p.args.numberOfFilesInBatch
	return nil
}
func (p *paramParserValidator) pvgHTTPTimeOut() error {
	if p.args.hTTPClientTimeout < defaultHTTPClientTimeout {
		fmt.Printf("Warning! The HTTP timeout is too low (<30). Setting value to default (%v)s \n", defaultHTTPClientTimeout)
		p.args.hTTPClientTimeout = defaultHTTPClientTimeout
	}
	internal.HTTPClientTimeout = p.args.hTTPClientTimeout
	return nil
}
func (p *paramParserValidator) pvgDupCheck() error {
	var err error
	p.params.dedupeLevel, err = transfer.ParseDupeCheckLevel(p.args.dedupeLevelOptStr)
	if err != nil {
		return fmt.Errorf("Duplicate detection level is invalid.  Found '%s', must be one of %s. Error:%v", p.args.dedupeLevelOptStr, transfer.DupeCheckLevelStr, err)
	}

	return nil
}
func (p *paramParserValidator) pvgTransferType() error {
	var err error
	if p.params.transferType, err = transfer.ParseTransferDefinition(p.args.transferDefStr); err != nil {
		return err
	}
	return nil
}

//Transfer segment specific rules...
func (p *paramParserValidator) pvSourceBaseURL() error {
	p.params.blobSource.baseBlobURL = p.args.baseBlobURL

	return nil
}
func (p *paramParserValidator) pvTargetBaseURL() error {
	p.params.blobTarget.baseBlobURL = p.args.baseBlobURL

	return nil
}
func (p *paramParserValidator) pvSetTargetAliases() error {
	p.params.targetAliases = p.args.blobNames

	return nil
}

func (p *paramParserValidator) pvNumOfHandlesPerFile() error {
	if p.args.numberOfHandlesPerFile < 1 {
		return fmt.Errorf("Invalid value for option -h, the value must be greater than 1")
	}

	p.params.numberOfHandlesPerFile = p.args.numberOfHandlesPerFile

	return nil
}

func (p *paramParserValidator) pvBlockSizeCheckForBlockBlobs() error {

	blockSizeMax := util.LargeBlockSizeMax
	if p.params.blockSize > blockSizeMax {
		return fmt.Errorf("Block size specified (%v) exceeds maximum of %v", p.params.blockSize, util.PrintSize(blockSizeMax))
	}

	return nil
}

func (p *paramParserValidator) pvIsSourceHTTP() error {

	url, err := url.Parse(p.params.sourceURIs[0])

	if err != nil {
		return err
	}

	if strings.ToLower(url.Scheme) != "http" && strings.ToLower(url.Scheme) != "https" {
		return fmt.Errorf("Invalid scheme. Expected http or https. Scheme: %v", url.Scheme)
	}

	return nil
}

func (p *paramParserValidator) pvBlockSizeCheckForPageBlobs() error {
	if p.params.blockSize%uint64(targets.PageSize) != 0 {
		return fmt.Errorf("Invalid block size (%v) for a page blob. The size must be a multiple of %v (bytes) and less or equal to %v (4MB)", p.params.blockSize, targets.PageSize, 4*util.MB)
	}

	if p.params.blockSize > 4*util.MB {
		//adjust the block size to 4 MB
		p.params.blockSize = 4 * util.MB
	}

	return nil
}

func (p *paramParserValidator) pvSourceURIISReq() error {
	if len(p.args.sourceURIs) == 0 || p.args.sourceURIs == nil {
		return fmt.Errorf("The source parameter is missing (-f).\nMust be a file, URL, Azure Blob URL or file pattern (e.g. /data/*.fastq) ")
	}

	p.params.sourceURIs = p.args.sourceURIs

	return nil
}

func (p *paramParserValidator) pvTargetContainerIsReq() error {
	if p.args.containerName == "" {
		return fmt.Errorf("container name not specified ")
	}

	p.params.blobTarget.container = p.args.containerName

	return nil
}

//assumes that one source URI exists as a validated parameter
//must follow pvSourceURIIsReq
func (p *paramParserValidator) pvPerfSourceIsReq() error {
	var err error
	p.params.perfSourceDefinitions, err = sources.ParseSourceDefinitions(p.params.sourceURIs[0])

	if err != nil {
		return err
	}

	return nil
}

func (p *paramParserValidator) pvSetEmptyPrefixIfNone() error {

	if len(p.params.blobSource.prefixes) == 0 {
		//if empty set an empty prefix so the entire container is downloaded..
		p.params.blobSource.prefixes = []string{""}
	}

	return nil
}

//this rule checks if the transfer type is blob to file (download). Im which case blob authorization rule also aplies since
//there are two combinations of param line options that can be provided. One, similar to upload, where the source is main
// storage account that is the target in all other cases. And the second with the URI provided, as when blob to blob transfers occur.
func (p *paramParserValidator) pvSourceInfoForBlobIsReq() error {

	//if the scenario is download check for short-mode download
	if p.params.transferType == transfer.BlobToFile {
		p.params.blobSource.accountName = p.args.storageAccountName
		if p.params.blobSource.accountName == "" {
			p.params.blobSource.accountName = os.Getenv(storageAccountNameEnvVar)
		}

		p.params.blobSource.accountKey = p.args.storageAccountKey
		if p.params.blobSource.accountKey == "" {
			p.params.blobSource.accountKey = os.Getenv(storageAccountKeyEnvVar)
		}

		p.params.blobSource.container = p.args.containerName

		p.params.blobSource.prefixes = p.args.blobNames

		if p.args.readTokenExp < 1 {
			return fmt.Errorf("Invalid read token expiration value. Minimum is 1 (1m)")
		}

		p.params.blobSource.sasExpMin = p.args.readTokenExp

		if p.params.blobSource.isSourceAuthAndContainerInfoSet() {
			return nil
		}
	}

	err := p.pvSourceURIISReq()

	if err != nil {
		return fmt.Errorf("%s \n Or the account name or key were not set", err)
	}

	var burl *url.URL
	burl, err = url.Parse(p.params.sourceURIs[0])

	if err != nil {
		return fmt.Errorf("Invalid Blob URL. Parsing error: %v", err)
	}

	host := strings.Split(burl.Hostname(), ".")

	p.params.blobSource.accountName = host[0]

	if p.params.blobSource.accountName == "" {
		return fmt.Errorf("Invalid source Azure Blob URL. Account name could not be parsed from the domain")
	}

	segments := strings.Split(burl.Path, "/")

	p.params.blobSource.container = segments[1]

	if p.params.blobSource.container == "" {
		return fmt.Errorf("Invalid source Azure Blob URL. A container is required")
	}

	sourceBlobName := ""
	if len(segments) > 1 {
		sourceBlobName = strings.Join(segments[2:len(segments)], "/")
	}

	p.params.blobSource.prefixes = []string{sourceBlobName}

	p.params.blobSource.accountKey = os.Getenv(sourceAuthorizationEnvVar)
	if p.params.blobSource.accountKey == "" {
		return fmt.Errorf("The source storage acccount key (env variable: %v) is required for this transfer type", sourceAuthorizationEnvVar)
	}

	if p.args.readTokenExp < 1 {
		return fmt.Errorf("Invalid read token expiration value. Minimum is 1 (1m)")
	}

	p.params.blobSource.sasExpMin = p.args.readTokenExp

	return nil
}

func (p *paramParserValidator) pvSourceInfoForS3IsReq() error {
	burl, err := url.Parse(p.params.sourceURIs[0])

	if err != nil {
		return fmt.Errorf("Invalid S3 endpoint URL. Parsing error: %v.\nThe format is s3://[END_POINT]/[BUCKET]/[PREFIX]", err)
	}

	p.params.s3Source.endpoint = burl.Hostname()

	if p.params.s3Source.endpoint == "" {
		return fmt.Errorf("Invalid source S3 URI. The endpoint is required")
	}

	segments := strings.Split(burl.Path, "/")

	if len(segments) < 2 {
		return fmt.Errorf("Invalid S3 endpoint URL. Bucket not specified. The format is s3://[END_POINT]/[BUCKET]/[PREFIX]")
	}

	p.params.s3Source.bucket = segments[1]

	if p.params.s3Source.bucket == "" {
		return fmt.Errorf("Invalid source S3 URI. Bucket name could not be parsed")
	}

	prefix := ""
	if len(segments) > 1 {
		prefix = strings.Join(segments[2:len(segments)], "/")
	}

	p.params.s3Source.prefixes = []string{prefix}

	p.params.s3Source.accessKey = os.Getenv(s3AccessKeyEnvVar)
	if p.params.s3Source.accessKey == "" {
		return fmt.Errorf("The S3 access key is required for this transfer type. Environment variable name:%v", s3AccessKeyEnvVar)
	}

	p.params.s3Source.secretKey = os.Getenv(s3SecretKeyEnvVar)

	if p.params.s3Source.secretKey == "" {
		return fmt.Errorf("The S3 secret key is required for this transfer type. Environment variable name:%v", s3SecretKeyEnvVar)
	}

	if p.args.readTokenExp < 1 {
		return fmt.Errorf("Invalid read token expiration value. Minimum is 1 (1m)")
	}

	p.params.s3Source.preSignedExpMin = p.args.readTokenExp

	return nil
}

func (p *paramParserValidator) pvTargetBlobAuthInfoIsReq() error {

	p.params.blobTarget.accountName = p.args.storageAccountName
	// wasn't specified, try the environment variable
	if p.params.blobTarget.accountName == "" {
		p.params.blobTarget.accountName = os.Getenv(storageAccountNameEnvVar)
		if p.params.blobTarget.accountName == "" {
			return errors.New("storage account name not specified or found in environment variable " + storageAccountNameEnvVar)
		}
	}

	p.params.blobTarget.accountKey = p.args.storageAccountKey
	// wasn't specified, try the environment variable
	if p.params.blobTarget.accountKey == "" {
		p.params.blobTarget.accountKey = os.Getenv(storageAccountKeyEnvVar)
		if p.params.blobTarget.accountKey == "" {
			return errors.New("storage account key not specified or found in environment variable " + storageAccountKeyEnvVar)
		}
	}
	return nil
}

func (p *paramParserValidator) pvKeepDirStructueIsFalseWarning() error {
	if !p.params.keepDirStructure && !p.params.quietMode {
		fmt.Printf("Warning!\nThe directory structure from the source won't be created.\nFiles with the same file name will be overwritten.\n")
	}

	return nil
}

func (p *paramParserValidator) runParseAndValidationRules(rules ...parseAndValidationRule) error {
	for i := 0; i < len(rules); i++ {
		val := rules[i]
		if err := val(); err != nil {
			return err
		}
	}
	return nil
}
