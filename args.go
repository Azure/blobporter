package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strings"

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

//defautls for some of the arguments and parameters...
const defaulPreSignedExpMins = 90
const numOfWorkersFactor = 8
const numOfReadersFactor = 5
const defaultNumberOfFilesInBatch = 500
const defaultNumberOfHandlesPerFile = 2
const defaultHTTPClientTimeout = 30
const defaultBlockSizeStr = "8MB"
const defaultDedupeLevelStr = "None"

//sets the parameters by parsing and validating the arguments
type paramParserValidator struct {
	args          *arguments
	params        *validatedParameters
	sourceSegment transfer.TransferSegment
	targetSegment transfer.TransferSegment
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
	targetBaseBlobURL        string
	sourceParameters         map[string]string
	sourceAuthorization      string
	quietMode                bool
	calculateMD5             bool
	exactNameMatch           bool
	keepDirStructure         bool
	hTTPClientTimeout        int
	numberOfHandlesPerFile   int //numberOfHandlesPerFile = defaultNumberOfHandlesPerFile
	numberOfFilesInBatch     int //numberOfFilesInBatch = defaultNumberOfFilesInBatch
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
	accountName string
	accountKey  string
	container   string
	prefixes    []string
	baseBlobURL string
}

func (b blobParams) isSourceAuthAndContainerInfoSet() bool {
	return b.accountName != "" && b.accountKey != "" && b.container != ""
}

//creates a new param parser validator, with default values for some paramater and arguments
func newParamParserValidator() paramParserValidator {

	var defaultNumberOfWorkers = runtime.NumCPU() * numOfWorkersFactor
	var defaultNumberOfReaders = runtime.NumCPU() * numOfReadersFactor

	if defaultNumberOfWorkers > 50 {
		defaultNumberOfWorkers = 50
	}
	if defaultNumberOfReaders > 80 {
		defaultNumberOfReaders = 80
	}

	args := &arguments{
		keepDirStructure: 		true,
		numberOfReaders:        defaultNumberOfReaders,
		numberOfWorkers:        defaultNumberOfWorkers,
		blockSizeStr:           defaultBlockSizeStr,
		dedupeLevelOptStr:      defaultDedupeLevelStr,
		transferDefStr:         string(transfer.FileToBlock),
		numberOfHandlesPerFile: defaultNumberOfHandlesPerFile,
		hTTPClientTimeout:      defaultHTTPClientTimeout,
		numberOfFilesInBatch:   defaultNumberOfFilesInBatch}
	params := &validatedParameters{
		s3Source: s3Source{
			preSignedExpMin: defaulPreSignedExpMins},
		blobSource: blobParams{},
		blobTarget: blobParams{}}

	p := paramParserValidator{args: args, params: params}

	return p
}

func (p *paramParserValidator) parseAndValidate() error {
	//Run global rules.. this will set the transfer type
	err := p.pvgTransferType()
	if err != nil {
		return err
	}
	s, t := transfer.ParseTransferSegment(p.params.transferType)
	p.sourceSegment = s
	p.targetSegment = t
	err = p.runParseAndValidationRules(
		p.pvgCalculateReadersAndWorkers,
		p.pvgBatchLimits,
		p.pvgHTTPTimeOut,
		p.pvgDupCheck,
		p.pvgParseBlockSize,
		p.pvgQuietMode,
		p.pvgKeepDirectoryStructure)

	if err != nil {
		return err
	}

	//get and run target rules...
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
			p.pvNumOfHandlesPerFile}, nil
	case transfer.PageBlob:
		return []parseAndValidationRule{
			p.pvTargetContainerIsReq,
			p.pvBlockSizeCheckForPageBlobs,
			p.pvTargetBlobAuthInfoIsReq}, nil
	case transfer.BlockBlob:
		return []parseAndValidationRule{
			p.pvTargetContainerIsReq,
			p.pvBlockSizeCheckForBlockBlobs,
			p.pvTargetBlobAuthInfoIsReq}, nil
	case transfer.Perf:
		return []parseAndValidationRule{
			p.pvPerfSourceIsReq}, nil
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
func (p *paramParserValidator) pvgKeepDirectoryStructure() error {
	p.params.keepDirStructure = p.args.keepDirStructure
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
		fmt.Printf("Warning! The storage HTTP client timeout is too low (<30). Setting value to default (%v)s \n", defaultHTTPClientTimeout)
		p.args.hTTPClientTimeout = defaultHTTPClientTimeout
	}
	util.HTTPClientTimeout = p.args.hTTPClientTimeout
	return nil
}
func (p *paramParserValidator) pvgDupCheck() error {
	var err error
	p.params.dedupeLevel, err = transfer.ParseDupeCheckLevel(p.args.dedupeLevelOptStr)
	if err != nil {
		fmt.Errorf("Duplicate detection level is invalid.  Found '%s', must be one of %s. Error:%v", p.args.dedupeLevelOptStr, transfer.DupeCheckLevelStr, err)
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
		//if empty set an empty prefix so the entire container is downlaoded..
		p.params.blobSource.prefixes = []string{""}
	}

	return nil
}

//this rule checks if the transfer type is blob to file (download). Im which case blob authorization rule also aplies since
//there are two combinations of param line options that can be provided. One, similar to upload, where the source is main
// storage account that is the target in all other cases. And the second with the URI provided, as when blob to blob transfers occurr.
func (p *paramParserValidator) pvSourceInfoForBlobIsReq() error {

	//if the scenarios is download then check if download is via short-mode
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

		if p.params.blobSource.isSourceAuthAndContainerInfoSet() {
			return nil
		}
	}

	err := p.pvSourceURIISReq()

	if err != nil {
		return err
	}

	var burl *url.URL
	burl, err = url.Parse(p.params.sourceURIs[0])

	if err != nil {
		return fmt.Errorf("Invalid Blob URL. Parsing error: %v", err)
	}

	host := strings.Split(burl.Hostname(), ".")

	p.params.blobSource.accountName = host[0]

	if p.params.blobSource.accountName == "" {
		return fmt.Errorf("Invalid source Azure Blob URL. Account name could be parsed from the domain")
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

	return nil
}

func (p *paramParserValidator) pvSourceInfoForS3IsReq() error {
	burl, err := url.Parse(p.params.sourceURIs[0])

	if err != nil {
		return fmt.Errorf("Invalid S3 endpoint URL. Parsing error: %v.\nThe format is s3://[END_POINT]/[BUCKET]/[OBJECT]", err)
	}

	p.params.s3Source.endpoint = burl.Hostname()

	if p.params.s3Source.endpoint == "" {
		return fmt.Errorf("Invalid source S3 URI. The endpoint is required")
	}

	segments := strings.Split(burl.Path, "/")

	p.params.s3Source.bucket = segments[1]

	if p.params.s3Source.bucket == "" {
		return fmt.Errorf("Invalid source S3 URI. Bucket name could be parsed")
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

func (p *paramParserValidator) runParseAndValidationRules(rules ...parseAndValidationRule) error {
	for i := 0; i < len(rules); i++ {
		val := rules[i]
		if err := val(); err != nil {
			return err
		}
	}
	return nil
}
