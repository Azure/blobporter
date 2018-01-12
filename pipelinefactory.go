package main

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"runtime"
	"strings"

	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/sources"
	"github.com/Azure/blobporter/targets"
	"github.com/Azure/blobporter/transfer"
	"github.com/Azure/blobporter/util"
)

type parseAndValidationRule func() error
type transferPipelinesFactory func() ([]pipeline.SourcePipeline, pipeline.TargetPipeline, error)

func isSourceHTTP() bool {

	url, err := url.Parse(sourceURIs[0])

	return err == nil && (strings.ToLower(url.Scheme) == "http" || strings.ToLower(url.Scheme) == "https")
}

func getTransferPipelines(pipelinesFactory transferPipelinesFactory, validationRules ...parseAndValidationRule) ([]pipeline.SourcePipeline, pipeline.TargetPipeline, error) {

	if err := runParseAndValidationRules(validationRules...); err != nil {
		return nil, nil, err
	}

	return pipelinesFactory()
}

func runParseAndValidationRules(rules ...parseAndValidationRule) error {
	for i := 0; i < len(rules); i++ {
		val := rules[i]
		if err := val(); err != nil {
			return err
		}
	}
	return nil
}

func getPipelines() ([]pipeline.SourcePipeline, pipeline.TargetPipeline, error) {

	//run global rules...
	err := runParseAndValidationRules(
		pvgTransferType,
		pvgBatchLimits,
		pvgHTTPTimeOut,
		pvgDupCheck,
		pvgParseBlockSize)

	if err != nil {
		return nil, nil, err
	}

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
			pvBlockSizeCheckForBlockBlobs,
			pvNumOfHandlesPerFile)
	case transfer.HTTPToFile:
		return getTransferPipelines(getHTTPToFilePipelines,
			pvSourceURIISReq,
			pvIsSourceHTTP,
			pvNumOfHandlesPerFile)
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
	case transfer.S3ToPage:
		return getTransferPipelines(getS3ToPagePipelines,
			pvBlobAuthInfoIsReq,
			pvSourceURIISReq,
			pvSourceInfoForS3IsReq,
			pvBlockSizeCheckForPageBlobs,
			pvContainerIsReq)
	case transfer.S3ToBlock:
		return getTransferPipelines(getS3ToBlockPipelines,
			pvBlobAuthInfoIsReq,
			pvSourceURIISReq,
			pvSourceInfoForS3IsReq,
			pvContainerIsReq,
			pvBlockSizeCheckForBlockBlobs)

	}

	return nil, nil, fmt.Errorf("Invalid transfer type: %v ", transferType)

	return nil, nil, nil
}
func getFileToPagePipelines() (source []pipeline.SourcePipeline, target pipeline.TargetPipeline, err error) {

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

	err = adjustReaders(len(source))

	return
}
func getFileToBlockPipelines() (source []pipeline.SourcePipeline, target pipeline.TargetPipeline, err error) {
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

	err = adjustReaders(len(source))

	return
}
func getHTTPToPagePipelines() (source []pipeline.SourcePipeline, target pipeline.TargetPipeline, err error) {
	source = []pipeline.SourcePipeline{sources.NewHTTP(sourceURIs, blobNames, calculateMD5)}
	target = targets.NewAzurePage(storageAccountName, storageAccountKey, containerName)
	return
}
func getHTTPToBlockPipelines() (source []pipeline.SourcePipeline, target pipeline.TargetPipeline, err error) {
	source = []pipeline.SourcePipeline{sources.NewHTTP(sourceURIs, blobNames, calculateMD5)}
	target = targets.NewAzureBlock(storageAccountName, storageAccountKey, containerName)
	return
}
func getS3ToPagePipelines() (source []pipeline.SourcePipeline, target pipeline.TargetPipeline, err error) {

	params := &sources.S3Params{
		Bucket:   sourceParameters["BUCKET"],
		Prefixes: []string{sourceParameters["PREFIX"]},
		Endpoint: sourceParameters["ENDPOINT"],
		Region:   sourceParameters[s3RegionEnvVar],
		SourceParams: sources.SourceParams{
			CalculateMD5:      calculateMD5,
			UseExactNameMatch: exactNameMatch,
			FilesPerPipeline:  numberOfFilesInBatch,
			//default to always true so blob names are kept
			KeepDirStructure: true}}

	source = sources.NewS3Pipeline(params)
	target = targets.NewAzurePage(storageAccountName, storageAccountKey, containerName)
	return
}
func getS3ToBlockPipelines() (source []pipeline.SourcePipeline, target pipeline.TargetPipeline, err error) {

	params := &sources.S3Params{
		Bucket:   sourceParameters["BUCKET"],
		Prefixes: []string{sourceParameters["PREFIX"]},
		Endpoint: sourceParameters["ENDPOINT"],
		Region:   sourceParameters[s3RegionEnvVar],
		SourceParams: sources.SourceParams{
			CalculateMD5:      calculateMD5,
			UseExactNameMatch: exactNameMatch,
			FilesPerPipeline:  numberOfFilesInBatch,
			//default to always true so blob names are kept
			KeepDirStructure: true}}

	source = sources.NewS3Pipeline(params)
	target = targets.NewAzureBlock(storageAccountName, storageAccountKey, containerName)
	return
}
func getBlobToPagePipelines() (source []pipeline.SourcePipeline, target pipeline.TargetPipeline, err error) {

	params := &sources.AzureBlobParams{
		Container:   sourceParameters["CONTAINER"],
		BlobNames:   blobNames,
		AccountName: sourceParameters["ACCOUNT_NAME"],
		AccountKey:  sourceAuthorization,
		SourceParams: sources.SourceParams{
			CalculateMD5:      calculateMD5,
			UseExactNameMatch: exactNameMatch,
			FilesPerPipeline:  numberOfFilesInBatch,
			//default to always true so blob names are kept
			KeepDirStructure: true}}

	source = sources.NewAzureBlob(params)
	target = targets.NewAzurePage(storageAccountName, storageAccountKey, containerName)
	return
}
func getBlobToBlockPipelines() (source []pipeline.SourcePipeline, target pipeline.TargetPipeline, err error) {

	blobNames = []string{sourceParameters["PREFIX"]}

	params := &sources.AzureBlobParams{
		Container:   sourceParameters["CONTAINER"],
		BlobNames:   blobNames,
		AccountName: sourceParameters["ACCOUNT_NAME"],
		AccountKey:  sourceAuthorization,
		SourceParams: sources.SourceParams{
			CalculateMD5:      calculateMD5,
			UseExactNameMatch: exactNameMatch,
			FilesPerPipeline:  numberOfFilesInBatch,
			//default to always true so blob names are kept
			KeepDirStructure: true}}

	source = sources.NewAzureBlob(params)
	target = targets.NewAzureBlock(storageAccountName, storageAccountKey, containerName)
	return
}
func getBlobToFilePipelines() (source []pipeline.SourcePipeline, target pipeline.TargetPipeline, err error) {

	if len(blobNames) == 0 {
		//use empty prefix to get the bloblist
		blobNames = []string{""}
	}

	params := &sources.AzureBlobParams{
		Container:   containerName,
		BlobNames:   blobNames,
		AccountName: storageAccountName,
		AccountKey:  storageAccountKey,
		SourceParams: sources.SourceParams{
			CalculateMD5:      calculateMD5,
			UseExactNameMatch: exactNameMatch,
			FilesPerPipeline:  numberOfFilesInBatch,
			KeepDirStructure:  keepDirStructure}}

	source = sources.NewAzureBlob(params)
	target = targets.NewMultiFile(true, numberOfHandlesPerFile)
	return
}
func getHTTPToFilePipelines() (source []pipeline.SourcePipeline, target pipeline.TargetPipeline, err error) {
	source = []pipeline.SourcePipeline{sources.NewHTTP(sourceURIs, blobNames, calculateMD5)}

	var targetAliases []string

	if len(blobNames) > 0 {
		targetAliases = blobNames
	} else {
		targetAliases = make([]string, len(sourceURIs))
		for i, src := range sourceURIs {
			targetAliases[i], err = util.GetFileNameFromURL(src)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	target = targets.NewMultiFile(true, numberOfHandlesPerFile)
	return
}

const openFileLimitForLinux = 1024

//validates if the number of readers needs to be adjusted to accommodate max filehandle limits in Debian systems
func adjustReaders(numOfSources int) error {
	if runtime.GOOS != "linux" {
		return nil
	}

	if (numOfSources * numberOfHandlesPerFile) > openFileLimitForLinux {
		numberOfHandlesPerFile = openFileLimitForLinux / (numOfSources * numberOfHandlesPerFile)

		if numberOfHandlesPerFile == 0 {
			return fmt.Errorf("The number of files will cause the process to exceed the limit of open files allowed by the OS. Reduce the number of files to be transferred")
		}

		fmt.Printf("Warning! Adjusting the number of handles per file (-h) to %v\n", numberOfHandlesPerFile)

	}

	return nil
}

//**************************
//Parsing and validation rules...
//These rules validate and prepare the parameters to construct the pipelines
// and are applied for all transfers or for an specific one.
//**************************

//Global rules....
func pvgParseBlockSize() error {
	var err error
	blockSize, err = util.ByteCountFromSizeString(blockSizeStr)
	if err != nil {
		return fmt.Errorf("Invalid block size specified: %v. Parse Error: %v ", blockSizeStr, err)
	}
	return nil
}
func pvgBatchLimits() error {
	if numberOfFilesInBatch < 1 {
		log.Fatal("Invalid value for option -x, the value must be greater than 1")
	}
	return nil
}
func pvgHTTPTimeOut() error {
	if util.HTTPClientTimeout < 30 {
		fmt.Printf("Warning! The storage HTTP client timeout is too low (>5). Setting value to 600s \n")
		util.HTTPClientTimeout = 600
	}
	return nil
}
func pvgDupCheck() error {
	var err error
	dedupeLevel, err = transfer.ParseDupeCheckLevel(dedupeLevelOptStr)
	if err != nil {
		fmt.Errorf("Duplicate detection level is invalid.  Found '%s', must be one of %s. Error:%v", dedupeLevelOptStr, transfer.DupeCheckLevelStr, err)
	}

	return nil
}
func pvgTransferType() error {
	var err error
	if transferType, err = transfer.ParseTransferDefinition(transferDefStr); err != nil {
		return err
	}
	return nil
}

//Transfer specific rules...
func pvNumOfHandlesPerFile() error {
	if numberOfHandlesPerFile < 1 {
		return fmt.Errorf("Invalid value for option -h, the value must be greater than 1")
	}

	return nil
}

func pvBlockSizeCheckForBlockBlobs() error {

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

	sourceParameters = make(map[string]string)

	sourceParameters["ACCOUNT_NAME"] = sourceAccountName
	sourceParameters["CONTAINER"] = sourceContName
	sourceParameters["PREFIX"] = sourceBlobName

	if sourceAuthorization == "" {
		envVal := os.Getenv(sourceAuthorizationEnvVar)
		if envVal == "" {
			return fmt.Errorf("The source storage acccount key is required for this transfer type")
		}
		sourceAuthorization = envVal
	}

	return nil
}

func pvSourceInfoForS3IsReq() error {
	burl, err := url.Parse(sourceURIs[0])

	if err != nil {
		return fmt.Errorf("Invalid S3 endpoint URL. Parsing error: %v.\nThe format is s3://[END_POINT]/[BUCKET]/[OBJECT]", err)
	}

	endpoint := burl.Hostname()

	if endpoint == "" {
		return fmt.Errorf("Invalid source S3 URI. The endpoint is required")
	}

	segments := strings.Split(burl.Path, "/")

	bucket := segments[1]

	if bucket == "" {
		return fmt.Errorf("Invalid source S3 URI. Bucket name could be parsed")
	}

	prefix := ""
	if len(segments) > 1 {
		prefix = strings.Join(segments[2:len(segments)], "/")
	}

	region := os.Getenv(s3RegionEnvVar)
	/*
	if region == "" {
		return fmt.Errorf("The S3 region is required for this transfer type. Environment variable name:%v", s3RegionEnvVar)
	}
	*/

	//The S3 Source uses the environment variables cred provider. So here we are just checking they are set.

	if os.Getenv(s3KeyEnvVar) == "" {
		return fmt.Errorf("The S3 access key is required for this transfer type. Environment variable name:%v", s3KeyEnvVar)
	}

	if os.Getenv(s3SecretKeyEnvVar) == "" {
		return fmt.Errorf("The S3 secret key is required for this transfer type. Environment variable name:%v", s3SecretKeyEnvVar)
	}

	sourceParameters = make(map[string]string)

	sourceParameters[s3RegionEnvVar] = region
	sourceParameters["PREFIX"] = prefix
	sourceParameters["BUCKET"] = bucket
	sourceParameters["ENDPOINT"] = endpoint

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
