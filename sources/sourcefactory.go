package sources

import (
	"fmt"
	"path/filepath"

	"github.com/Azure/blobporter/pipeline"
)

//AzureBlobSource constructs parts channel and implements data readers for Azure Blobs exposed via HTTP
type AzureBlobSource struct {
	HTTPSource
	container      string
	blobNames      []string
	exactNameMatch bool
}

//S3Source S3 source HTTP based pipeline
type S3Source struct {
	HTTPSource
	exactNameMatch bool
}

//FactoryResult  TODO
type FactoryResult struct {
	Source pipeline.SourcePipeline
	Err    error
}

//NewHTTPSourcePipelineFactory TODO
func NewHTTPSourcePipelineFactory(params HTTPSourceParams) <-chan FactoryResult {
	result := make(chan FactoryResult, 1)
	defer close(result)

	result <- FactoryResult{
		Source: newHTTPSourcePipeline(params.SourceURIs,
			params.TargetAliases,
			params.SourceParams.CalculateMD5),
	}
	return result
}

//NewPerfSourcePipelineFactory TODO
func NewPerfSourcePipelineFactory(params PerfSourceParams) <-chan FactoryResult {
	result := make(chan FactoryResult, 1)
	defer close(result)

	perf := newPerfSourcePipeline(params)
	result <- FactoryResult{Source: perf[0]}
	return result
}

//NewS3SourcePipelineFactory returns  TODO
func NewS3SourcePipelineFactory(params *S3Params) <-chan FactoryResult {
	var err error
	s3Provider, err := newS3InfoProvider(params)

	if err != nil {
		return factoryError(err)
	}

	filter := &defaultItemFilter{}
	return newObjectListPipelineFactory(s3Provider, filter, params.CalculateMD5, params.FilesPerPipeline)
}

//NewAzBlobSourcePipelineFactory TODO
func NewAzBlobSourcePipelineFactory(params *AzureBlobParams) <-chan FactoryResult {
	azProvider := newazBlobInfoProvider(params)

	filter := &defaultItemFilter{}
	return newObjectListPipelineFactory(azProvider, filter, params.CalculateMD5, params.FilesPerPipeline)
}

//NewFileSystemSourcePipelineFactory TODO
func NewFileSystemSourcePipelineFactory(params *FileSystemSourceParams) <-chan FactoryResult {
	var files []string
	var err error
	result := make(chan FactoryResult, 1)

	//get files from patterns
	for i := 0; i < len(params.SourcePatterns); i++ {
		var sourceFiles []string
		if sourceFiles, err = filepath.Glob(params.SourcePatterns[i]); err != nil {
			return factoryError(err)
		}
		files = append(files, sourceFiles...)
	}

	if params.FilesPerPipeline <= 0 {
		err = fmt.Errorf("Invalid operation. The number of files per batch must be greater than zero")
		return factoryError(err)
	}

	if len(files) == 0 {
		err = fmt.Errorf("The pattern(s) %v did not match any files", fmt.Sprint(params.SourcePatterns))
		return factoryError(err)
	}

	go func() {
		numOfBatches := (len(files) + params.FilesPerPipeline - 1) / params.FilesPerPipeline
		numOfFilesInBatch := params.FilesPerPipeline
		filesSent := len(files)
		start := 0
		for b := 0; b < numOfBatches; b++ {
			var targetAlias []string

			if filesSent < numOfFilesInBatch {
				numOfFilesInBatch = filesSent
			}
			start = b * numOfFilesInBatch

			if len(params.TargetAliases) == len(files) {
				targetAlias = params.TargetAliases[start : start+numOfFilesInBatch]
			}
			result <- FactoryResult{Source: newMultiFilePipeline(files[start:start+numOfFilesInBatch],
				targetAlias,
				params.BlockSize,
				params.NumOfPartitions,
				params.MD5,
				params.KeepDirStructure),
			}
			filesSent = filesSent - numOfFilesInBatch
		}
		close(result)
	}()

	return result
}

func newObjectListPipelineFactory(provider objectListProvider, filter SourceFilter, includeMD5 bool, filesPerPipeline int) <-chan FactoryResult {
	result := make(chan FactoryResult, 1)
	var err error

	if filesPerPipeline <= 0 {
		err = fmt.Errorf("Invalid operation. The number of files per batch must be greater than zero")
		return factoryError(err)
	}

	go func() {
		defer close(result)
		for lst := range provider.listObjects(filter) {
			if lst.Err != nil {
				result <- FactoryResult{Err: lst.Err}
				return
			}

			httpSource := &HTTPSource{Sources: lst.Sources,
				HTTPClient: httpSourceHTTPClient,
				includeMD5: includeMD5,
			}
			result <- FactoryResult{Source: httpSource}
		}
	}()

	return result
}

func factoryError(err error) chan FactoryResult {
	result := make(chan FactoryResult, 1)
	defer close(result)
	result <- FactoryResult{Err: err}
	return result
}

type defaultItemFilter struct {
}

//IsIncluded TODO
func (f *defaultItemFilter) IsIncluded(key string) bool {
	return true
}

//SourceFilter TODO
type SourceFilter interface {
	IsIncluded(key string) bool
}

//ObjectListingResult TODO
type ObjectListingResult struct {
	Sources []pipeline.SourceInfo
	Err     error
}
