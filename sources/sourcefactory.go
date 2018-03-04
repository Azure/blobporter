package sources

import (
	"fmt"

	"github.com/Azure/blobporter/internal"
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

	var filter internal.SourceFilter
	filter = &defaultItemFilter{}

	if params.Tracker != nil {
		filter = params.Tracker
	}
	return newObjectListPipelineFactory(s3Provider, filter, params.CalculateMD5, params.FilesPerPipeline)
}

//NewAzBlobSourcePipelineFactory TODO
func NewAzBlobSourcePipelineFactory(params *AzureBlobParams) <-chan FactoryResult {
	azProvider := newazBlobInfoProvider(params)

	var filter internal.SourceFilter
	filter = &defaultItemFilter{}

	if params.Tracker != nil {
		filter = params.Tracker
	}

	return newObjectListPipelineFactory(azProvider, filter, params.CalculateMD5, params.FilesPerPipeline)
}

//NewFileSystemSourcePipelineFactory TODO
func NewFileSystemSourcePipelineFactory(params *FileSystemSourceParams) <-chan FactoryResult {
	result := make(chan FactoryResult, 1)
	provider := newfileInfoProvider(params)

	go func() {
		defer close(result)
		for fInfoResp := range provider.listSourcesInfo() {

			if fInfoResp.err != nil {
				result <- FactoryResult{Err: fInfoResp.err}
				return
			}

			handlePool := internal.NewFileHandlePool(maxNumOfHandlesPerFile, internal.Read, false)
			result <- FactoryResult{
				Source: &FileSystemSource{filesInfo: fInfoResp.fileInfos,
					totalNumberOfBlocks: int(fInfoResp.totalNumOfBlocks),
					blockSize:           params.BlockSize,
					totalSize:           uint64(fInfoResp.totalSize),
					numOfPartitions:     params.NumOfPartitions,
					includeMD5:          params.CalculateMD5,
					handlePool:          handlePool,
				},
			}
		}
		return
	}()

	return result
}

func newObjectListPipelineFactory(provider objectListProvider, filter internal.SourceFilter, includeMD5 bool, filesPerPipeline int) <-chan FactoryResult {
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
func (f *defaultItemFilter) IsTransferredAndTrackIfNot(name string, size int64) (bool, error) {
	return false, nil
}

//ObjectListingResult TODO
type ObjectListingResult struct {
	Sources []pipeline.SourceInfo
	Err     error
}
