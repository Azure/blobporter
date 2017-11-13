package sources

import (
	"log"
	"path"
	"sync"
	"time"

	"fmt"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/util"
)

////////////////////////////////////////////////////////////
///// Azure Blob source
//////////////////////////////////////////////////////////
//const sasTokenNumberOfHours = 4

//AzureBlob constructs parts  channel and implements data readers for Azure Blobs exposed via HTTP
type AzureBlob struct {
	HTTPSource     HTTPPipeline
	Container      string
	BlobNames      []string
	exactNameMatch bool
	storageClient  storage.BlobStorageClient
}

//AzureBlobParams TODO
type AzureBlobParams struct {
	Container         string
	BlobNames         []string
	AccountName       string
	AccountKey        string
	CalculateMD5      bool
	UseExactNameMatch bool
	KeepDirStructure  bool
	FilesPerPipeline  int
}

//NewAzureBlob creates a new instance of HTTPPipeline for Azure Blobs
func NewAzureBlob(params *AzureBlobParams) []pipeline.SourcePipeline {

	var err error
	var sourceInfos []pipeline.SourceInfo
	if sourceInfos, err = getSourcesInfoForBlobs(params); err != nil {
		log.Fatal(err)
	}

	if params.FilesPerPipeline <= 0 {
		log.Fatal(fmt.Errorf("Invalid operation. The number of files per batch must be greater than zero"))
	}

	numOfBatches := (len(sourceInfos) + params.FilesPerPipeline - 1) / params.FilesPerPipeline
	pipelines := make([]pipeline.SourcePipeline, numOfBatches)
	numOfFilesInBatch := params.FilesPerPipeline
	filesSent := len(sourceInfos)
	start := 0

	for b := 0; b < numOfBatches; b++ {

		if filesSent < numOfFilesInBatch {
			numOfFilesInBatch = filesSent
		}
		start = b * numOfFilesInBatch

		httpSource := HTTPPipeline{Sources: sourceInfos[start : start+numOfFilesInBatch], HTTPClient: util.NewHTTPClient(), includeMD5: params.CalculateMD5}
		pipelines[b] = &AzureBlob{Container: params.Container,
			BlobNames:      params.BlobNames,
			HTTPSource:     httpSource,
			storageClient:  util.GetBlobStorageClient(params.AccountName, params.AccountKey),
			exactNameMatch: params.UseExactNameMatch}
		filesSent = filesSent - numOfFilesInBatch
	}

	return pipelines
}

//GetSourcesInfo implements GetSourcesInfo from the pipeline.SourcePipeline Interface.
func (f *AzureBlob) GetSourcesInfo() []pipeline.SourceInfo {
	return f.HTTPSource.GetSourcesInfo()
}

//ExecuteReader implements ExecuteReader from the pipeline.SourcePipeline Interface.
//For each part the reader makes a byte range request to the source
// starting from the part's Offset to BytesToRead - 1 (zero based).
func (f *AzureBlob) ExecuteReader(partitionsQ chan pipeline.PartsPartition, partsQ chan pipeline.Part, readPartsQ chan pipeline.Part, id int, wg *sync.WaitGroup) {
	f.HTTPSource.ExecuteReader(partitionsQ, partsQ, readPartsQ, id, wg)
}

//ConstructBlockInfoQueue implements GetSourcesInfo from the pipeline.SourcePipeline Interface.
//Constructs the Part's channel arithmetically from the size of the sources.
func (f *AzureBlob) ConstructBlockInfoQueue(blockSize uint64) (partitionsQ chan pipeline.PartsPartition, partsQ chan pipeline.Part, numOfBlocks int, size uint64) {
	return f.HTTPSource.ConstructBlockInfoQueue(blockSize)
}

func getSourcesInfoForBlobs(params *AzureBlobParams) ([]pipeline.SourceInfo, error) {
	var err error
	storageClient := util.GetBlobStorageClient(params.AccountName, params.AccountKey)

	date := time.Now().UTC().Add(time.Duration(sasTokenNumberOfHours) * time.Hour)

	var blobLists []storage.BlobListResponse
	blobLists, err = getBlobLists(params, &storageClient)
	if err != nil {
		return nil, err
	}

	sourceURIs := make([]pipeline.SourceInfo, 0)
	var sourceURI string
	for _, blobList := range blobLists {
		for _, blob := range blobList.Blobs {

			include := true
			if params.UseExactNameMatch {
				include = blob.Name == blobList.Prefix
			}

			if include {
				sourceURI, err = storageClient.GetBlobSASURI(params.Container, blob.Name, date, "r")

				if err != nil {
					return nil, err
				}

				targetAlias := blob.Name
				if !params.KeepDirStructure {
					targetAlias = path.Base(blob.Name)
				}

				sourceURIs = append(sourceURIs, pipeline.SourceInfo{
					SourceName:  sourceURI,
					Size:        uint64(blob.Properties.ContentLength),
					TargetAlias: targetAlias})
			}
		}
	}

	if len(sourceURIs) == 0 {
		nameMatchMode := "prefix"
		if params.UseExactNameMatch {
			nameMatchMode = "name"
		}
		return nil, fmt.Errorf("the %v %s did not match any blob names", nameMatchMode, params.BlobNames)
	}

	return sourceURIs, nil
}

func getBlobLists(params *AzureBlobParams, client *storage.BlobStorageClient) ([]storage.BlobListResponse, error) {
	var err error
	listLength := 1

	if len(params.BlobNames) > 1 {
		listLength = len(params.BlobNames)
	}

	listOfLists := make([]storage.BlobListResponse, listLength)

	for i, blobname := range params.BlobNames {
		var list *storage.BlobListResponse
		listParams := storage.ListBlobsParameters{Prefix: blobname}

		for {
			var listpage storage.BlobListResponse
			if listpage, err = client.ListBlobs(params.Container, listParams); err != nil {
				return nil, err
			}

			if list == nil {
				list = &listpage
			} else {
				(*list).Blobs = append((*list).Blobs, listpage.Blobs...)
			}

			if listpage.NextMarker == "" {
				break
			}

			listParams.Marker = listpage.NextMarker
		}

		listOfLists[i] = *list
	}

	return listOfLists, nil

}
