package sources

import (
	"sync"
	"time"

	"log"

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
	HTTPSource    HTTPPipeline
	Container     string
	BlobNames     []string
	storageClient storage.BlobStorageClient
}

//NewAzureBlob creates a new instance of HTTPPipeline
//Creates a SASURI to the blobName with an expiration of sasTokenNumberOfHours.
//blobName is used as the target alias.
func NewAzureBlob(container string, blobNames []string, accountName string, accountKey string) pipeline.SourcePipeline {
	azureSource := AzureBlob{Container: container, BlobNames: blobNames, storageClient: util.GetBlobStorageClient(accountName, accountKey)}
	if sourceURIs, err := azureSource.getSourceURIs(); err == nil {
		httpSource := NewHTTP(sourceURIs, nil)
		azureSource.HTTPSource = httpSource.(HTTPPipeline)
	} else {
		log.Fatal(err)
	}

	return azureSource
}

//GetSourcesInfo implements GetSourcesInfo from the pipeline.SourcePipeline Interface.
func (f AzureBlob) GetSourcesInfo() []pipeline.SourceInfo {
	return f.HTTPSource.GetSourcesInfo()
}

//ExecuteReader implements ExecuteReader from the pipeline.SourcePipeline Interface.
//For each part the reader makes a byte range request to the source
// starting from the part's Offset to BytesToRead - 1 (zero based).
func (f AzureBlob) ExecuteReader(partitionsQ chan pipeline.PartsPartition, partsQ chan pipeline.Part, readPartsQ chan pipeline.Part, id int, wg *sync.WaitGroup) {
	f.HTTPSource.ExecuteReader(partitionsQ, partsQ, readPartsQ, id, wg)
}

//ConstructBlockInfoQueue implements GetSourcesInfo from the pipeline.SourcePipeline Interface.
//Constructs the Part's channel arithmetically from the size of the sources.
func (f AzureBlob) ConstructBlockInfoQueue(blockSize uint64) (partitionsQ chan pipeline.PartsPartition, partsQ chan pipeline.Part, numOfBlocks int, size uint64) {
	return f.HTTPSource.ConstructBlockInfoQueue(blockSize)
}

func (f AzureBlob) getSourceURIs() ([]string, error) {
	var err error
	date := time.Now().UTC().Add(time.Duration(sasTokenNumberOfHours) * time.Hour)

	var blobLists []storage.BlobListResponse
	blobLists, err = f.getBlobLists()
	if err != nil {
		return nil, err
	}

	sourceURIs := make([]string, 0)
	var sourceURI string
	for _, blobList := range blobLists {
		for _, blob := range blobList.Blobs {
			sourceURI, err = f.storageClient.GetBlobSASURI(f.Container, blob.Name, date, "r")

			if err != nil {
				return nil, err
			}
			sourceURIs = append(sourceURIs, sourceURI)
		}
	}

	if len(sourceURIs) == 0 {
		return nil, fmt.Errorf("the prefix %v (-n option) did not match any blob names", f.BlobNames)
	}

	return sourceURIs, nil
}

func (f AzureBlob) getBlobLists() ([]storage.BlobListResponse, error) {
	var err error
	listLength := 1

	if len(f.BlobNames) > 1 {
		listLength = len(f.BlobNames)
	}

	listOfLists := make([]storage.BlobListResponse, listLength)

	for i, blobname := range f.BlobNames {
		params := storage.ListBlobsParameters{Prefix: blobname}
		var list storage.BlobListResponse
		if list, err = f.storageClient.ListBlobs(f.Container, params); err != nil {
			return nil, err
		}
		listOfLists[i] = list
	}

	return listOfLists, nil

}
