package sources

import (
	"fmt"
	"path"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/util"
)

//AzureBlobParams parameters for the creation of Azure Blob source pipeline
type AzureBlobParams struct {
	SourceParams
	Container           string
	BlobNames           []string
	AccountName         string
	AccountKey          string
	SasExpNumberOfHours int
}

const defaultSasExpHours = 2

type azBlobInfoProvider struct {
	params        *AzureBlobParams
	storageClient *storage.BlobStorageClient
}

func newazBlobInfoProvider(params *AzureBlobParams) *azBlobInfoProvider {
	client := util.GetBlobStorageClient(params.AccountName, params.AccountKey)

	return &azBlobInfoProvider{params: params, storageClient: &client}
}

//getSourceInfo gets a list of SourceInfo that represent the list of azure blobs returned by the service
// based on the provided criteria (container/prefix). If the exact match flag is set, then a specific match is
// performed instead of the prefix. Marker semantics are also honored so a complete list is expected
func (b *azBlobInfoProvider) getSourceInfo() ([]pipeline.SourceInfo, error) {
	var err error
	exp := b.params.SasExpNumberOfHours
	if exp == 0 {
		exp = defaultSasExpHours
	}
	date := time.Now().Add(time.Duration(exp) * time.Hour).UTC()

	var blobLists []storage.BlobListResponse
	blobLists, err = b.getBlobLists()
	if err != nil {
		return nil, err
	}

	sourceURIs := make([]pipeline.SourceInfo, 0)
	var sourceURI string
	for _, blobList := range blobLists {
		for _, blob := range blobList.Blobs {

			include := true
			if b.params.UseExactNameMatch {
				include = blob.Name == blobList.Prefix
			}

			if include {
				sourceURI, err = b.storageClient.GetBlobSASURI(b.params.Container, blob.Name, date, "r")

				if err != nil {
					return nil, err
				}

				targetAlias := blob.Name
				if !b.params.KeepDirStructure {
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
		if b.params.UseExactNameMatch {
			nameMatchMode = "name"
		}
		return nil, fmt.Errorf(" the %v %s did not match any blob names ", nameMatchMode, b.params.BlobNames)
	}

	return sourceURIs, nil
}

func (b *azBlobInfoProvider) getBlobLists() ([]storage.BlobListResponse, error) {
	var err error
	listLength := 1

	if len(b.params.BlobNames) > 1 {
		listLength = len(b.params.BlobNames)
	}

	listOfLists := make([]storage.BlobListResponse, listLength)

	for i, blobname := range b.params.BlobNames {
		var list *storage.BlobListResponse
		listParams := storage.ListBlobsParameters{Prefix: blobname}

		for {
			var listpage storage.BlobListResponse
			if listpage, err = b.storageClient.ListBlobs(b.params.Container, listParams); err != nil {
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
