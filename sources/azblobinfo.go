package sources

import (
	"fmt"
	"log"
	"path"
	"time"

	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"github.com/Azure/blobporter/internal"
	"github.com/Azure/blobporter/pipeline"
)

//AzureBlobParams parameters for the creation of Azure Blob source pipeline
type AzureBlobParams struct {
	SourceParams
	Container   string
	BlobNames   []string
	AccountName string
	AccountKey  string
	SasExp      int
	BaseBlobURL string
}

const defaultSasExpHours = 2

type azBlobInfoProvider struct {
	params *AzureBlobParams
	azUtil *internal.AzUtil
}

func newazBlobInfoProvider(params *AzureBlobParams) *azBlobInfoProvider {
	azutil, err := internal.NewAzUtil(params.AccountName, params.AccountKey, params.Container, params.BaseBlobURL)

	if err != nil {
		log.Fatal(err)
	}

	return &azBlobInfoProvider{params: params, azUtil: azutil}
}

//getSourceInfo gets a list of SourceInfo that represent the list of azure blobs returned by the service
// based on the provided criteria (container/prefix). If the exact match flag is set, then a specific match is
// performed instead of the prefix. Marker semantics are also honored so a complete list is expected
func (b *azBlobInfoProvider) getSourceInfo() ([]pipeline.SourceInfo, error) {
	var err error
	exp := b.params.SasExp
	if exp == 0 {
		exp = defaultSasExpHours
	}
	date := time.Now().Add(time.Duration(exp) * time.Minute).UTC()
	sourceURIs := make([]pipeline.SourceInfo, 0)

	blobCallback := func(blob *azblob.Blob, prefix string) (bool, error) {
		include := true
		if b.params.UseExactNameMatch {
			include = blob.Name == prefix
		}
		if include {
			sourceURLWithSAS := b.azUtil.GetBlobURLWithReadOnlySASToken(blob.Name, date)

			targetAlias := blob.Name
			if !b.params.KeepDirStructure {
				targetAlias = path.Base(blob.Name)
			}

			sourceURIs = append(sourceURIs, pipeline.SourceInfo{
				SourceName:  sourceURLWithSAS.String(),
				Size:        uint64(*blob.Properties.ContentLength),
				TargetAlias: targetAlias})
			if b.params.UseExactNameMatch {
				//true, stops iteration
				return true, nil
			}
		}

		//false, continues the iteration
		return false, nil
	}

	for _, blobName := range b.params.BlobNames {
		if err = b.azUtil.IterateBlobList(blobName, blobCallback); err != nil {
			return nil, err
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
