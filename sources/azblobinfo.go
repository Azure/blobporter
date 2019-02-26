package sources

import (
	"log"
	"path"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
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

func (b *azBlobInfoProvider) toSourceInfo(obj *azblob.BlobItem) (*pipeline.SourceInfo, error) {
	exp := b.params.SasExp
	if exp == 0 {
		exp = defaultSasExpHours
	}
	date := time.Now().Add(time.Duration(exp) * time.Minute).UTC()

	sourceURLWithSAS, err := b.azUtil.GetBlobURLWithReadOnlySASToken(obj.Name, date)

	if err != nil {
		return nil, err
	}

	targetAlias := obj.Name
	if !b.params.KeepDirStructure {
		targetAlias = path.Base(obj.Name)
	}

	return &pipeline.SourceInfo{
		SourceName:  sourceURLWithSAS.String(),
		Size:        uint64(*obj.Properties.ContentLength),
		TargetAlias: targetAlias}, nil
}

func (b *azBlobInfoProvider) listObjects(filter internal.SourceFilter) <-chan ObjectListingResult {
	sources := make(chan ObjectListingResult, 2)
	list := make([]pipeline.SourceInfo, 0)
	bsize := 0

	blobCallback := func(blob *azblob.BlobItem, prefix string) (bool, error) {
		include := true
		if b.params.UseExactNameMatch {
			include = blob.Name == prefix
		}

		transferred, err := filter.IsTransferredAndTrackIfNot(blob.Name, int64(*blob.Properties.ContentLength))

		if err != nil {
			return true, err
		}

		if include && !transferred {

			si, err := b.toSourceInfo(blob)

			if err != nil {
				return true, err
			}

			list = append(list, *si)

			if bsize++; bsize == b.params.FilesPerPipeline {
				sources <- ObjectListingResult{Sources: list}
				list = make([]pipeline.SourceInfo, 0)
				bsize = 0
			}

			if b.params.UseExactNameMatch {
				//true, stops iteration
				return true, nil
			}
		}

		//false, continues the iteration
		return false, nil
	}

	go func() {
		for _, blobName := range b.params.BlobNames {
			if err := b.azUtil.IterateBlobList(blobName, blobCallback); err != nil {
				sources <- ObjectListingResult{Err: err}
				return
			}
			if bsize > 0 {
				sources <- ObjectListingResult{Sources: list}
				list = make([]pipeline.SourceInfo, 0)
				bsize = 0
			}
		}
		close(sources)

	}()

	return sources
}
