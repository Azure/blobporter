package sources

import (
	"fmt"
	"log"

	"github.com/Azure/blobporter/pipeline"
)

//AzureBlob constructs parts channel and implements data readers for Azure Blobs exposed via HTTP
type AzureBlob struct {
	HTTPPipeline
	Container      string
	BlobNames      []string
	exactNameMatch bool
}

//NewAzureBlob creates a new instance of the HTTPPipeline for Azure Blobs
func NewAzureBlob(params *AzureBlobParams) []pipeline.SourcePipeline {
	var err error
	var azObjStorage objectListManager
	azObjStorage = newazBlobInfoProvider(params)

	if params.FilesPerPipeline <= 0 {
		log.Fatal(fmt.Errorf("Invalid operation. The number of files per batch must be greater than zero"))
	}

	factory := func(httpSource HTTPPipeline) (pipeline.SourcePipeline, error) {
		return &AzureBlob{Container: params.Container,
			BlobNames:      params.BlobNames,
			HTTPPipeline:   httpSource,
			exactNameMatch: params.SourceParams.UseExactNameMatch}, nil
	}

	var pipelines []pipeline.SourcePipeline
	if pipelines, err = newHTTPSource(azObjStorage, factory, params.SourceParams.FilesPerPipeline, params.SourceParams.CalculateMD5); err != nil {
		log.Fatal(err)
	}

	return pipelines
}

//S3Pipeline S3 source HTTP based pipeline
type S3Pipeline struct {
	HTTPPipeline
	exactNameMatch bool
}

//NewS3Pipeline creates a new instance of the HTTPPipeline for S3
func NewS3Pipeline(params *S3Params) []pipeline.SourcePipeline {
	var err error
	var s3ObjStorage objectListManager
	s3ObjStorage, err = newS3InfoProvider(params)

	if err != nil {
		log.Fatal(err)
	}

	if params.FilesPerPipeline <= 0 {
		log.Fatal(fmt.Errorf("Invalid operation. The number of files per batch must be greater than zero"))
	}

	factory := func(httpSource HTTPPipeline) (pipeline.SourcePipeline, error) {
		return &S3Pipeline{
			HTTPPipeline:   httpSource,
			exactNameMatch: params.SourceParams.UseExactNameMatch}, nil
	}

	var pipelines []pipeline.SourcePipeline
	if pipelines, err = newHTTPSource(s3ObjStorage, factory, params.SourceParams.FilesPerPipeline, params.SourceParams.CalculateMD5); err != nil {
		log.Fatal(err)
	}

	return pipelines
}
