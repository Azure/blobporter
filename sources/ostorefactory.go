package sources

import (
	"fmt"
	"log"

	"github.com/Azure/blobporter/pipeline"
)

//AzureBlobSource constructs parts channel and implements data readers for Azure Blobs exposed via HTTP
type AzureBlobSource struct {
	HTTPSource
	container      string
	blobNames      []string
	exactNameMatch bool
}

//NewAzureBlobSourcePipeline creates a new instance of the HTTPPipeline for Azure Blobs
func NewAzureBlobSourcePipeline(params *AzureBlobParams) []pipeline.SourcePipeline {
	var err error
	var azObjStorage objectListManager
	azObjStorage = newazBlobInfoProvider(params)

	if params.FilesPerPipeline <= 0 {
		log.Fatal(fmt.Errorf("Invalid operation. The number of files per batch must be greater than zero"))
	}

	factory := func(httpSource HTTPSource) (pipeline.SourcePipeline, error) {
		return &AzureBlobSource{container: params.Container,
			blobNames:      params.BlobNames,
			HTTPSource:     httpSource,
			exactNameMatch: params.SourceParams.UseExactNameMatch}, nil
	}

	var pipelines []pipeline.SourcePipeline
	if pipelines, err = newHTTPSource(azObjStorage, factory, params.SourceParams.FilesPerPipeline, params.SourceParams.CalculateMD5); err != nil {
		log.Fatal(err)
	}

	return pipelines
}

//S3Source S3 source HTTP based pipeline
type S3Source struct {
	HTTPSource
	exactNameMatch bool
}

//NewS3SourcePipeline creates a new instance of the HTTPPipeline for S3
func NewS3SourcePipeline(params *S3Params) []pipeline.SourcePipeline {
	var err error
	var s3ObjStorage objectListManager
	s3ObjStorage, err = newS3InfoProvider(params)

	if err != nil {
		log.Fatal(err)
	}

	if params.FilesPerPipeline <= 0 {
		log.Fatal(fmt.Errorf("Invalid operation. The number of files per batch must be greater than zero"))
	}

	factory := func(httpSource HTTPSource) (pipeline.SourcePipeline, error) {
		return &S3Source{
			HTTPSource:     httpSource,
			exactNameMatch: params.SourceParams.UseExactNameMatch}, nil
	}

	var pipelines []pipeline.SourcePipeline
	if pipelines, err = newHTTPSource(s3ObjStorage, factory, params.SourceParams.FilesPerPipeline, params.SourceParams.CalculateMD5); err != nil {
		log.Fatal(err)
	}

	return pipelines
}
