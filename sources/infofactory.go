package sources

import "github.com/Azure/blobporter/pipeline"

//ObjectsListManager TODO
type ObjectsListManager interface {
	GetSourceInfo() ([]pipeline.SourceInfo, error)
}

//NewAzureBlobListManager TODO
func NewAzureBlobListManager(params *AzureBlobParams) (ObjectsListManager, error) {
	return newazBlobInfoProvider(params), nil
}
