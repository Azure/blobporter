package sources

import "github.com/Azure/blobporter/internal"

type objectListProvider interface {
	listObjects(filter internal.SourceFilter) <-chan ObjectListingResult
}

//SourceParams input base parameters for blob and S3 based pipelines
type SourceParams struct {
	CalculateMD5      bool
	UseExactNameMatch bool
	KeepDirStructure  bool
	FilesPerPipeline  int
	Tracker           *internal.TransferTracker
}

//HTTPSourceParams input parameters for HTTP pipelines
type HTTPSourceParams struct {
	SourceParams
	SourceURIs    []string
	TargetAliases []string
}
