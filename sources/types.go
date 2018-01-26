package sources

import "github.com/Azure/blobporter/pipeline"

//objectListManager abstracs the oerations required to get a list of sources/objects from a underlying service such as Azure Object storage and S3
type objectListManager interface {
	getSourceInfo() ([]pipeline.SourceInfo, error)
}

//SourceParams input base parameters for blob and S3 based pipelines
type SourceParams struct {
	CalculateMD5      bool
	UseExactNameMatch bool
	KeepDirStructure  bool
	FilesPerPipeline  int
}

//HTTPSourceParams input parameters for HTTP pipelines
type HTTPSourceParams struct {
	SourceParams
	SourceURIs    []string
	TargetAliases []string
}
