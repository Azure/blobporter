package sources

import "github.com/Azure/blobporter/pipeline"

//objectListManager abstracs the oerations required to get a list of sources/objects from a underlying service such as Azure Object storage and S3
type objectListManager interface {
	getSourceInfo() ([]pipeline.SourceInfo, error)
}

//SourceParams base parameters for HTTP based pipelines
type SourceParams struct {
	CalculateMD5      bool
	UseExactNameMatch bool
	KeepDirStructure  bool
	FilesPerPipeline  int
}
