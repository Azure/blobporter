package sources

import "github.com/Azure/blobporter/pipeline"

//objectListManager TODO
type objectListManager interface {
	getSourceInfo() ([]pipeline.SourceInfo, error)
}

//SourceParams TODO
type SourceParams struct {
	CalculateMD5      bool
	UseExactNameMatch bool
	KeepDirStructure  bool
	FilesPerPipeline  int
}
