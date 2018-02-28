package sources



type objectListProvider interface {
	listObjects(filter SourceFilter) <-chan ObjectListingResult
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
