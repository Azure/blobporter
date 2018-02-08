package targets

//FileTargetParams TODO
type FileTargetParams struct {
	Overwrite              bool
	NumberOfHandlesPerFile int
}

//AzureTargetParams TODO
type AzureTargetParams struct {
	AccountName string
	AccountKey  string
	Container   string
	BaseBlobURL string
}
