package targets

import (
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/util"
)

////////////////////////////////////////////////////////////
///// AzurePage Target
////////////////////////////////////////////////////////////

//AzurePage represents an Azure Block target
type AzurePage struct {
	Creds         *pipeline.StorageAccountCredentials
	Container     string
	StorageClient *storage.BlobStorageClient
}

//NewAzurePage creates a new Azure Block target
func NewAzurePage(accountName string, accountKey string, container string) pipeline.TargetPipeline {
	creds := pipeline.StorageAccountCredentials{AccountName: accountName, AccountKey: accountKey}
	client := util.GetBlobStorageClient(creds.AccountName, creds.AccountKey)
	return AzurePage{Creds: &creds, Container: container, StorageClient: &client}
}

//PageSize size of page in Azure Page Blob storage
const PageSize int64 = 512

//PreProcessSourceInfo implementation of PreProcessSourceInfo from the pipeline.TargetPipeline interface.
//Passthrough no need to pre-process for blob blocks.
func (t AzurePage) PreProcessSourceInfo(source *pipeline.SourceInfo) (err error) {
	size := int64(source.Size)

	if size%PageSize != 0 {
		return fmt.Errorf("Invalid size for a page blob. The size of the file %v (%v) is not a multiple of %v", source.SourceName, source.Size, PageSize)
	}

	if size > int64(util.TB) {
		return fmt.Errorf("The file %v is too big (%v). Tha maximum size of a page blob is %v ", source.SourceName, source.Size, util.TB)
	}

	//if the max retries is exceeded, panic will happen, hence no error is returned.
	util.RetriableOperation(func(r int) error {
		if err := (*t.StorageClient).PutPageBlob(t.Container, (*source).TargetAlias, size, nil); err != nil {
			return err
		}
		return nil
	})

	return nil
}

//CommitList implements CommitList from the pipeline.TargetPipeline interface.
//Passthrough no need to a commit for page blob.
func (t AzurePage) CommitList(listInfo *pipeline.TargetCommittedListInfo, NumberOfBlocks int, targetName string) (msg string, err error) {

	msg = "Page blob committed"
	err = nil
	return
}

//ProcessWrittenPart implements ProcessWrittenPart from the pipeline.TargetPipeline interface.
//Passthrough no need to process a written part when transfering to a page blob.
func (t AzurePage) ProcessWrittenPart(result *pipeline.WorkerResult, listInfo *pipeline.TargetCommittedListInfo) (requeue bool, err error) {
	requeue = false
	err = nil
	return
}

//WritePart implements WritePart from the pipeline.TargetPipeline interface.
//Performs a PUT page operation with the data contained in the part.
//This assumes the part.BytesToRead is a multiple of the PageSize
func (t AzurePage) WritePart(part *pipeline.Part) (duration time.Duration, startTime time.Time, numOfRetries int, err error) {

	offset := int64(part.Offset)
	endByte := int64(part.Offset + uint64(part.BytesToRead) - 1)

	//if the max retries is exceeded, panic will happen, hence no error is returned.
	duration, startTime, numOfRetries = util.RetriableOperation(func(r int) error {
		if err := (*t.StorageClient).PutPage(t.Container, part.TargetAlias, offset, endByte, "update", part.Data, nil); err != nil {
			if util.Verbose {
				fmt.Printf("EH|S|%v|%v|%v|%v\n", part.Offset, len(part.Data), part.TargetAlias, err)
			}
			return err
		}

		if util.Verbose {
			fmt.Printf("OKA|S|%v|%v|%v|%v\n", part.Offset, len(part.Data), part.TargetAlias, err)
		}
		return nil
	})

	return
}
