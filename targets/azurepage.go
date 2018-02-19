package targets

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/Azure/blobporter/internal"
	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/util"
)

////////////////////////////////////////////////////////////
///// AzurePage Target
////////////////////////////////////////////////////////////

//AzurePageTarget represents an Azure Block target
type AzurePageTarget struct {
	azUtil *internal.AzUtil
}

//NewAzurePageTargetPipeline creates a new Azure Block target
func NewAzurePageTargetPipeline(params AzureTargetParams) pipeline.TargetPipeline {
	az, err := internal.NewAzUtil(params.AccountName, params.AccountKey, params.Container, params.BaseBlobURL)

	if err != nil {
		log.Fatal(err)
	}

	var notfound bool
	notfound, err = az.CreateContainerIfNotExists()

	if notfound {
		fmt.Printf("Info! Container was not found, creating it...\n")
	}

	if err != nil {
		log.Fatal(err)
	}
	return &AzurePageTarget{azUtil: az}
}

//Page blobs limits and units

//PageSize page size for page blobs
const PageSize uint64 = 512
const maxPageSize uint64 = 4 * util.MB
const maxPageBlobSize uint64 = 8 * util.TB

//PreProcessSourceInfo implementation of PreProcessSourceInfo from the pipeline.TargetPipeline interface.
//initializes the page blob.
func (t *AzurePageTarget) PreProcessSourceInfo(source *pipeline.SourceInfo, blockSize uint64) (err error) {
	size := source.Size

	if size%PageSize != 0 {
		return fmt.Errorf(" invalid size for a page blob. The size of the file %v (%v) is not a multiple of %v ", source.SourceName, source.Size, PageSize)
	}

	if size > maxPageBlobSize {
		return fmt.Errorf(" the file %v is too big (%v). Tha maximum size of a page blob is %v ", source.SourceName, source.Size, maxPageBlobSize)
	}

	if blockSize > maxPageSize || blockSize < PageSize {
		return fmt.Errorf(" invalid block size for page blob: %v. The value must be greater than %v and less than %v", source.SourceName, PageSize, maxPageSize)
	}

	err = t.azUtil.CreatePageBlob(source.TargetAlias, size)
	return
}

//CommitList implements CommitList from the pipeline.TargetPipeline interface.
//Passthrough no need to a commit for page blob.
func (t *AzurePageTarget) CommitList(listInfo *pipeline.TargetCommittedListInfo, NumberOfBlocks int, targetName string) (msg string, err error) {
	msg = "Page blob committed"
	err = nil
	return
}

//ProcessWrittenPart implements ProcessWrittenPart from the pipeline.TargetPipeline interface.
//Passthrough no need to process a written part when transferring to a page blob.
func (t *AzurePageTarget) ProcessWrittenPart(result *pipeline.WorkerResult, listInfo *pipeline.TargetCommittedListInfo) (requeue bool, err error) {
	requeue = false
	err = nil
	return
}

//WritePart implements WritePart from the pipeline.TargetPipeline interface.
//Performs a PUT page operation with the data contained in the part.
//This assumes the part.BytesToRead is a multiple of the PageSize
func (t *AzurePageTarget) WritePart(part *pipeline.Part) (duration time.Duration, startTime time.Time, numOfRetries int, err error) {

	start := int64(part.Offset)
	end := int64(part.Offset + uint64(part.BytesToRead) - 1)
	defer util.PrintfIfDebug("WritePart -> start:%v end:%v name:%v err:%v", start, end, part.TargetAlias, err)

	err = t.azUtil.PutPages(part.TargetAlias, start, end, bytes.NewReader(part.Data))

	return
}
