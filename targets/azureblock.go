package targets

import (
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/util"
)

////////////////////////////////////////////////////////////
///// AzureBlock Target
////////////////////////////////////////////////////////////

//AzureBlock represents an Azure Block target
type AzureBlock struct {
	Creds         *pipeline.StorageAccountCredentials
	Container     string
	StorageClient storage.BlobStorageClient
}

//NewAzureBlock creates a new Azure Block target
func NewAzureBlock(accountName string, accountKey string, container string) pipeline.TargetPipeline {

	util.CreateContainerIfNotExists(container, accountName, accountKey)

	creds := pipeline.StorageAccountCredentials{AccountName: accountName, AccountKey: accountKey}
	bc := util.GetBlobStorageClient(creds.AccountName, creds.AccountKey)

	return AzureBlock{Creds: &creds, Container: container, StorageClient: bc}
}

//CommitList implements CommitList from the pipeline.TargetPipeline interface.
//Commits the list of blocks to Azure Storage to finalize the transfer.
func (t AzureBlock) CommitList(listInfo *pipeline.TargetCommittedListInfo, numberOfBlocks int, targetName string) (msg string, err error) {

	lInfo := (*listInfo)

	blockList := convertToStorageBlockList(lInfo.List, numberOfBlocks)

	if util.Verbose {
		fmt.Printf("Final BlockList:\n")
		for j := 0; j < numberOfBlocks; j++ {
			fmt.Printf("   [%2d]: ID=%s, Status=%s", j, blockList[j].ID, blockList[j].Status)
		}
	}

	//if the max retries is exceeded, panic will happen, hence no error is returned.
	duration, _, _ := util.RetriableOperation(func(r int) error {
		if err := t.StorageClient.PutBlockList(t.Container, targetName, blockList); err != nil {
			return err
		}
		return nil
	})

	msg = fmt.Sprintf("\rFile:%v, Blocks Committed: %d, Commit time = %v",
		targetName, len(blockList), duration)
	err = nil
	return
}

func convertToStorageBlockList(list interface{}, numOfBlocks int) []storage.Block {

	if list == nil {
		return make([]storage.Block, numOfBlocks)
	}

	return list.([]storage.Block)
}

//PreProcessSourceInfo implementation of PreProcessSourceInfo from the pipeline.TargetPipeline interface.
//Checks if uncommitted blocks are present and cleans them by creating an empty blob.
func (t AzureBlock) PreProcessSourceInfo(source *pipeline.SourceInfo) (err error) {
	return util.CleanUncommittedBlocks(&t.StorageClient, t.Container, source.TargetAlias)
}

//ProcessWrittenPart implements ProcessWrittenPart from the pipeline.TargetPipeline interface.
//Appends the written part to a list. If the part is duplicated the list is updated with a reference, to the first occurrence of the block.
//If the first occurrence has not yet being processed, the part is requested to be placed back in the results channel (requeue == true).
func (t AzureBlock) ProcessWrittenPart(result *pipeline.WorkerResult, listInfo *pipeline.TargetCommittedListInfo) (requeue bool, err error) {
	requeue = false
	blockList := convertToStorageBlockList((*listInfo).List, result.NumberOfBlocks)

	if result.DuplicateOfBlockOrdinal >= 0 { // this block is a duplicate of another.
		if blockList[result.DuplicateOfBlockOrdinal].ID != "" {
			blockList[result.Ordinal] = blockList[result.DuplicateOfBlockOrdinal]
		} else { // we haven't yet see the block of which this is a dup, so requeue this one
			requeue = true
		}
	} else {

		blockList[result.Ordinal].ID = result.ItemID
		blockList[result.Ordinal].Status = "Uncommitted"
	}

	(*listInfo).List = blockList

	return
}

//WritePart implements WritePart from the pipeline.TargetPipeline interface.
//Performs a PUT block operation with the data contained in the part.
func (t AzureBlock) WritePart(part *pipeline.Part) (duration time.Duration, startTime time.Time, numOfRetries int, err error) {

	headers := make(map[string]string)
	//if the max retries is exceeded, panic will happen, hence no error is returned.
	duration, startTime, numOfRetries = util.RetriableOperation(func(r int) error {
		//computation of the MD5 happens is done by the readers.
		if part.IsMD5Computed() {
			headers["Content-MD5"] = part.MD5()
		}
		if err := t.StorageClient.PutBlockWithLength(t.Container,
			part.TargetAlias,
			part.BlockID,
			uint64(len(part.Data)),
			part.NewBuffer(),
			headers); err != nil {
			if util.Verbose {
				fmt.Printf("EH|S|%v|%v|%v|%v\n", (*part).BlockID, len((*part).Data), (*part).TargetAlias, err)
			}
			return err
		}

		if util.Verbose {
			fmt.Printf("OKA|S|%v|%v|%v|%v\n", (*part).BlockID, len((*part).Data), (*part).TargetAlias, err)
		}
		return nil
	})
	return
}
