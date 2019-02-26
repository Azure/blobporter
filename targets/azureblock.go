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
///// AzureBlock Target
////////////////////////////////////////////////////////////

//AzureBlockTarget represents an Azure Block target
type AzureBlockTarget struct {
	container     string
	azutil        *internal.AzUtil
	useServerSide bool
}

//NewAzureBlockTargetPipeline TODO
func NewAzureBlockTargetPipeline(params AzureTargetParams) pipeline.TargetPipeline {

	azutil, err := internal.NewAzUtil(params.AccountName, params.AccountKey, params.Container, params.BaseBlobURL)

	if err != nil {
		log.Fatal(err)
	}
	var notfound bool

	notfound, err = azutil.CreateContainerIfNotExists()

	if notfound {
		fmt.Printf("Info! Container was not found, creating it...\n")
	}

	if err != nil {
		log.Fatal(err)
	}

	return &AzureBlockTarget{azutil: azutil,
		useServerSide: params.UseServerSide,
		container:     params.Container}
}

//CommitList implements CommitList from the pipeline.TargetPipeline interface.
//Commits the list of blocks to Azure Storage to finalize the transfer.
func (t *AzureBlockTarget) CommitList(listInfo *pipeline.TargetCommittedListInfo, numberOfBlocks int, targetName string) (msg string, err error) {
	startTime := time.Now()

	//Only commit if the number blocks is greater than one.
	if numberOfBlocks == 1 && !t.useServerSide {
		msg = fmt.Sprintf("\rFile:%v, The blob is already committed.",
			targetName)
		err = nil
		return
	}

	lInfo := (*listInfo)
	blockList := convertToBase64EncodedList(lInfo.List, numberOfBlocks)
	defer func() {
		duration := time.Now().Sub(startTime)
		msg = fmt.Sprintf("\rFile:%v, Blocks Committed: %d, Commit time = %v",
			targetName, len(blockList), duration)
	}()
	util.PrintfIfDebug("Blocklist -> blob: %s\n list:%+v", targetName, blockList)

	err = t.azutil.PutBlockList(targetName, blockList)
	return
}

func convertToBase64EncodedList(list interface{}, numOfBlocks int) []string {

	if list == nil {
		return make([]string, numOfBlocks)
	}

	return list.([]string)
}

//PreProcessSourceInfo implementation of PreProcessSourceInfo from the pipeline.TargetPipeline interface.
//Checks if uncommitted blocks are present and cleans them by creating an empty blob.
func (t *AzureBlockTarget) PreProcessSourceInfo(source *pipeline.SourceInfo, blockSize uint64) (err error) {
	numOfBlocks := int(source.Size+(blockSize-1)) / int(blockSize)

	if numOfBlocks > util.MaxBlockCount { // more than 50,000 blocks needed, so can't work
		var minBlkSize = (source.Size + util.MaxBlockCount - 1) / util.MaxBlockCount
		return fmt.Errorf("Block size is too small, minimum block size for this file would be %d bytes", minBlkSize)
	}

	return nil
}

//ProcessWrittenPart implements ProcessWrittenPart from the pipeline.TargetPipeline interface.
//Appends the written part to a list. If the part is duplicated the list is updated with a reference, to the first occurrence of the block.
//If the first occurrence has not yet being processed, the part is requested to be placed back in the results channel (requeue == true).
func (t *AzureBlockTarget) ProcessWrittenPart(result *pipeline.WorkerResult, listInfo *pipeline.TargetCommittedListInfo) (requeue bool, err error) {
	requeue = false
	blockList := convertToBase64EncodedList((*listInfo).List, result.NumberOfBlocks)

	if result.DuplicateOfBlockOrdinal >= 0 { // this block is a duplicate of another.
		if blockList[result.DuplicateOfBlockOrdinal] != "" {
			blockList[result.Ordinal] = blockList[result.DuplicateOfBlockOrdinal]
		} else { // we haven't yet see the block of which this is a dup, so requeue this one
			requeue = true
		}
	} else {

		blockList[result.Ordinal] = result.ItemID
	}

	(*listInfo).List = blockList

	return
}

//WritePart implements WritePart from the pipeline.TargetPipeline interface.
//Performs a PUT block operation with the data contained in the part.
func (t *AzureBlockTarget) WritePart(part *pipeline.Part) (duration time.Duration, startTime time.Time, numOfRetries int, err error) {
	startTime = time.Now()
	defer func() { duration = time.Now().Sub(startTime) }()

	if t.useServerSide {
		err = t.azutil.PutBlockBlobFromURL(part.TargetAlias,
			part.BlockID,
			part.SourceURI,
			int64(part.Offset),
			int64(part.BytesToRead))

		return
	}

	//computation of the MD5 happens is done by the readers.
	var md5 []byte
	if part.IsMD5Computed() {
		md5 = part.MD5Bytes()
	}
	if part.NumberOfBlocks == 1 {
		//empty blob
		if part.BytesToRead == 0 {
			err = t.azutil.PutEmptyBlockBlob(part.TargetAlias)
			return
		}

		if err = t.azutil.PutBlockBlob(part.TargetAlias,
			bytes.NewReader(part.Data), md5); err != nil {
		}

		return
	}
	reader := bytes.NewReader(part.Data)

	err = t.azutil.PutBlock(t.container, part.TargetAlias, part.BlockID, reader, md5)

	return
}
