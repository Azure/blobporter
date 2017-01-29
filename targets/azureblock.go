package targets

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/util"
)

// blobporter Tool
//
// Copyright (c) Microsoft Corporation
//
// All rights reserved.
//
// MIT License
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.
//

////////////////////////////////////////////////////////////
///// AzureBlock Target
////////////////////////////////////////////////////////////

//AzureBlock TODO
type AzureBlock struct {
	Creds     *pipeline.StorageAccountCredentials
	Container string
}

//NewAzureBlock TODO
func NewAzureBlock(creds *pipeline.StorageAccountCredentials, container string) pipeline.TargetPipeline {
	return AzureBlock{Creds: creds, Container: container}
}

//CommitList TODO
func (t AzureBlock) CommitList(listInfo *pipeline.TargetCommittedListInfo, numberOfBlocks int, targetName string) (msg string, err error) {

	lInfo := (*listInfo)
	//blobInfo := (*lInfo.Info)

	blockList := convertToStorageBlockList(lInfo.List, numberOfBlocks)

	if util.Verbose { //TODO: should add option to turn on this level of tracing
		fmt.Printf("Final BlockList:\n")
		for j := 0; j < numberOfBlocks; j++ {
			fmt.Printf("   [%2d]: ID=%s, Status=%s", j, blockList[j].ID, blockList[j].Status)
		}
	}

	//if the max retries is exceeded, panic will happen, hence no error is returned.
	duration, _, _ := util.RetriableOperation(func(r int) error {
		var bc = getBlobStorageClient(t.Creds)
		if err := bc.PutBlockList(t.Container, targetName, blockList); err != nil {
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

//ProcessWrittenPart TODO
func (t AzureBlock) ProcessWrittenPart(result *pipeline.WorkerResult, listInfo *pipeline.TargetCommittedListInfo) (requeue bool, err error) {
	//stInfo := (*result.Info)
	//lInfo := (*listInfo)
	requeue = false
	blockList := convertToStorageBlockList((*listInfo).List, result.NumberOfBlocks)

	// fmt.Printf("List ordinal %d being processed, dup of %d, offset=%d\n", wr.Ordinal, wr.DuplicateOfBlockOrdinal, wr.Offset)
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

//WritePart TODO
func (t AzureBlock) WritePart(part *pipeline.Part) (duration time.Duration, startTime time.Time, numOfRetries int, err error) {

	//if the max retries is exceeded, panic will happen, hence no error is returned.
	duration, startTime, numOfRetries = util.RetriableOperation(func(r int) error {
		bc := getBlobStorageClient(t.Creds)
		if err := bc.PutBlock(t.Container, (*part).SourceName, (*part).BlockID, (*part).Data); err != nil {
			if util.Verbose {
				//data, _ := base64.StdEncoding.DecodeString((*part).BlockID)
				fmt.Printf("EH|S|%v|%v|%v|%v\n", (*part).BlockID, len((*part).Data), (*part).SourceName, err)

			}
			bc = getBlobStorageClient(t.Creds) // reset to a fresh client for the retry
			return err
		}

		if util.Verbose {
			fmt.Printf("OKA|S|%v|%v|%v|%v\n", (*part).BlockID, len((*part).Data), (*part).SourceName, err)
		}

		return nil
	})
	(*part).ReturnBuffer()
	return
}

func getBlobStorageClient(creds *pipeline.StorageAccountCredentials) storage.BlobStorageClient {
	var bc storage.BlobStorageClient
	var client storage.Client
	var err error
	var accountName string
	var accountKey string

	if creds != nil {
		accountName = creds.AccountName
		accountKey = creds.AccountKey
	} else {
		accountName = os.Getenv("ACCOUNT_NAME") //TODO: clean up.. should always be in creds at this point
		accountKey = os.Getenv("ACCOUNT_KEY")
	}

	if accountName == "" || accountKey == "" {
		log.Fatal("Storage account and/or key not specified via options or in environment variables ACCOUNT_NAME and ACCOUNT_KEY")
	}

	if client, err = storage.NewClient(accountName, accountKey, storage.DefaultBaseURL, util.LargeBlockAPIVersion, true); err != nil {
		log.Fatal(err)
	}

	bc = client.GetBlobService()

	return bc
}
