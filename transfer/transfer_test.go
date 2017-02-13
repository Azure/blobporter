package transfer

// BlobPorter Tool
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

import (
	"testing"

	"os"

	"fmt"
	"time"

	"log"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/sources"
	"github.com/Azure/blobporter/targets"
	"github.com/Azure/blobporter/util"
	"github.com/stretchr/testify/assert"
)

var accountName = os.Getenv("ACCOUNT_NAME")
var accountKey = os.Getenv("ACCOUNT_KEY")
var blockSize = uint64(4 * util.MiByte)
var delegate = func(r pipeline.WorkerResult, committedCount int, bufferLevel int) {}
var numOfReaders = 10
var numOfWorkers = 10

const (
	containerName1 = "bptest"
	containerName2 = "bphttptest"
)

func TestFileToBlob(t *testing.T) {
	container, _ := getContainers()
	var sourceFile = createFile("tb", 1)

	fp := sources.NewMultiFilePipeline(sourceFile, blockSize, "")
	ap := targets.NewAzureBlock(accountName, accountKey, container)
	tfer := NewTransfer(&fp, &ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None, delegate)
	tfer.WaitForCompletion()

	os.Remove(sourceFile)

}
func TestFileToBlobWithLargeBlocks(t *testing.T) {
	container, _ := getContainers()
	var sourceFile = createFile("tb", 1)
	bsize := uint64(16 * util.MiByte)

	fp := sources.NewMultiFilePipeline(sourceFile, bsize, "")
	ap := targets.NewAzureBlock(accountName, accountKey, container)
	tfer := NewTransfer(&fp, &ap, numOfReaders, numOfWorkers, bsize)
	tfer.StartTransfer(None, delegate)
	tfer.WaitForCompletion()

	os.Remove(sourceFile)
}
func TestFilesToBlob(t *testing.T) {
	container, _ := getContainers()

	var sf1 = createFile("tbm", 1)
	var sf2 = createFile("tbm", 1)
	var sf3 = createFile("tbm", 1)
	var sf4 = createFile("tbm", 1)

	fp := sources.NewMultiFilePipeline("tbm*", blockSize, "")
	ap := targets.NewAzureBlock(accountName, accountKey, container)
	tfer := NewTransfer(&fp, &ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None, delegate)
	tfer.WaitForCompletion()

	os.Remove(sf1)
	os.Remove(sf2)
	os.Remove(sf3)
	os.Remove(sf4)
}

func TestFileToBlobHTTPToBlob(t *testing.T) {
	container, containerHTTP := getContainers()

	var sourceFile = createFile("tb", 1)

	fp := sources.NewMultiFilePipeline(sourceFile, blockSize, "")
	ap := targets.NewAzureBlock(accountName, accountKey, container)
	tfer := NewTransfer(&fp, &ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None, delegate)
	tfer.WaitForCompletion()
	sourceURI, err := createSasTokenURL(sourceFile, container)

	assert.NoError(t, err)

	ap = targets.NewAzureBlock(accountName, accountKey, containerHTTP)
	fp = sources.NewHTTPPipeline(sourceURI, sourceFile)
	tfer = NewTransfer(&fp, &ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None, delegate)
	tfer.WaitForCompletion()

	os.Remove(sourceFile)
}
func TestFileToBlobHTTPToFile(t *testing.T) {
	container, _ := getContainers()
	var sourceFile = createFile("tb", 1)

	fp := sources.NewMultiFilePipeline(sourceFile, blockSize, "")
	ap := targets.NewAzureBlock(accountName, accountKey, container)
	tfer := NewTransfer(&fp, &ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None, delegate)
	tfer.WaitForCompletion()
	sourceURI, err := createSasTokenURL(sourceFile, container)

	assert.NoError(t, err)

	ap = targets.NewFile("d"+sourceFile, true, numOfWorkers)
	fp = sources.NewHTTPPipeline(sourceURI, sourceFile)
	tfer = NewTransfer(&fp, &ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None, delegate)
	tfer.WaitForCompletion()

	os.Remove("d" + sourceFile)
	os.Remove(sourceFile)

}
func TestFileToBlobToFile(t *testing.T) {
	container, _ := getContainers()
	var sourceFile = createFile("tb", 1)

	fp := sources.NewMultiFilePipeline(sourceFile, blockSize, "")
	ap := targets.NewAzureBlock(accountName, accountKey, container)
	tfer := NewTransfer(&fp, &ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None, delegate)
	tfer.WaitForCompletion()

	ap = targets.NewFile("d"+sourceFile, true, numOfWorkers)
	fp = sources.NewHTTPAzureBlockPipeline(container, sourceFile, accountName, accountKey)
	tfer = NewTransfer(&fp, &ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None, delegate)
	tfer.WaitForCompletion()

	os.Remove("d" + sourceFile)
	os.Remove(sourceFile)

}
func TestFileToBlobToFileWithAlias(t *testing.T) {
	container, _ := getContainers()
	var sourceFile = createFile("tb", 1)
	var alias = "x" + sourceFile

	fp := sources.NewMultiFilePipeline(sourceFile, blockSize, alias)
	ap := targets.NewAzureBlock(accountName, accountKey, container)
	tfer := NewTransfer(&fp, &ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None, delegate)
	tfer.WaitForCompletion()

	ap = targets.NewFile(alias, true, numOfWorkers)
	fp = sources.NewHTTPAzureBlockPipeline(container, alias, accountName, accountKey)
	tfer = NewTransfer(&fp, &ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None, delegate)
	tfer.WaitForCompletion()

	os.Remove(alias)
	os.Remove(sourceFile)

}
func deleteContainer(containerName string) {
	_, err := getClient().DeleteContainerIfExists(containerName)
	if err != nil {
		panic(err)
	}
}
func getContainers() (string, string) {
	return createContainer(containerName1), createContainer(containerName2)
}

func createContainer(containerName string) string {
	_, err := getClient().CreateContainerIfNotExists(containerName, storage.ContainerAccessTypePrivate)

	if err != nil {
		panic(err)
	}
	return containerName
}
func getClient() storage.BlobStorageClient {
	client, err := storage.NewBasicClient(accountName, accountKey)

	if err != nil {
		panic(err)
	}

	return client.GetBlobService()

}
func createSasTokenURL(blobName string, container string) (string, error) {
	date := time.Now().UTC().AddDate(0, 0, 3)
	return getClient().GetBlobSASURI(container, blobName, date, "r")
}
func getStorageAccountCreds() *pipeline.StorageAccountCredentials {
	return &pipeline.StorageAccountCredentials{AccountName: accountName, AccountKey: accountKey}
}

func createFile(prefix string, approxSizeInMB int) string {
	fileName := fmt.Sprintf("%v%v", prefix, time.Now().Nanosecond())
	var file *os.File
	var err error

	defer func() {
		if file != nil {
			file.Close()
		}
	}()

	if file, err = os.Create(fileName); os.IsExist(err) {
		if err = os.Remove(fileName); err != nil {
			log.Fatal(err)
		}
		if file, err = os.Create(fileName); err != nil {
			log.Fatal(err)
		}
	}

	b := make([]byte, util.MiByte)

	for i := 0; i < util.MiByte; i++ {
		b[i] = 1
	}

	for i := 0; i < approxSizeInMB; i++ {
		file.Write(b)
	}

	//Add a more data to make the size not a multiple of 1 MB
	file.Write(b[:123])

	return fileName
}
