package transfer

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
var delegate = func(r pipeline.WorkerResult, committedCount int) {}
var numOfReaders = 10
var numOfWorkers = 10

func TestFileLegacyToBlob(t *testing.T) {
	var container = createContainer("bptest")
	var sourceFile = createFile("tb", 1)

	fp := sources.NewFilePipeline(sourceFile, sourceFile)
	creds := getStorageAccountCreds()
	ap := targets.NewAzureBlock(creds, container)
	tfer := NewTransfer(&fp, &ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None, delegate)
	tfer.WaitForCompletion()
}

func TestFileToBlob(t *testing.T) {
	var container = createContainer("bptest")
	var sourceFile = createFile("tb", 1)

	fp := sources.NewMultiFilePipeline(sourceFile, blockSize)
	creds := getStorageAccountCreds()
	ap := targets.NewAzureBlock(creds, container)
	tfer := NewTransfer(&fp, &ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None, delegate)
	tfer.WaitForCompletion()
}
func TestFileToBlobWithLargeBlocks(t *testing.T) {
	var container = createContainer("bptest")
	var sourceFile = createFile("tb", 1)
	bsize := uint64(16 * util.MiByte)

	fp := sources.NewMultiFilePipeline(sourceFile, bsize)
	creds := getStorageAccountCreds()
	ap := targets.NewAzureBlock(creds, container)
	tfer := NewTransfer(&fp, &ap, numOfReaders, numOfWorkers, bsize)
	tfer.StartTransfer(None, delegate)
	tfer.WaitForCompletion()
}
func TestFilesToBlob(t *testing.T) {
	var container = createContainer("bptest")
	createFile("tbm", 1)
	createFile("tbm", 1)
	createFile("tbm", 1)
	createFile("tbm", 1)

	fp := sources.NewMultiFilePipeline("tbm*", blockSize)
	creds := getStorageAccountCreds()
	ap := targets.NewAzureBlock(creds, container)
	tfer := NewTransfer(&fp, &ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None, delegate)
	tfer.WaitForCompletion()
}

func TestFileToBlobHTTPToBlob(t *testing.T) {
	var container = createContainer("bptest")
	var sourceFile = createFile("tb", 1)

	fp := sources.NewMultiFilePipeline(sourceFile, blockSize)
	creds := getStorageAccountCreds()
	ap := targets.NewAzureBlock(creds, container)
	tfer := NewTransfer(&fp, &ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None, delegate)
	tfer.WaitForCompletion()
	sourceURI, err := createSasTokenURL(sourceFile, container)

	assert.NoError(t, err)

	container = createContainer("bphttptest")
	ap = targets.NewAzureBlock(creds, container)
	fp = sources.NewHTTPPipeline(sourceURI, sourceFile)
	tfer = NewTransfer(&fp, &ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None, delegate)
	tfer.WaitForCompletion()

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
