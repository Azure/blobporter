package transfer

import (
	"testing"

	"github.com/Azure/blobporter/internal"

	"os"

	"fmt"
	"time"

	"log"

	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/sources"
	"github.com/Azure/blobporter/targets"
	"github.com/Azure/blobporter/util"
	"github.com/stretchr/testify/assert"
)

//********
//IMPORTANT:
// -- Tests require a valid storage account and env variables set up accordingly.
// -- Tests create a working directory named transfer_testdata and temp files under it. Plase make sure the working directory is in .gitignore
//********
var sourceFiles = make([]string, 1)

var accountName = os.Getenv("ACCOUNT_NAME")
var accountKey = os.Getenv("ACCOUNT_KEY")

var blockSize = uint64(4 * util.MiByte)
var delegate = func(r pipeline.WorkerResult, committedCount int, bufferLevel int) {}
var numOfReaders = 10
var numOfWorkers = 10
var filesPerPipeline = 10
var targetBaseBlobURL = ""

const (
	containerName1 = "bptest"
	containerName2 = "bphttptest"
	tempDir        = "transfer_testdata"
)

func TestFileToPageHTTPToPage(t *testing.T) {
	container, containerHTTP := getContainers()

	var sourceFile = createPageFile("tb", 1)

	sourceParams := &sources.FileSystemSourceParams{
		SourcePatterns:  []string{sourceFile},
		BlockSize:       blockSize,
		NumOfPartitions: numOfReaders,
		SourceParams: sources.SourceParams{
			FilesPerPipeline: filesPerPipeline,
			KeepDirStructure: true,
			CalculateMD5:     true}}

	fpf := <-sources.NewFileSystemSourcePipelineFactory(sourceParams)
	fp := fpf.Source

	tparams := targets.AzureTargetParams{
		AccountName: accountName,
		AccountKey:  accountKey,
		Container:   container,
		BaseBlobURL: targetBaseBlobURL}

	ap := targets.NewAzurePageTargetPipeline(tparams)
	tfer := NewTransfer(fp, ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None)
	tfer.WaitForCompletion()
	sourceURI, err := createSasTokenURL(sourceFile, container)

	assert.NoError(t, err)
	tparams = targets.AzureTargetParams{
		AccountName: accountName,
		AccountKey:  accountKey,
		Container:   containerHTTP,
		BaseBlobURL: targetBaseBlobURL}

	ap = targets.NewAzurePageTargetPipeline(tparams)

	httpparams := sources.HTTPSourceParams{SourceParams: sources.SourceParams{
		CalculateMD5: true},
		SourceURIs:    []string{sourceURI},
		TargetAliases: []string{sourceFile},
	}

	fpf = <-sources.NewHTTPSourcePipelineFactory(httpparams)
	fp = fpf.Source
	tfer = NewTransfer(fp, ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None)
	tfer.WaitForCompletion()

	os.Remove(sourceFile)
}

func TestFileToPage(t *testing.T) {
	var sourceFile = createPageFile("tb", 1)
	container, _ := getContainers()

	sourceParams := &sources.FileSystemSourceParams{
		SourcePatterns:  []string{sourceFile},
		BlockSize:       blockSize,
		NumOfPartitions: numOfReaders,
		SourceParams: sources.SourceParams{
			FilesPerPipeline: filesPerPipeline,
			KeepDirStructure: true,
			CalculateMD5:     true}}

	fpf := <-sources.NewFileSystemSourcePipelineFactory(sourceParams)
	fp := fpf.Source
	tparams := targets.AzureTargetParams{
		AccountName: accountName,
		AccountKey:  accountKey,
		Container:   container,
		BaseBlobURL: targetBaseBlobURL}

	pt := targets.NewAzurePageTargetPipeline(tparams)

	tfer := NewTransfer(fp, pt, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None)
	tfer.WaitForCompletion()

	os.Remove(sourceFile)
}

func TestFileToFile(t *testing.T) {
	var sourceFile = createFile("tb", 1)
	var destFile = sourceFile + "d"

	sourceParams := &sources.FileSystemSourceParams{
		SourcePatterns:  []string{sourceFile},
		BlockSize:       blockSize,
		NumOfPartitions: numOfReaders,
		TargetAliases:   []string{destFile},
		SourceParams: sources.SourceParams{
			FilesPerPipeline: filesPerPipeline,
			KeepDirStructure: true,
			CalculateMD5:     true}}

	fpf := <-sources.NewFileSystemSourcePipelineFactory(sourceParams)
	fp := fpf.Source
	ft := targets.NewFileSystemTargetPipeline(true, numOfWorkers)

	tfer := NewTransfer(fp, ft, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None)
	tfer.WaitForCompletion()

	os.Remove(sourceFile)
	os.Remove(destFile)

}

func TestFileToBlob(t *testing.T) {
	container, _ := getContainers()
	var sourceFile = createFile("tb", 1)

	sourceParams := &sources.FileSystemSourceParams{
		SourcePatterns:  []string{sourceFile},
		BlockSize:       blockSize,
		NumOfPartitions: numOfReaders,
		SourceParams: sources.SourceParams{
			FilesPerPipeline: filesPerPipeline,
			KeepDirStructure: true,
			CalculateMD5:     true}}

	fpf := <-sources.NewFileSystemSourcePipelineFactory(sourceParams)
	fp := fpf.Source
	tparams := targets.AzureTargetParams{
		AccountName: accountName,
		AccountKey:  accountKey,
		Container:   container}

	ap := targets.NewAzureBlockTargetPipeline(tparams)

	tfer := NewTransfer(fp, ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None)
	tfer.WaitForCompletion()

	os.Remove(sourceFile)

}
func TestFileToBlobToBlock(t *testing.T) {
	container, _ := getContainers()
	var sourceFile = createFile("tb", 1)

	sourceParams := &sources.FileSystemSourceParams{
		SourcePatterns:  []string{sourceFile},
		BlockSize:       blockSize,
		NumOfPartitions: numOfReaders,
		SourceParams: sources.SourceParams{
			FilesPerPipeline: filesPerPipeline,
			KeepDirStructure: true,
			CalculateMD5:     true}}

	fpf := <-sources.NewFileSystemSourcePipelineFactory(sourceParams)
	fp := fpf.Source
	tparams := targets.AzureTargetParams{
		AccountName: accountName,
		AccountKey:  accountKey,
		Container:   container}

	ap := targets.NewAzureBlockTargetPipeline(tparams)
	tfer := NewTransfer(fp, ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None)
	tfer.WaitForCompletion()

	os.Remove(sourceFile)

}
func TestFileToBlobWithLargeBlocks(t *testing.T) {
	container, _ := getContainers()
	var sourceFile = createFile("tb", 1)
	bsize := uint64(16 * util.MiByte)

	sourceParams := &sources.FileSystemSourceParams{
		SourcePatterns:  []string{sourceFile},
		BlockSize:       blockSize,
		NumOfPartitions: numOfReaders,
		SourceParams: sources.SourceParams{
			FilesPerPipeline: filesPerPipeline,
			KeepDirStructure: true,
			CalculateMD5:     true}}

	fpf := <-sources.NewFileSystemSourcePipelineFactory(sourceParams)
	fp := fpf.Source
	tparams := targets.AzureTargetParams{
		AccountName: accountName,
		AccountKey:  accountKey,
		Container:   container}

	ap := targets.NewAzureBlockTargetPipeline(tparams)

	tfer := NewTransfer(fp, ap, numOfReaders, numOfWorkers, bsize)
	tfer.StartTransfer(None)
	tfer.WaitForCompletion()

	os.Remove(sourceFile)
}
func TestFilesToBlob(t *testing.T) {
	container, _ := getContainers()

	var sf1 = createFile("tbm", 1)
	var sf2 = createFile("tbm", 1)
	var sf3 = createFile("tbm", 1)
	var sf4 = createFile("tbm", 1)

	sourceParams := &sources.FileSystemSourceParams{
		SourcePatterns:  []string{tempDir + "/tbm*"},
		BlockSize:       blockSize,
		NumOfPartitions: numOfReaders,
		SourceParams: sources.SourceParams{
			FilesPerPipeline: filesPerPipeline,
			KeepDirStructure: true,
			CalculateMD5:     true}}
	fpf := <-sources.NewFileSystemSourcePipelineFactory(sourceParams)
	fp := fpf.Source
	tparams := targets.AzureTargetParams{
		AccountName: accountName,
		AccountKey:  accountKey,
		Container:   container,
		BaseBlobURL: ""}

	ap := targets.NewAzureBlockTargetPipeline(tparams)

	tfer := NewTransfer(fp, ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None)
	tfer.WaitForCompletion()

	os.Remove(sf1)
	os.Remove(sf2)
	os.Remove(sf3)
	os.Remove(sf4)
}

func TestFileToBlobHTTPToBlob(t *testing.T) {
	container, containerHTTP := getContainers()

	var sourceFile = createFile("tb", 1)

	sourceParams := &sources.FileSystemSourceParams{
		SourcePatterns:  []string{sourceFile},
		BlockSize:       blockSize,
		NumOfPartitions: numOfReaders,
		SourceParams: sources.SourceParams{
			FilesPerPipeline: filesPerPipeline,
			KeepDirStructure: true,
			CalculateMD5:     true}}

	fpf := <-sources.NewFileSystemSourcePipelineFactory(sourceParams)
	fp := fpf.Source

	tparams := targets.AzureTargetParams{
		AccountName: accountName,
		AccountKey:  accountKey,
		Container:   container,
		BaseBlobURL: ""}

	ap := targets.NewAzureBlockTargetPipeline(tparams)

	tfer := NewTransfer(fp, ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None)
	tfer.WaitForCompletion()
	sourceURI, err := createSasTokenURL(sourceFile, container)

	assert.NoError(t, err)

	tparams = targets.AzureTargetParams{
		AccountName: accountName,
		AccountKey:  accountKey,
		Container:   containerHTTP,
		BaseBlobURL: ""}

	ap = targets.NewAzureBlockTargetPipeline(tparams)

	httpparams := sources.HTTPSourceParams{SourceParams: sources.SourceParams{
		CalculateMD5: true},
		SourceURIs:    []string{sourceURI},
		TargetAliases: []string{sourceFile},
	}

	fpf = <-sources.NewHTTPSourcePipelineFactory(httpparams)
	fp = fpf.Source
	tfer = NewTransfer(fp, ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None)
	tfer.WaitForCompletion()

	os.Remove(sourceFile)
}
func TestFileToBlobHTTPToFile(t *testing.T) {
	container, _ := getContainers()
	var sourceFile = createFile("tb", 1)

	sourceParams := &sources.FileSystemSourceParams{
		SourcePatterns:  []string{sourceFile},
		BlockSize:       blockSize,
		NumOfPartitions: numOfReaders,
		SourceParams: sources.SourceParams{
			FilesPerPipeline: filesPerPipeline,
			KeepDirStructure: true,
			CalculateMD5:     true}}

	fpf := <-sources.NewFileSystemSourcePipelineFactory(sourceParams)
	fp := fpf.Source
	tparams := targets.AzureTargetParams{
		AccountName: accountName,
		AccountKey:  accountKey,
		Container:   container,
		BaseBlobURL: ""}

	ap := targets.NewAzureBlockTargetPipeline(tparams)

	tfer := NewTransfer(fp, ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None)
	tfer.WaitForCompletion()
	sourceURI, err := createSasTokenURL(sourceFile, container)

	assert.NoError(t, err)

	sourceFiles[0] = sourceFile + "d"
	ap = targets.NewFileSystemTargetPipeline(true, numOfWorkers)

	httpparams := sources.HTTPSourceParams{SourceParams: sources.SourceParams{
		CalculateMD5: true},
		SourceURIs:    []string{sourceURI},
		TargetAliases: []string{sourceFile},
	}

	fpf = <-sources.NewHTTPSourcePipelineFactory(httpparams)
	fp = fpf.Source
	tfer = NewTransfer(fp, ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None)
	tfer.WaitForCompletion()

	os.Remove(sourceFile + "d")
	os.Remove(sourceFile)

}
func TestFileToBlobToFile(t *testing.T) {
	container, _ := getContainers()
	var sourceFile = createFile("tb", 1)

	sourceParams := &sources.FileSystemSourceParams{
		SourcePatterns:  []string{sourceFile},
		BlockSize:       blockSize,
		NumOfPartitions: numOfReaders,
		SourceParams: sources.SourceParams{
			FilesPerPipeline: filesPerPipeline,
			KeepDirStructure: true,
			CalculateMD5:     true}}

	fpf := <-sources.NewFileSystemSourcePipelineFactory(sourceParams)
	fp := fpf.Source
	tparams := targets.AzureTargetParams{
		AccountName: accountName,
		AccountKey:  accountKey,
		Container:   container,
		BaseBlobURL: ""}

	ap := targets.NewAzureBlockTargetPipeline(tparams)
	tfer := NewTransfer(fp, ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None)
	tfer.WaitForCompletion()

	ap = targets.NewFileSystemTargetPipeline(true, numOfWorkers)
	azureBlobParams := &sources.AzureBlobParams{
		Container:   container,
		BlobNames:   []string{sourceFile},
		AccountName: accountName,
		AccountKey:  accountKey,
		SourceParams: sources.SourceParams{
			FilesPerPipeline:  filesPerPipeline,
			CalculateMD5:      true,
			KeepDirStructure:  true,
			UseExactNameMatch: false}}

	fpf = <-sources.NewAzBlobSourcePipelineFactory(azureBlobParams)
	fp = fpf.Source
	tfer = NewTransfer(fp, ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None)
	tfer.WaitForCompletion()

	os.Remove(sourceFile)

}
func TestFileToBlobToFileWithAlias(t *testing.T) {
	container, _ := getContainers()
	var sourceFile = createFile("tb", 1)
	var alias = sourceFile + "alias"

	sourceParams := &sources.FileSystemSourceParams{
		SourcePatterns:  []string{sourceFile},
		BlockSize:       blockSize,
		TargetAliases:   []string{alias},
		NumOfPartitions: numOfReaders,
		SourceParams: sources.SourceParams{
			FilesPerPipeline: filesPerPipeline,
			KeepDirStructure: true,
			CalculateMD5:     true}}

	fpf := <-sources.NewFileSystemSourcePipelineFactory(sourceParams)
	fp := fpf.Source
	tparams := targets.AzureTargetParams{
		AccountName: accountName,
		AccountKey:  accountKey,
		Container:   container,
		BaseBlobURL: ""}

	ap := targets.NewAzureBlockTargetPipeline(tparams)

	tfer := NewTransfer(fp, ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None)
	tfer.WaitForCompletion()

	ap = targets.NewFileSystemTargetPipeline(true, numOfWorkers)
	azureBlobParams := &sources.AzureBlobParams{
		Container:   container,
		BlobNames:   []string{alias},
		AccountName: accountName,
		AccountKey:  accountKey,
		SourceParams: sources.SourceParams{
			FilesPerPipeline:  filesPerPipeline,
			CalculateMD5:      true,
			KeepDirStructure:  true,
			UseExactNameMatch: true}}
	fpf = <-sources.NewAzBlobSourcePipelineFactory(azureBlobParams)
	fp = fpf.Source
	tfer = NewTransfer(fp, ap, numOfReaders, numOfWorkers, blockSize)
	tfer.StartTransfer(None)
	tfer.WaitForCompletion()

	os.Remove(sourceFile)
	os.Remove(alias)

}
func deleteContainer(containerName string) {
	az, err := internal.NewAzUtil(accountName, accountKey, containerName, "")

	err = az.DeleteContainerIfNotExists()
	if err != nil {
		panic(err)
	}
}
func getContainers() (string, string) {
	return createContainer(containerName1), createContainer(containerName2)
}

func createContainer(containerName string) string {
	az, err := internal.NewAzUtil(accountName, accountKey, containerName, "")

	_, err = az.CreateContainerIfNotExists()
	if err != nil {
		panic(err)
	}
	return containerName
}

/*
func getClient() storage.BlobStorageClient {
	client, err := storage.NewBasicClient(accountName, accountKey)

	if err != nil {
		panic(err)
	}

	return client.GetBlobService()

}
*/
func createSasTokenURL(blobName string, container string) (string, error) {
	az, err := internal.NewAzUtil(accountName, accountKey, container, "")

	if err != nil {
		return "", err
	}

	date := time.Now().UTC().AddDate(0, 0, 3)
	burl, err := az.GetBlobURLWithReadOnlySASToken(blobName, date)

	if err != nil {
		return "", err
	}

	return burl.String(), nil

}
func getStorageAccountCreds() *pipeline.StorageAccountCredentials {
	return &pipeline.StorageAccountCredentials{AccountName: accountName, AccountKey: accountKey}
}
func createPageFile(prefix string, sizeInMB int) string {
	var err error

	fileName := fmt.Sprintf("%s/%v%v", tempDir, prefix, time.Now().Nanosecond())

	err = os.MkdirAll(tempDir, 0777)

	if err != nil {
		log.Fatal(err)
	}

	var file *os.File

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

	for i := 0; i < sizeInMB; i++ {
		file.Write(b)
	}

	return fileName
}

func createFile(prefix string, approxSizeInMB int) string {

	fileName := fmt.Sprintf("%s/%v%v", tempDir, prefix, time.Now().Nanosecond())

	err := os.MkdirAll(tempDir, 0777)

	if err != nil {
		log.Fatal(err)
	}

	var file *os.File

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
