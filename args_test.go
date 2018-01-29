package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/Azure/blobporter/transfer"
	"github.com/Azure/blobporter/util"
	"github.com/stretchr/testify/assert"
)

func TestBasicUpload(t *testing.T) {
	val := newParamParserValidator()
	val.args.sourceURIs = []string{"data"}
	val.args.storageAccountName = "myaccount"
	val.args.storageAccountKey = "mykey"
	val.args.containerName = "mycont"

	err := val.parseAndValidate()
	assert.NoError(t, err, "un expected error, all params should be set")
	assert.Equal(t, val.args.storageAccountName, val.params.blobTarget.accountName, "account name is not set")
	assert.Equal(t, val.args.storageAccountKey, val.params.blobTarget.accountKey, "account key is not set")
	assert.Equal(t, val.args.containerName, val.params.blobTarget.container, "container is not set")
	assert.Equal(t, val.args.sourceURIs[0], val.params.sourceURIs[0], "source is missing")
}
func TestBasicUploadWithAlias(t *testing.T) {
	val := newParamParserValidator()
	val.args.sourceURIs = []string{"data"}
	val.args.blobNames = []string{"data2"}
	val.args.storageAccountName = "myaccount"
	val.args.storageAccountKey = "mykey"
	val.args.containerName = "mycont"

	err := val.parseAndValidate()
	assert.NoError(t, err, "un expected error, all params should be set")
	assert.Equal(t, val.args.storageAccountName, val.params.blobTarget.accountName, "account name is not set")
	assert.Equal(t, val.args.storageAccountKey, val.params.blobTarget.accountKey, "account key is not set")
	assert.Equal(t, val.args.containerName, val.params.blobTarget.container, "container is not set")
	assert.Equal(t, val.args.sourceURIs[0], val.params.sourceURIs[0], "source is missing")
	assert.Equal(t, val.args.blobNames[0], val.params.targetAliases[0], "target alias is missing")
}
func TestBasicUploaBlockSizeLimits(t *testing.T) {
	val := newParamParserValidator()
	val.args.sourceURIs = []string{"data"}
	val.args.blobNames = []string{"data2"}
	val.args.storageAccountName = "myaccount"
	val.args.storageAccountKey = "mykey"
	val.args.containerName = "mycont"
	val.args.blockSizeStr = "16MB"
	bs, err := util.ByteCountFromSizeString("16MB")
	assert.NoError(t, err, "byte size could not be parsed")

	err = val.parseAndValidate()
	assert.NoError(t, err, "unexpected error, all params should be set")
	assert.Equal(t, val.args.storageAccountName, val.params.blobTarget.accountName, "account name is not set")
	assert.Equal(t, val.args.storageAccountKey, val.params.blobTarget.accountKey, "account key is not set")
	assert.Equal(t, val.args.containerName, val.params.blobTarget.container, "container is not set")
	assert.Equal(t, val.args.sourceURIs[0], val.params.sourceURIs[0], "source is missing")
	assert.Equal(t, val.args.blobNames[0], val.params.targetAliases[0], "target alias is missing")
	assert.Equal(t, bs, val.params.blockSize, "blocksize don't match")

	val.args.blockSizeStr = "0"
	err = val.parseAndValidate()
	assert.Error(t, err, "expected to fail as it is an invalid block size")

	val.args.blockSizeStr = "101MB"
	err = val.parseAndValidate()
	assert.Error(t, err, "expected to fail as it is an invalid block size")
}
func TestBasicUploaBlockSizeLimitsForPageBlobs(t *testing.T) {
	val := newParamParserValidator()
	val.args.sourceURIs = []string{"data"}
	val.args.blobNames = []string{"data2"}
	val.args.storageAccountName = "myaccount"
	val.args.storageAccountKey = "mykey"
	val.args.containerName = "mycont"
	val.args.blockSizeStr = "2MB"
	val.args.transferDefStr = "file-pageblob"
	bs, err := util.ByteCountFromSizeString("2MB")
	assert.NoError(t, err, "byte size could not be parsed")

	err = val.parseAndValidate()
	assert.NoError(t, err, "unexpected error, all params should be set")
	assert.Equal(t, val.args.storageAccountName, val.params.blobTarget.accountName, "account name is not set")
	assert.Equal(t, val.args.storageAccountKey, val.params.blobTarget.accountKey, "account key is not set")
	assert.Equal(t, val.args.containerName, val.params.blobTarget.container, "container is not set")
	assert.Equal(t, val.args.sourceURIs[0], val.params.sourceURIs[0], "source is missing")
	assert.Equal(t, val.args.blobNames[0], val.params.targetAliases[0], "target alias is missing")
	assert.Equal(t, bs, val.params.blockSize, "blocksize don't match")
	assert.Equal(t, transfer.FileToPage, string(val.params.transferType), "transfer definition does not match")

	val.args.blockSizeStr = "0"
	err = val.parseAndValidate()
	assert.Error(t, err, "expected to fail as it is an invalid block size")

	val.args.blockSizeStr = "513"
	err = val.parseAndValidate()
	assert.Error(t, err, "expected to fail as it is an invalid block size")

	val.args.blockSizeStr = "512"
	err = val.parseAndValidate()
	assert.NoError(t, err, "unexpected error, 512 is a valid page size")

	//test auto adjusment
	val.args.blockSizeStr = "5MB"
	err = val.parseAndValidate()
	assert.NoError(t, err, "unexpected error, the block size should adjusted to 4MB")

	bs, err = util.ByteCountFromSizeString("4MB")
	assert.NoError(t, err, "byte size could not be parsed")

	assert.Equal(t, bs, val.params.blockSize, "block size does not match")

}
func TestShortOptionDownload(t *testing.T) {
	val := newParamParserValidator()
	val.args.blobNames = []string{"data"}
	val.args.storageAccountName = "myaccount"
	val.args.storageAccountKey = "mykey"
	val.args.containerName = "mycont"
	val.args.transferDefStr = "blob-file"

	err := val.parseAndValidate()
	assert.NoError(t, err, "un expected error, all params should be set")
	assert.Equal(t, val.args.storageAccountName, val.params.blobSource.accountName, "account name is not set")
	assert.Equal(t, val.args.storageAccountKey, val.params.blobSource.accountKey, "account key is not set")
	assert.Equal(t, val.args.containerName, val.params.blobSource.container, "container is not set")
	assert.Equal(t, val.args.blobNames[0], val.params.blobSource.prefixes[0], "blobname is missing")
}

func TestLongOptionDownload(t *testing.T) {
	val := newParamParserValidator()
	back := os.Getenv(sourceAuthorizationEnvVar)

	tempval := back
	if tempval == "" {
		tempval = "TEST_KEY"
	}
	os.Setenv(sourceAuthorizationEnvVar, tempval)

	defer os.Setenv(sourceAuthorizationEnvVar, back)

	a := "myaccount"
	c := "mycontainer"
	b := "myblob"
	val.args.sourceURIs = []string{fmt.Sprintf("http://%s.blob.core.windows.net/%s/%s", a, c, b)}
	val.args.sourceAuthorization = "mykey"
	val.args.transferDefStr = "blob-file"

	err := val.parseAndValidate()
	assert.NoError(t, err, "un expected error, all params should be set")
	assert.Equal(t, a, val.params.blobSource.accountName, "account name is not set")
	assert.Equal(t, tempval, val.params.blobSource.accountKey, "account key is not set")
	assert.Equal(t, c, val.params.blobSource.container, "container is not set")
	assert.Equal(t, b, val.params.blobSource.prefixes[0], "blobname is missing")
}
