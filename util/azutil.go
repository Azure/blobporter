package util

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
)

//AzUtil TODO
type AzUtil struct {
	serviceURL   *azblob.ServiceURL
	containerURL *azblob.ContainerURL
	creds        *azblob.SharedKeyCredential
}

//NewAzUtil TODO
func NewAzUtil(accountName string, accountKey string, container string, baseBlobURL string) (*AzUtil, error) {

	creds := azblob.NewSharedKeyCredential(accountName, accountKey)

	ua, _ := GetUserAgentInfo()

	pipeline := azblob.NewPipeline(creds, azblob.PipelineOptions{
		Telemetry: azblob.TelemetryOptions{
			Value: fmt.Sprintf("%s/%s", ua, azblob.UserAgent())},
		Retry: azblob.RetryOptions{
			Policy:        azblob.RetryPolicyFixed,
			MaxTries:      1000,
			RetryDelay:    200 * time.Millisecond,
			MaxRetryDelay: 5 * time.Minute}})

	baseURL, err := parseBaseURL(accountName, baseBlobURL)
	if err != nil {
		return nil, err
	}

	surl := azblob.NewServiceURL(*baseURL, pipeline)
	curl := surl.NewContainerURL(container)

	return &AzUtil{serviceURL: &surl,
		containerURL: &curl,
		creds:        creds}, nil
}

//CreateContainerIfNotExists returs true if the container did not exist.
func (p *AzUtil) CreateContainerIfNotExists() (bool, error) {
	ctx := context.Background()
	response, err := p.containerURL.GetPropertiesAndMetadata(ctx, azblob.LeaseAccessConditions{})

	if response != nil {
		defer response.Response().Body.Close()
		if response.StatusCode() == 200 {
			return false, nil
		}
	}

	if err != nil {
		storageErr := err.(azblob.StorageError)
		errResp := storageErr.Response()
		if errResp != nil && errResp.StatusCode == 404 {
			//not found
			_, err = p.containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)

			if err != nil {
				return true, err
			}

			return true, nil

		}
	}

	return false, err
}

func (p *AzUtil) blobExists(bburl azblob.BlockBlobURL) (bool, error) {
	ctx := context.Background()
	response, err := bburl.GetPropertiesAndMetadata(ctx, azblob.BlobAccessConditions{})

	if response != nil {
		defer response.Response().Body.Close()
		if response.StatusCode() == 200 {
			return true, nil
		}
	}

	storageErr, ok := err.(azblob.StorageError)

	if ok {
		errResp := storageErr.Response()
		if errResp != nil {
			defer errResp.Body.Close()
			if errResp.StatusCode == 404 {
				return false, nil
			}
		}
	}

	return false, err
}

//CleanUncommittedBlocks TODO
func (p *AzUtil) CleanUncommittedBlocks(blobName string) error {
	bburl := p.containerURL.NewBlockBlobURL(blobName)

	exists, err := p.blobExists(bburl)

	if !exists {
		return nil
	}

	if err != nil {
		return err
	}

	var blst *azblob.BlockList
	ctx := context.Background()

	blst, err = bburl.GetBlockList(ctx, azblob.BlockListUncommitted, azblob.LeaseAccessConditions{})

	if blst != nil {
		defer blst.Response().Body.Close()
		if len(blst.UncommittedBlocks) == 0 {
			blst.Response().Body.Close()
			return nil
		}
	}

	fmt.Printf("Warning! Uncommitted blocks detected for a large blob %v \nAttempting to clean them up\n", blobName)

	if err != nil {
		return err
	}

	empty := make([]byte, 0)

	var resp *azblob.BlobsPutResponse
	ctx = context.Background()

	resp, err = bburl.PutBlob(ctx, bytes.NewReader(empty), azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{})
	defer resp.Response().Body.Close()

	return err
}

//PutBlockList TODO
func (p *AzUtil) PutBlockList(blobName string, blockIDs []string) error {
	bburl := p.containerURL.NewBlockBlobURL(blobName)

	h := azblob.BlobHTTPHeaders{}
	ctx := context.Background()
	resp, err := bburl.PutBlockList(ctx, blockIDs, azblob.Metadata{}, h, azblob.BlobAccessConditions{})

	if err != nil {
		return err
	}
	resp.Response().Body.Close()

	return nil
}

//PutBlock TODO
func (p *AzUtil) PutBlock(container string, blobName string, id string, body io.ReadSeeker) error {
	curl := p.serviceURL.NewContainerURL(container)
	bburl := curl.NewBlockBlobURL(blobName)

	ctx := context.Background()
	resp, err := bburl.PutBlock(ctx, id, body, azblob.LeaseAccessConditions{})

	if err != nil {
		return err
	}
	resp.Response().Body.Close()

	return nil
}

//PutBlockBlob TODO
func (p *AzUtil) PutBlockBlob(blobName string, body io.ReadSeeker, md5 []byte) error {
	bburl := p.containerURL.NewBlockBlobURL(blobName)

	h := azblob.BlobHTTPHeaders{}

	//16 is md5.Size
	if len(md5) != 16 {
		var md5bytes [16]byte
		copy(md5bytes[:], md5)
		h.ContentMD5 = md5bytes
	}

	ctx := context.Background()

	resp, err := bburl.PutBlob(ctx, body, h, azblob.Metadata{}, azblob.BlobAccessConditions{})

	if err != nil {
		return err
	}

	resp.Response().Body.Close()

	return nil
}

//CreatePageBlob TODO
func (p *AzUtil) CreatePageBlob(blobName string, size uint64) error {
	pburl := p.containerURL.NewPageBlobURL(blobName)
	h := azblob.BlobHTTPHeaders{}
	ctx := context.Background()

	resp, err := pburl.Create(ctx, int64(size), 0, azblob.Metadata{}, h, azblob.BlobAccessConditions{})

	if err != nil {
		return err
	}
	resp.Response().Body.Close()

	return nil
}

//PutPages TODO
func (p *AzUtil) PutPages(blobName string, start int32, end int32, body io.ReadSeeker) error {
	pburl := p.containerURL.NewPageBlobURL(blobName)
	pageRange := azblob.PageRange{
		Start: start,
		End:   end}
	ctx := context.Background()

	resp, err := pburl.PutPages(ctx, pageRange, body, azblob.BlobAccessConditions{})

	if err != nil {
		return err
	}
	resp.Response().Body.Close()

	return nil
}

//GetBlobURLWithReadOnlySASToken  TODO
func (p *AzUtil) GetBlobURLWithReadOnlySASToken(blobName string, expTime time.Time) url.URL {
	bu := p.containerURL.NewBlobURL(blobName)
	bp := azblob.NewBlobURLParts(bu.URL())

	sas := azblob.BlobSASSignatureValues{BlobName: blobName,
		ContainerName: bp.ContainerName,
		ExpiryTime:    expTime,
		Permissions:   "r"}

	sq := sas.NewSASQueryParameters(p.creds)
	bp.SAS = sq
	return bp.URL()
}

//BlobCallback TODO
type BlobCallback func(*azblob.Blob, string) (bool, error)

//IterateBlobList TODO
func (p *AzUtil) IterateBlobList(prefix string, callback BlobCallback) error {

	var marker azblob.Marker
	options := azblob.ListBlobsOptions{
		Details: azblob.BlobListingDetails{
			Metadata: true},
		Prefix: prefix}

	for {
		ctx := context.Background()
		response, err := p.containerURL.ListBlobs(ctx, marker, options)

		if err != nil {
			return err
		}
		exit := false
		for _, blob := range response.Blobs.Blob {
			exit, err = callback(&blob, prefix)
			if err != nil {
				return err
			}

			if exit {
				return nil
			}
		}

		if response.NextMarker.NotDone() {
			marker = response.NextMarker
			continue
		}

		break

	}
	return nil
}

func parseBaseURL(accountName string, baseURL string) (*url.URL, error) {
	var err error
	var url *url.URL

	if baseURL == "" {
		url, err = url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))

		if err != nil {
			return nil, err
		}

		return url, nil
	}

	if url, err = url.Parse(fmt.Sprintf("https://%s.%s", accountName, baseURL)); err != nil {
		return nil, err
	}

	return url, nil

}
