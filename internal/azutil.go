package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"syscall"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/blobporter/util"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

//AzUtil TODO
type AzUtil struct {
	serviceURL   *azblob.ServiceURL
	containerURL *azblob.ContainerURL
	creds        *azblob.SharedKeyCredential
	pipeline     pipeline.Pipeline
}

//NewAzUtil TODO
func NewAzUtil(accountName string, accountKey string, container string, baseBlobURL string) (*AzUtil, error) {

	creds, err := azblob.NewSharedKeyCredential(accountName, accountKey)

	if err != nil {
		return nil, err
	}

	pipeline := newPipeline(creds, azblob.PipelineOptions{
		Telemetry: azblob.TelemetryOptions{
			Value: fmt.Sprintf("%s/%s", UserAgentStr, azblob.UserAgent())},
		Retry: azblob.RetryOptions{
			Policy:        azblob.RetryPolicyFixed,
			TryTimeout:    time.Duration(HTTPClientTimeout) * time.Second,
			MaxTries:      500,
			RetryDelay:    100 * time.Millisecond,
			MaxRetryDelay: 2 * time.Second}})

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

//DeleteContainerIfNotExists TODO
func (p *AzUtil) DeleteContainerIfNotExists() error {
	ctx := context.Background()
	_, err := p.containerURL.Delete(ctx, azblob.ContainerAccessConditions{})

	return err
}

//CreateContainerIfNotExists returs true if the container did not exist.
func (p *AzUtil) CreateContainerIfNotExists() (bool, error) {
	ctx := context.Background()
	response, err := p.containerURL.GetProperties(ctx, azblob.LeaseAccessConditions{})

	if response != nil {
		defer response.Response().Body.Close()
		if response.StatusCode() == 200 {
			return false, nil
		}
	}

	if err != nil {
		if storageErr, okay := err.(azblob.StorageError); okay {
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
	}

	return false, err
}

func (p *AzUtil) blobExists(bburl azblob.BlockBlobURL) (bool, error) {
	ctx := context.Background()
	response, err := bburl.GetProperties(ctx, azblob.BlobAccessConditions{})

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

	var resp *azblob.BlockBlobUploadResponse
	ctx = context.Background()

	resp, err = bburl.Upload(ctx, bytes.NewReader(empty), azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{})
	defer resp.Response().Body.Close()

	return err
}

//PutBlockList TODO
func (p *AzUtil) PutBlockList(blobName string, blockIDs []string) error {
	bburl := p.containerURL.NewBlockBlobURL(blobName)

	h := azblob.BlobHTTPHeaders{}
	ctx := context.Background()
	resp, err := bburl.CommitBlockList(ctx, blockIDs, h, azblob.Metadata{}, azblob.BlobAccessConditions{})

	if err != nil {
		return err
	}
	resp.Response().Body.Close()

	return nil
}

//PutEmptyBlockBlob  TODO
func (p *AzUtil) PutEmptyBlockBlob(blobName string) error {
	empty := make([]string, 0)
	return p.PutBlockList(blobName, empty)
}

//PutBlock TODO
func (p *AzUtil) PutBlock(container string, blobName string, id string, body io.ReadSeeker, md5 []byte) error {
	curl := p.serviceURL.NewContainerURL(container)
	bburl := curl.NewBlockBlobURL(blobName)

	ctx := context.Background()
	resp, err := bburl.StageBlock(ctx, id, body, azblob.LeaseAccessConditions{}, md5)

	if err != nil {
		return err
	}
	resp.Response().Body.Close()

	return nil
}

//PutBlockBlobFromURL TODO
func (p *AzUtil) PutBlockBlobFromURL(blobName string, id string, sourceURL string, offset int64, length int64) error {
	bburl := p.containerURL.NewBlockBlobURL(blobName)

	ctx := context.Background()
	surl, err := url.ParseRequestURI(sourceURL)
	if err != nil {
		return err
	}
	resp, err := bburl.StageBlockFromURL(ctx, id, *surl, offset, length, azblob.LeaseAccessConditions{})

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
	h.ContentMD5 = md5

	ctx := context.Background()

	resp, err := bburl.Upload(ctx, body, h, azblob.Metadata{}, azblob.BlobAccessConditions{})

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

	resp, err := pburl.Create(ctx, int64(size), 0, h, azblob.Metadata{}, azblob.BlobAccessConditions{})

	if err != nil {
		return err
	}
	resp.Response().Body.Close()

	return nil
}

//PutPages TODO
func (p *AzUtil) PutPages(blobName string, start int64, end int64, body io.ReadSeeker, md5 []byte) error {
	pburl := p.containerURL.NewPageBlobURL(blobName)

	ctx := context.Background()

	resp, err := pburl.UploadPages(ctx, start, body, azblob.PageBlobAccessConditions{}, md5)

	if err != nil {
		return err
	}
	resp.Response().Body.Close()

	return nil
}

//GetBlobURLWithReadOnlySASToken  TODO
func (p *AzUtil) GetBlobURLWithReadOnlySASToken(blobName string, expTime time.Time) (*url.URL, error) {
	bu := p.containerURL.NewBlobURL(blobName)
	bp := azblob.NewBlobURLParts(bu.URL())

	sas := azblob.BlobSASSignatureValues{BlobName: blobName,
		ContainerName: bp.ContainerName,
		ExpiryTime:    expTime,
		Permissions:   "r"}

	sq, err := sas.NewSASQueryParameters(p.creds)
	if err != nil {
		return nil, err
	}
	bp.SAS = sq
	burl := bp.URL()
	return &burl, nil
}

//BlobCallback TODO
type BlobCallback func(*azblob.BlobItem, string) (bool, error)

//IterateBlobList TODO
func (p *AzUtil) IterateBlobList(prefix string, callback BlobCallback) error {

	var marker azblob.Marker
	options := azblob.ListBlobsSegmentOptions{
		Details: azblob.BlobListingDetails{
			Metadata: true},
		Prefix: prefix}

	for {
		ctx := context.Background()
		response, err := p.containerURL.ListBlobsFlatSegment(ctx, marker, options)

		if err != nil {
			return err
		}
		exit := false
		for _, blob := range response.Segment.BlobItems {
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

func newPipeline(c azblob.Credential, o azblob.PipelineOptions) pipeline.Pipeline {
	if c == nil {
		panic("c can't be nil")
	}

	// Closest to API goes first; closest to the wire goes last
	f := []pipeline.Factory{
		azblob.NewTelemetryPolicyFactory(o.Telemetry),
		azblob.NewUniqueRequestIDPolicyFactory(),
		azblob.NewRetryPolicyFactory(o.Retry),
		c,
	}

	f = append(f,
		pipeline.MethodFactoryMarker(), // indicates at what stage in the pipeline the method factory is invoked
		azblob.NewRequestLogPolicyFactory(o.RequestLog))

	return pipeline.NewPipeline(f, pipeline.Options{HTTPSender: newHTTPClientFactory(), Log: o.Log})
}

func newHTTPClientFactory() pipeline.Factory {
	return &clientPolicyFactory{}
}

type clientPolicyFactory struct {
}

// Create initializes a logging policy object.
func (f *clientPolicyFactory) New(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.Policy {
	return &clientPolicy{po: po}
}

type clientPolicy struct {
	po *pipeline.PolicyOptions
}

const winWSAETIMEDOUT syscall.Errno = 10060

// checks if the underlying error is a connectex error and if the underying cause is winsock timeout or temporary error, in which case we should retry.
func isWinsockTimeOutError(err error) net.Error {
	if uerr, ok := err.(*url.Error); ok {
		if derr, ok := uerr.Err.(*net.OpError); ok {
			if serr, ok := derr.Err.(*os.SyscallError); ok && serr.Syscall == "connectex" {
				if winerr, ok := serr.Err.(syscall.Errno); ok && (winerr == winWSAETIMEDOUT || winerr.Temporary()) {
					return &retriableError{error: err}
				}
			}
		}
	}
	return nil
}

func isDialConnectError(err error) net.Error {
	if uerr, ok := err.(*url.Error); ok {
		if derr, ok := uerr.Err.(*net.OpError); ok {
			if serr, ok := derr.Err.(*os.SyscallError); ok && serr.Syscall == "connect" {
				return &retriableError{error: err}
			}
		}
	}
	return nil
}

func isRetriableDialError(err error) net.Error {
	if derr := isWinsockTimeOutError(err); derr != nil {
		return derr
	}
	return isDialConnectError(err)
}

type retriableError struct {
	error
}

func (*retriableError) Timeout() bool {
	return false
}

func (*retriableError) Temporary() bool {
	return true
}

const tcpKeepOpenMinLength = 8 * int64(util.MB)

func (p *clientPolicy) Do(ctx context.Context, request pipeline.Request) (pipeline.Response, error) {
	req := request.WithContext(ctx)

	if req.ContentLength < tcpKeepOpenMinLength {
		req.Close = true
	}

	r, err := pipelineHTTPClient.Do(req)
	pipresp := pipeline.NewHTTPResponse(r)
	if err != nil {
		if derr := isRetriableDialError(err); derr != nil {
			return pipresp, derr
		}
		err = pipeline.NewError(err, "HTTP request failed")
	}
	return pipresp, err
}

var pipelineHTTPClient = newpipelineHTTPClient()

func newpipelineHTTPClient() *http.Client {

	return &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).Dial,
			MaxIdleConns:           100,
			MaxIdleConnsPerHost:    100,
			IdleConnTimeout:        60 * time.Second,
			TLSHandshakeTimeout:    10 * time.Second,
			ExpectContinueTimeout:  1 * time.Second,
			DisableKeepAlives:      false,
			DisableCompression:     false,
			MaxResponseHeaderBytes: 0}}

}

//Custom header policy implementation. This policy allows the
// injection of custom http headers in the requests via a value contained in the context
// this approach is primarily a workaround because of feature gaps in the storage sdk

func newCustomHeadersPolicyFactory() pipeline.Factory {
	return &customHeadersPolicyFactory{}
}

type customHeadersPolicyFactory struct {
}

func (c *customHeadersPolicyFactory) New(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.Policy {
	return &customHeaderPolicy{next: next, po: po}
}

type customHeaderPolicy struct {
	po   *pipeline.PolicyOptions
	next pipeline.Policy
}
type key int

const customHeaderKey key = 0

func (p *customHeaderPolicy) Do(ctx context.Context, request pipeline.Request) (pipeline.Response, error) {
	customHeaders, ok := ctx.Value(customHeaderKey).(map[string]string)
	if ok && customHeaders != nil {
		for header, value := range customHeaders {
			request.Header.Del(header)
			request.Header.Add(header, value)
		}
	}

	return p.next.Do(ctx, request)
}
