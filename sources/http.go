package sources

import (
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"fmt"
	"net/http"
	"net/url"

	"github.com/Azure/blobporter/internal"
	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/util"
)

////////////////////////////////////////////////////////////
///// HttpPipeline
////////////////////////////////////////////////////////////

const sasTokenNumberOfHours = 4

// HTTPSource  constructs parts  channel and implements data readers for file exposed via HTTP
type HTTPSource struct {
	Sources       []pipeline.SourceInfo
	HTTPClient    *http.Client
	includeMD5    bool
	referenceMode bool
}

//newHTTPSourcePipeline creates a new instance of an HTTP source
//To get the file size, a HTTP HEAD request is issued and the Content-Length header is inspected.
func newHTTPSourcePipeline(sourceURIs []string, targetAliases []string, md5 bool, referenceMode bool) pipeline.SourcePipeline {
	setTargetAlias := len(sourceURIs) == len(targetAliases)
	sources := make([]pipeline.SourceInfo, len(sourceURIs))
	for i := 0; i < len(sourceURIs); i++ {
		targetAlias := sourceURIs[i]
		if setTargetAlias {
			targetAlias = targetAliases[i]
		} else {
			var err error
			targetAlias, err = getFileNameFromURL(sourceURIs[i])

			if err != nil {
				log.Fatal(err)
			}
		}

		sources[i] = pipeline.SourceInfo{
			Size:        uint64(getSourceSize(sourceURIs[i])),
			TargetAlias: targetAlias,
			SourceName:  sourceURIs[i]}
	}
	return &HTTPSource{Sources: sources, HTTPClient: httpSourceHTTPClient, includeMD5: md5, referenceMode: referenceMode}
}

// returns last part of URL (filename)
func getFileNameFromURL(sourceURI string) (string, error) {

	purl, err := url.Parse(sourceURI)

	if err != nil {
		return "", err
	}

	parts := strings.Split(purl.Path, "/")

	if len(parts) == 0 {
		return "", fmt.Errorf("Invalid URL file was not found in the path")
	}

	return parts[len(parts)-1], nil
}

func getSourceSize(sourceURI string) (size int) {
	client := &http.Client{}
	resp, err := client.Head(sourceURI)

	if err != nil || resp.StatusCode != 200 {
		statusCode := ""
		if resp != nil {
			statusCode = fmt.Sprintf(" Status:%d ", resp.StatusCode)
		}
		err = fmt.Errorf("HEAD request failed. Please check the URL.%s Error: %v", statusCode, err)

		util.PrintfIfDebug("getSourceSize ->  err:%v", err)

		size = getSourceSizeFromByteRangeHeader(sourceURI)
		return
	}

	size, err = strconv.Atoi(resp.Header.Get("Content-Length"))

	if err != nil || size <= 0 {
		log.Fatalf("Content-Length is invalid. Expected a numeric value greater than zero. Error: %v", err)
	}

	return

}

func getSourceSizeFromByteRangeHeader(sourceURI string) (size int) {
	var req *http.Request
	var res *http.Response
	var err error
	client := &http.Client{}

	//Issue a fake request to see if can get the file size from the Content-Range header...
	if req, err = http.NewRequest("GET", sourceURI, nil); err != nil {
		log.Fatal(err)
	}
	header := fmt.Sprintf("bytes=%v-%v", 0, 10)
	req.Header.Set("Range", header)
	res, err = client.Get(sourceURI)
	if res, err = client.Do(req); err != nil || res.StatusCode != 206 {
		var status int
		if res != nil {
			status = res.StatusCode
			err = fmt.Errorf("Invalid status code in the response. Status: %v Bytes: %v", status, header)
		}
		log.Fatal(err)
	}
	crange := res.Header.Get("Content-Range")
	data := strings.Split(crange, "/")

	if len(data) != 2 {
		log.Fatalf("The Content-Range header does not contain the expected value. Value: %v", crange)
	}

	size, err = strconv.Atoi(data[1])

	if err != nil || size <= 0 {
		log.Fatalf("Content-Range is invalid. Expected a numeric value greater than zero. Value: %v", data[1])
	}

	return
}

//GetSourcesInfo implements GetSourcesInfo from the pipeline.SourcePipeline Interface.
//Returns an array of pipeline.SourceInfo[] with the files URL, alias and size.
func (f *HTTPSource) GetSourcesInfo() []pipeline.SourceInfo {
	return f.Sources
}

//ExecuteReader implements ExecuteReader from the pipeline.SourcePipeline Interface.
//For each part the reader makes a byte range request to the source
// starting from the part's Offset to BytesToRead - 1 (zero based).
func (f *HTTPSource) ExecuteReader(partitionsQ chan pipeline.PartsPartition, partsQ chan pipeline.Part, readPartsQ chan pipeline.Part, id int, wg *sync.WaitGroup) {
	var blocksHandled = 0
	var err error
	var req *http.Request
	var res *http.Response
	defer wg.Done()
	for {
		p, ok := <-partsQ

		if !ok {
			return // no more blocks of file data to be read
		}

		//when in reference mode the assumption is that data won't be read, we just need to pass
		// the reference (source info and byte range) to the workers. The target scenario is when the target
		// reads directly from the source, which is the case of the Put Block from URL functionally for block blobs.
		if f.referenceMode {
			readPartsQ <- p
			continue
		}

		if p.BytesToRead > 0 {
			util.RetriableOperation(func(r int) error {
				if req, err = http.NewRequest("GET", p.SourceURI, nil); err != nil {
					log.Fatal(err)
				}

				header := fmt.Sprintf("bytes=%v-%v", p.Offset, p.Offset-1+uint64(p.BytesToRead))
				req.Header.Set("Range", header)
				req.Header.Set("User-Agent", internal.UserAgentStr)

				if res, err = f.HTTPClient.Do(req); err != nil || res.StatusCode != 206 {
					var status int
					if res != nil {
						status = res.StatusCode
						err = fmt.Errorf("Invalid status code in the response. Status: %v Bytes: %v", status, header)
					}

					if res != nil && res.Body != nil {
						res.Body.Close()
					}

					util.PrintfIfDebug("ExecuteReader -> blockid:%v toread:%v status:%v err:%v head:%v", p.BlockID, p.BytesToRead, status, err, header)

					return err
				}

				//p.Data, err = ioutil.ReadAll(res.Body)
				p.GetBuffer()
				_, err := io.ReadAtLeast(res.Body, p.Data, int(p.BytesToRead))

				if err != nil && err != io.ErrUnexpectedEOF {
					return err
				}

				res.Body.Close()
				if err != nil {
					return err
				}

				if f.includeMD5 {
					p.MD5()
				}

				util.PrintfIfDebug("ExecuteReader -> blockid:%v toread:%v status:%v read:%v head:%v", p.BlockID, p.BytesToRead, res.StatusCode, res.ContentLength, header)

				return nil
			})
		}

		readPartsQ <- p
		blocksHandled++
	}

}

//ConstructBlockInfoQueue implements GetSourcesInfo from the pipeline.SourcePipeline Interface.
//Constructs the Part's channel arithmetically from the size of the sources.
func (f *HTTPSource) ConstructBlockInfoQueue(blockSize uint64) (partitionsQ chan pipeline.PartsPartition, partsQ chan pipeline.Part, numOfBlocks int, size uint64) {
	allParts := make([][]pipeline.Part, len(f.Sources))
	//disable memory buffer for parts (bufferQ == nil)
	//var bufferQ chan []byte
	bufferQ := make(chan []byte, 10)
	largestNumOfParts := 0
	for i, source := range f.Sources {
		size = size + source.Size
		parts, sourceNumOfBlocks := pipeline.ConstructPartsQueue(source.Size, blockSize, source.SourceName, source.TargetAlias, bufferQ)
		allParts[i] = parts
		numOfBlocks = numOfBlocks + sourceNumOfBlocks
		if largestNumOfParts < len(parts) {
			largestNumOfParts = len(parts)
		}
	}

	partsQ = make(chan pipeline.Part, numOfBlocks)

	for i := 0; i < largestNumOfParts; i++ {
		for _, ps := range allParts {
			if i < len(ps) {
				partsQ <- ps[i]
			}
		}
	}

	close(partsQ)

	return
}

const (
	maxIdleConns        = 50
	maxIdleConnsPerHost = 50
)

var httpSourceHTTPClient = newSourceHTTPClient()

func newSourceHTTPClient() *http.Client {
	return &http.Client{
		Timeout: time.Duration(internal.HTTPClientTimeout) * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   30 * time.Second, // dial timeout
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).Dial,
			MaxIdleConns:           maxIdleConns,
			MaxIdleConnsPerHost:    maxIdleConnsPerHost,
			IdleConnTimeout:        90 * time.Second,
			TLSHandshakeTimeout:    30 * time.Second,
			ExpectContinueTimeout:  1 * time.Second,
			DisableKeepAlives:      false,
			DisableCompression:     false,
			MaxResponseHeaderBytes: 0}}
}
