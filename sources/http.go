package sources

import (
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"

	"fmt"
	"net/http"

	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/util"
)

////////////////////////////////////////////////////////////
///// HttpPipeline
////////////////////////////////////////////////////////////

const sasTokenNumberOfHours = 4

// HTTPPipeline  constructs parts  channel and implements data readers for file exposed via HTTP
type HTTPPipeline struct {
	Sources    []pipeline.SourceInfo
	HTTPClient *http.Client
	includeMD5 bool
}

type sourceHTTPPipelineFactory func(httpSource HTTPPipeline) (pipeline.SourcePipeline, error)

func newHTTPSource(sourceListManager objectListManager, pipelineFactory sourceHTTPPipelineFactory, numOfFilePerPipeline int, includeMD5 bool) ([]pipeline.SourcePipeline, error) {
	var err error
	var sourceInfos []pipeline.SourceInfo

	if sourceInfos, err = sourceListManager.getSourceInfo(); err != nil {
		return nil, err
	}

	if numOfFilePerPipeline <= 0 {
		return nil, fmt.Errorf("Invalid operation. The number of files per batch must be greater than zero")
	}

	numOfBatches := (len(sourceInfos) + numOfFilePerPipeline - 1) / numOfFilePerPipeline
	pipelines := make([]pipeline.SourcePipeline, numOfBatches)
	numOfFilesInBatch := numOfFilePerPipeline
	filesSent := len(sourceInfos)
	start := 0

	for b := 0; b < numOfBatches; b++ {

		if filesSent < numOfFilesInBatch {
			numOfFilesInBatch = filesSent
		}
		start = b * numOfFilesInBatch

		httpSource := HTTPPipeline{Sources: sourceInfos[start : start+numOfFilesInBatch], HTTPClient: util.NewHTTPClient(), includeMD5: includeMD5}

		pipelines[b], err = pipelineFactory(httpSource)

		if err != nil {
			return nil, err
		}

		filesSent = filesSent - numOfFilesInBatch
	}

	return pipelines, err
}

//NewHTTP creates a new instance of an HTTP source
//To get the file size, a HTTP HEAD request is issued and the Content-Length header is inspected.
func NewHTTP(sourceURIs []string, targetAliases []string, md5 bool) pipeline.SourcePipeline {
	setTargetAlias := len(sourceURIs) == len(targetAliases)
	sources := make([]pipeline.SourceInfo, len(sourceURIs))
	for i := 0; i < len(sourceURIs); i++ {
		targetAlias := sourceURIs[i]
		if setTargetAlias {
			targetAlias = targetAliases[i]
		} else {
			var err error
			targetAlias, err = util.GetFileNameFromURL(sourceURIs[i])

			if err != nil {
				log.Fatal(err)
			}
		}

		sources[i] = pipeline.SourceInfo{
			Size:        uint64(getSourceSize(sourceURIs[i])),
			TargetAlias: targetAlias,
			SourceName:  sourceURIs[i]}
	}
	return &HTTPPipeline{Sources: sources, HTTPClient: util.NewHTTPClient(), includeMD5: md5}
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

		util.PrintfIfDebug("getSourceSize -> %v", err)

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
func (f *HTTPPipeline) GetSourcesInfo() []pipeline.SourceInfo {
	return f.Sources
}

//ExecuteReader implements ExecuteReader from the pipeline.SourcePipeline Interface.
//For each part the reader makes a byte range request to the source
// starting from the part's Offset to BytesToRead - 1 (zero based).
func (f *HTTPPipeline) ExecuteReader(partitionsQ chan pipeline.PartsPartition, partsQ chan pipeline.Part, readPartsQ chan pipeline.Part, id int, wg *sync.WaitGroup) {
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

		util.RetriableOperation(func(r int) error {
			if req, err = http.NewRequest("GET", p.SourceURI, nil); err != nil {
				log.Fatal(err)
			}

			header := fmt.Sprintf("bytes=%v-%v", p.Offset, p.Offset-1+uint64(p.BytesToRead))
			req.Header.Set("Range", header)

			//set the close header only when the block is larger than the blob
			//to minimize the number of open when transfering small files.
			if p.BytesToRead < p.BlockSize {
				req.Close = true
			}

			if res, err = f.HTTPClient.Do(req); err != nil || res.StatusCode != 206 {
				var status int
				if res != nil {
					status = res.StatusCode
					err = fmt.Errorf("Invalid status code in the response. Status: %v Bytes: %v", status, header)
				}

				if res != nil && res.Body != nil {
					res.Body.Close()
				}
				f.HTTPClient = util.NewHTTPClient()

				util.PrintfIfDebug("ExecuteReader -> |%v|%v|%v|%v|%v", p.BlockID, p.BytesToRead, status, err, header)

				return err
			}
			p.Data, err = ioutil.ReadAll(res.Body)
			//_, err = io.ReadFull(res.Body, p.Data[:p.BytesToRead])

			res.Body.Close()
			if err != nil {
				return err
			}

			if f.includeMD5 {
				p.MD5()
			}

			util.PrintfIfDebug("ExecuteReader -> |%v|%v|%v|%v|%v", p.BlockID, p.BytesToRead, res.StatusCode, res.ContentLength, header)

			return nil
		})

		readPartsQ <- p
		blocksHandled++
	}

}

//ConstructBlockInfoQueue implements GetSourcesInfo from the pipeline.SourcePipeline Interface.
//Constructs the Part's channel arithmetically from the size of the sources.
func (f *HTTPPipeline) ConstructBlockInfoQueue(blockSize uint64) (partitionsQ chan pipeline.PartsPartition, partsQ chan pipeline.Part, numOfBlocks int, size uint64) {
	allParts := make([][]pipeline.Part, len(f.Sources))
	//disable memory buffer for parts (bufferQ == nil)
	var bufferQ chan []byte
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
