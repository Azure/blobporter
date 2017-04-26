package sources

import (
	"io/ioutil"
	"log"
	"strconv"
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
	Sources    []SourceInfo
	HTTPClient *http.Client
}

//SourceInfo TODO
type SourceInfo struct {
	SourceURI   string
	SourceSize  uint64
	TargetAlias string
}

//NewHTTPAzureBlockPipeline creates a new instance of HTTPPipeline
//Creates a SASURI to the blobName with an expiration of sasTokenNumberOfHours.
//blobName is used as the target alias.
/*
func NewHTTPAzureBlockPipeline(container string, blobNames []string, accountName string, accountKey string) pipeline.SourcePipeline {
	var err error
	sourceURIs := make([]string, len(blobNames))

	bc := util.GetBlobStorageClient(accountName, accountKey)

	//Expiration in 4 hours
	date := time.Now().UTC().Add(time.Duration(sasTokenNumberOfHours) * time.Hour)

	for i := 0; i < len(sourceURIs); i++ {
		var sasURL string
		if sasURL, err = bc.GetBlobSASURI(container, blobNames[i], date, "r"); err != nil {
			log.Fatal(err)
		}
		sourceURIs[i] = sasURL
	}

	return NewHTTPPipeline(sourceURIs, nil)
}
*/

//NewHTTP creates a new instance of an HTTP source
//To get the file size, a HTTP HEAD request is issued and the Content-Length header is inspected.
func NewHTTP(sourceURIs []string, targetAliases []string) pipeline.SourcePipeline {
	setTargetAlias := len(sourceURIs) == len(targetAliases)
	sources := make([]SourceInfo, len(sourceURIs))
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

		sources[i] = SourceInfo{
			SourceSize:  uint64(getSourceSize(sourceURIs[i])),
			TargetAlias: targetAlias,
			SourceURI:   sourceURIs[i]}
	}
	return HTTPPipeline{Sources: sources, HTTPClient: util.NewHTTPClient()}
}

func getSourceSize(sourceURI string) (size int) {
	client := &http.Client{}
	resp, err := client.Head(sourceURI)

	if err != nil || resp.StatusCode != 200 {
		log.Fatalf("HEAD request failed. Please check the URL. Status:%d Error: %v", resp.StatusCode, err)
	}

	size, err = strconv.Atoi(resp.Header.Get("Content-Length"))

	if err != nil || size <= 0 {
		log.Fatalf("Content-Length is invalid. Expected a numeric value greater than zero. Error: %v", err)
	}

	return
}

//GetSourcesInfo implements GetSourcesInfo from the pipeline.SourcePipeline Interface.
//Returns an array of pipeline.SourceInfo[] with the files URL, alias and size.
func (f HTTPPipeline) GetSourcesInfo() []pipeline.SourceInfo {
	sources := make([]pipeline.SourceInfo, len(f.Sources))

	for i := 0; i < len(f.Sources); i++ {
		sources[i] = pipeline.SourceInfo{SourceName: f.Sources[i].SourceURI, TargetAlias: f.Sources[i].TargetAlias, Size: f.Sources[i].SourceSize}
	}

	return sources
}

//ExecuteReader implements ExecuteReader from the pipeline.SourcePipeline Interface.
//For each part the reader makes a byte range request to the source
// starting from the part's Offset to BytesToRead - 1 (zero based).
func (f HTTPPipeline) ExecuteReader(partitionsQ chan pipeline.PartsPartition, partsQ chan pipeline.Part, readPartsQ chan pipeline.Part, id int, wg *sync.WaitGroup) {
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

		if req, err = http.NewRequest("GET", p.SourceURI, nil); err != nil {
			log.Fatal(err)
		}

		header := fmt.Sprintf("bytes=%v-%v", p.Offset, p.Offset-1+uint64(p.BytesToRead))
		req.Header.Set("Range", header)

		util.RetriableOperation(func(r int) error {
			if res, err = f.HTTPClient.Do(req); err != nil || res.StatusCode != 206 {
				var status int
				if res != nil {
					status = res.StatusCode
					err = fmt.Errorf("Invalid status code in the response. Status: %v Bytes: %v", status, header)
				}

				if util.Verbose {
					fmt.Printf("EH|R|%v|%v|%v|%v|%v\n", p.BlockID, p.BytesToRead, status, err, header)
				}
				return err
			}

			p.Data, err = ioutil.ReadAll(res.Body)
			res.Body.Close()
			if err != nil {
				return err
			}

			if util.Verbose {
				fmt.Printf("OK|R|%v|%v|%v|%v|%v\n", p.BlockID, p.BytesToRead, res.StatusCode, res.ContentLength, header)
			}
			return nil
		})

		readPartsQ <- p
		blocksHandled++
	}

}

//ConstructBlockInfoQueue implements GetSourcesInfo from the pipeline.SourcePipeline Interface.
//Constructs the Part's channel arithmetically from the size of the sources.
func (f HTTPPipeline) ConstructBlockInfoQueue(blockSize uint64) (partitionsQ chan pipeline.PartsPartition, partsQ chan pipeline.Part, numOfBlocks int, size uint64) {
	allParts := make([][]pipeline.Part, len(f.Sources))
	//disable memory buffer for parts (bufferQ == nil)
	var bufferQ chan []byte
	largestNumOfParts := 0
	for i, source := range f.Sources {
		size = size + source.SourceSize
		parts, sourceNumOfBlocks := pipeline.ConstructPartsQueue(source.SourceSize, blockSize, source.SourceURI, source.TargetAlias, bufferQ)
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
