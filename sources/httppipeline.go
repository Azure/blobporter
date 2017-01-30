package sources

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

import (
	"io/ioutil"
	"log"
	"strconv"
	"sync"
	"time"

	"net/http"

	"fmt"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/util"
)

////////////////////////////////////////////////////////////
///// HttpPipeline
////////////////////////////////////////////////////////////

const sasTokenNumberOfHours = 4

// HTTPPipeline - Contructs blocks queue and implements data readers for file exposed via HTTP
type HTTPPipeline struct {
	SourceURI  string
	SourceSize uint64
	SourceName string
}

//NewHTTPAzureBlockPipeline TODO
func NewHTTPAzureBlockPipeline(container string, blobName string, accountName string, accountKey string) pipeline.SourcePipeline {
	var err error
	var sc storage.Client
	var bc storage.BlobStorageClient
	sc, err = storage.NewBasicClient(accountName, accountKey)

	if err != nil {
		log.Fatal(err)
	}

	bc = sc.GetBlobService()

	//Expiration in 4 hours
	date := time.Now().UTC().Add(time.Duration(sasTokenNumberOfHours) * time.Hour)

	var sasURL string
	if sasURL, err = bc.GetBlobSASURI(container, blobName, date, "r"); err != nil {
		log.Fatal(err)
	}

	return NewHTTPPipeline(sasURL, blobName)
}

// NewHTTPPipeline TODO
func NewHTTPPipeline(sourceURI string, sourceName string) pipeline.SourcePipeline {

	client := &http.Client{}

	resp, err := client.Head(sourceURI)

	if err != nil || resp.StatusCode != 200 {
		log.Fatalf("HEAD request failed. Please check the URL. Status:%d Error: %v", resp.StatusCode, err)
	}

	size, err := strconv.Atoi(resp.Header.Get("Content-Length"))

	if err != nil || size <= 0 {
		log.Fatalf("Content-Length is invalid. Expected a numeric value greater than zero. Error: %v", err)
	}

	return HTTPPipeline{SourceURI: sourceURI, SourceSize: uint64(size), SourceName: sourceName}
}

//GetSourcesInfo TODO
func (f HTTPPipeline) GetSourcesInfo() []string {
	// Single file support
	sources := make([]string, 1)
	sources[0] = fmt.Sprintf("URL:%v, Size:%v", f.SourceURI, f.SourceSize)

	return sources
}

//ExecuteReader TODO
func (f HTTPPipeline) ExecuteReader(partsQ *chan pipeline.Part, readPartsQ *chan pipeline.Part, id int, wg *sync.WaitGroup) {
	var blocksHandled = 0
	var err error
	var req *http.Request
	var res *http.Response
	client := &http.Client{Transport: &http.Transport{
		DisableCompression: true,
	},
	}
	for {
		p, ok := <-(*partsQ)

		if !ok {
			wg.Done()
			return // no more blocks of file data to be read
		}

		if req, err = http.NewRequest("GET", f.SourceURI, nil); err != nil {
			log.Fatal(err)
		}

		header := fmt.Sprintf("bytes=%v-%v", p.Offset, p.Offset-1+uint64(p.BytesToRead))

		req.Header.Set("Range", header)

		util.RetriableOperation(func(r int) error {
			if res, err = client.Do(req); err != nil || res.StatusCode != 206 {
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
			if util.Verbose {
				fmt.Printf("OK|R|%v|%v|%v|%v|%v\n", p.BlockID, p.BytesToRead, res.StatusCode, res.ContentLength, header)
			}
			return nil
		})

		p.GetBuffer()
		if p.Data, err = ioutil.ReadAll(res.Body); err != nil {
			log.Fatal(err)
		}
		res.Body.Close()

		*readPartsQ <- p
		blocksHandled++
	}

}

//ConstructBlockInfoQueue TODO
func (f HTTPPipeline) ConstructBlockInfoQueue(blockSize uint64) (partsQ *chan pipeline.Part, numOfBlocks int, size uint64) {
	size = f.SourceSize

	partsQ, numOfBlocks, _ = pipeline.ConstructPartsQueue(size, blockSize, f.SourceURI, f.SourceName)

	return
}
