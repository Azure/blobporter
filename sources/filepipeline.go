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
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/Azure/blobporter/pipeline"
)

////////////////////////////////////////////////////////////
///// FilePipeline
////////////////////////////////////////////////////////////

// FilePipeline - Contructs blocks queue and implements data readers
type FilePipeline struct {
	FileStats  *os.FileInfo
	SourceName string
	TargetName string
}

// NewFilePipeline TODO
func NewFilePipeline(sourceFileName string, targetName string) pipeline.SourcePipeline {

	var fileInfo os.FileInfo
	var err error

	if fileInfo, err = os.Stat(sourceFileName); err != nil {
		log.Fatal(err)
	}

	if targetName == "" {
		targetName = sourceFileName
	}

	return FilePipeline{FileStats: &fileInfo,
		SourceName: sourceFileName,
		TargetName: targetName}
}

//ExecuteReader TODO
func (f FilePipeline) ExecuteReader(partsQ *chan pipeline.Part, readPartsQ *chan pipeline.Part, id int, wg *sync.WaitGroup) {
	var blocksHandled = 0
	//var fileInfo os.FileInfo
	var fileHandle *os.File
	var err error

	for {
		p, ok := <-(*partsQ)

		if fileHandle == nil {
			if fileHandle, err = os.Open(f.SourceName); err != nil {
				log.Fatal(err)
			}
		}

		if !ok || p.BytesToRead == 0 {
			wg.Done()
			fileHandle.Close()
			return // no more blocks of file data to be read
		}

		//b := make([]byte, p.BytesToRead)
		p.GetBuffer()
		if _, err = fileHandle.ReadAt(p.Data, int64(p.Offset)); err != nil {
			log.Fatal(err)
		}

		*readPartsQ <- p
		blocksHandled++
	}

}

//GetSourcesInfo TODO
func (f FilePipeline) GetSourcesInfo() []string {
	// Single file support
	sources := make([]string, 1)
	sources[0] = fmt.Sprintf("File:%v, Size:%v", f.SourceName, (*f.FileStats).Size())

	return sources
}

//ConstructBlockInfoQueue creates
func (f FilePipeline) ConstructBlockInfoQueue(blockSize uint64) (partsQ *chan pipeline.Part, numOfBlocks int, size uint64) {

	size = uint64((*f.FileStats).Size())

	partsQ, numOfBlocks, _ = pipeline.ConstructPartsQueue(size, blockSize, f.SourceName, f.TargetName)

	return
}
