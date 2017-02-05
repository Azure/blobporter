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
	"log"
	"os"
	"sync"

	"path/filepath"

	"fmt"

	"io"

	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/util"
)

////////////////////////////////////////////////////////////
///// MultiFilePipeline
////////////////////////////////////////////////////////////

// MultiFilePipeline - Contructs blocks queue and implements data readers
type MultiFilePipeline struct {
	FilesInfo           map[string]FileInfo
	TotalNumberOfBlocks int
	TotalSize           uint64
	BlockSize           uint64
}

//FileInfo TODO
type FileInfo struct {
	FileStats   *os.FileInfo
	SourceURI   string
	SourceName  string
	NumOfBlocks int
}

// NewMultiFilePipeline TODO
func NewMultiFilePipeline(sourcePattern string, blockSize uint64) pipeline.SourcePipeline {
	var files []string
	var err error
	//get files from pattern
	if files, err = filepath.Glob(sourcePattern); err != nil {
		log.Fatal(err)
	}
	if len(files) == 0 {
		log.Fatal(fmt.Errorf("The pattern %v did not match any files", sourcePattern))
	}
	totalNumberOfBlocks := 0
	var totalSize uint64
	fileInfos := make(map[string]FileInfo, len(files))
	for _, file := range files {
		var fileStat os.FileInfo
		if fileStat, err = os.Stat(file); err != nil {
			log.Fatalf("Error: %v", err)
		}

		numOfBlocks := util.GetNumberOfBlocks(uint64(fileStat.Size()), blockSize)
		totalSize = totalSize + uint64(fileStat.Size())
		totalNumberOfBlocks = totalNumberOfBlocks + numOfBlocks
		fileInfo := FileInfo{FileStats: &fileStat, SourceURI: file, SourceName: fileStat.Name(), NumOfBlocks: numOfBlocks}
		//fmt.Printf("Source:%v FileName:%v Size:%v NumOfBlocks:%v BlockSize:%v\n", file, fileStat.Name(), uint64(fileStat.Size()), numOfBlocks, blockSize)
		fileInfos[file] = fileInfo
	}

	return MultiFilePipeline{FilesInfo: fileInfos, TotalNumberOfBlocks: totalNumberOfBlocks, BlockSize: blockSize, TotalSize: totalSize}
}

//ExecuteReader TODO
func (f MultiFilePipeline) ExecuteReader(partsQ *chan pipeline.Part, readPartsQ *chan pipeline.Part, id int, wg *sync.WaitGroup) {
	var blocksHandled = 0
	fileHandles := make(map[string]*os.File, len(f.FilesInfo))
	var err error

	for {
		p, ok := <-(*partsQ)

		if !ok {
			wg.Done()
			for _, fh := range fileHandles {
				fh.Close()
			}
			return // no more blocks of file data to be read
		}

		fileURI := f.FilesInfo[p.SourceURI].SourceURI

		fileHandle := fileHandles[fileURI]

		if fileHandle == nil {
			if fileHandle, err = os.Open(fileURI); err != nil {
				fmt.Printf("Error while opening the file %v /n", err)
				log.Fatal(err)
			} else {
				fileHandles[fileURI] = fileHandle
			}
		}

		p.GetBuffer()
		if _, err = fileHandle.ReadAt(p.Data, int64(p.Offset)); err != nil && err != io.EOF {
			fmt.Printf("Error while reading the file %v /n", err)
			log.Fatal(err)
		}
		if util.Verbose {
			fmt.Printf("OKR|R|%v|%v|%v|%v\n", p.BlockID, len(p.Data), p.SourceName, p.BytesToRead)
		}

		*readPartsQ <- p
		blocksHandled++
	}

}

//GetSourcesInfo TODO
func (f MultiFilePipeline) GetSourcesInfo() []string {
	// Single file support
	sources := make([]string, len(f.FilesInfo))

	var i = 0
	for _, file := range f.FilesInfo {
		sources[i] = fmt.Sprintf("File:%v, Size:%v", file.SourceName, (*file.FileStats).Size())
		i++
	}

	return sources
}

func createPartsFromSource(size uint64, sourceNumOfBlocks int, blockSize uint64, sourceURI string, sourceName string, bufferQ *chan []byte) []pipeline.Part {
	var bytesLeft = size
	var curFileOffset uint64
	parts := make([]pipeline.Part, sourceNumOfBlocks)

	for i := 0; i < sourceNumOfBlocks; i++ {
		var partSize = blockSize
		if bytesLeft < blockSize { // last is a short block
			partSize = bytesLeft
		}

		fp := pipeline.NewPart(curFileOffset, uint32(partSize), i, sourceURI, sourceName)

		fp.NumberOfBlocks = sourceNumOfBlocks
		fp.BufferQ = bufferQ

		parts[i] = fp
		curFileOffset = curFileOffset + blockSize
		bytesLeft = bytesLeft - partSize
	}

	return parts

}

//ConstructBlockInfoQueue creates
func (f MultiFilePipeline) ConstructBlockInfoQueue(blockSize uint64) (partsQ *chan pipeline.Part, numOfBlocks int, size uint64) {
	Qcopy := make(chan pipeline.Part, f.TotalNumberOfBlocks)
	bufferQ := pipeline.NewBytesBufferChan(blockSize)
	numOfBlocks = f.TotalNumberOfBlocks
	size = f.TotalSize
	numOfFiles := len(f.FilesInfo)

	parts := make(map[string][]pipeline.Part, numOfFiles)
	for sourceName, source := range f.FilesInfo {
		parts[sourceName] = createPartsFromSource(uint64((*source.FileStats).Size()), source.NumOfBlocks, f.BlockSize, source.SourceURI, sourceName, bufferQ)
	}

	//Fill the Q with evenly ordered parts from the sources...
	for i := 0; i < f.TotalNumberOfBlocks; i++ {
		for sourceName := range f.FilesInfo {
			if i < len(parts[sourceName]) {
				Qcopy <- parts[sourceName][i]
			}
		}
	}

	close(Qcopy)
	partsQ = &Qcopy

	return

}
