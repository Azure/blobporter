package sources

// BlobPorter Tool
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

// MultiFilePipeline  Contructs blocks queue and implements data readers
type MultiFilePipeline struct {
	FilesInfo           map[string]FileInfo
	TotalNumberOfBlocks int
	TotalSize           uint64
	BlockSize           uint64
}

//FileInfo Contains the metadata associated with a file to be transfered
type FileInfo struct {
	FileStats   *os.FileInfo
	SourceURI   string
	TargetAlias string
	NumOfBlocks int
}

// NewMultiFilePipeline creates a new MultiFilePipeline.
// If the sourcePattern results in a single file, the targetAlias, if set, will be used as the target name.
// Otherwise the full original file name will be used instead.
func NewMultiFilePipeline(sourcePattern string, blockSize uint64, targetAlias string) pipeline.SourcePipeline {
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
		var sName string

		if fileStat, err = os.Stat(file); err != nil {
			log.Fatalf("Error: %v", err)
		}

		numOfBlocks := util.GetNumberOfBlocks(uint64(fileStat.Size()), blockSize)
		totalSize = totalSize + uint64(fileStat.Size())
		totalNumberOfBlocks = totalNumberOfBlocks + numOfBlocks

		//use the param instead of the original filename only when  single file
		//transfer occurs.
		if targetAlias != "" && len(files) == 1 {
			sName = targetAlias
		} else {
			sName = fileStat.Name()
		}

		fileInfo := FileInfo{FileStats: &fileStat, SourceURI: file, TargetAlias: sName, NumOfBlocks: numOfBlocks}
		fileInfos[file] = fileInfo
	}

	return MultiFilePipeline{FilesInfo: fileInfos, TotalNumberOfBlocks: totalNumberOfBlocks, BlockSize: blockSize, TotalSize: totalSize}
}

//ExecuteReader implements ExecuteReader from the pipeline.SourcePipeline Interface.
//For each file the reader will maintain a open handle from which data will be read.
func (f MultiFilePipeline) ExecuteReader(partsQ *chan pipeline.Part, readPartsQ *chan pipeline.Part, id int, wg *sync.WaitGroup) {
	fileHandles := make(map[string]*os.File, len(f.FilesInfo))
	var err error
	var p pipeline.Part

	var ok bool
	var fileURI string
	var fileHandle *os.File

	for {
		p, ok = <-(*partsQ)

		if !ok {
			wg.Done()
			for _, fh := range fileHandles {
				fh.Close()
			}
			return // no more blocks of file data to be read
		}

		fileURI = f.FilesInfo[p.SourceURI].SourceURI

		fileHandle = fileHandles[fileURI]

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
			fmt.Printf("OKR|R|%v|%v|%v|%v\n", p.BlockID, len(p.Data), p.TargetAlias, p.BytesToRead)
		}

		*readPartsQ <- p
	}

}

//GetSourcesInfo implements GetSourcesInfo from the pipeline.SourcePipeline Interface.
//Returns a print friendly array of strings with the name and size of the files to be transfered.
func (f MultiFilePipeline) GetSourcesInfo() []string {
	// Single file support
	sources := make([]string, len(f.FilesInfo))

	var i = 0
	for _, file := range f.FilesInfo {
		sources[i] = fmt.Sprintf("File:%v, Size:%v", file.SourceURI, (*file.FileStats).Size())
		i++
	}

	return sources
}

//createPartsFromSource helper that creates an empty (no data) slice of parts from a source.
func createPartsFromSource(size uint64, sourceNumOfBlocks int, blockSize uint64, sourceURI string, targetAlias string, bufferQ *chan []byte) []pipeline.Part {
	var bytesLeft = size
	var curFileOffset uint64
	parts := make([]pipeline.Part, sourceNumOfBlocks)

	for i := 0; i < sourceNumOfBlocks; i++ {
		var partSize = blockSize
		if bytesLeft < blockSize { // last is a short block
			partSize = bytesLeft
		}

		fp := pipeline.NewPart(curFileOffset, uint32(partSize), i, sourceURI, targetAlias)

		fp.NumberOfBlocks = sourceNumOfBlocks
		fp.BufferQ = bufferQ
		fp.BlockSize = uint32(blockSize)

		parts[i] = *fp
		curFileOffset = curFileOffset + blockSize
		bytesLeft = bytesLeft - partSize
	}

	return parts

}

//ConstructBlockInfoQueue implements GetSourcesInfo from the pipeline.SourcePipeline Interface.
//Creates an channel of empty parts (i.e. no data) for all the files to be transfered.
func (f MultiFilePipeline) ConstructBlockInfoQueue(blockSize uint64) (partsQ *chan pipeline.Part, numOfBlocks int, size uint64) {
	Qcopy := make(chan pipeline.Part, f.TotalNumberOfBlocks)
	bufferQ := pipeline.NewBytesBufferChan(blockSize)
	numOfBlocks = f.TotalNumberOfBlocks
	size = f.TotalSize
	numOfFiles := len(f.FilesInfo)

	parts := make(map[string][]pipeline.Part, numOfFiles)
	for sourceName, source := range f.FilesInfo {
		parts[sourceName] = createPartsFromSource(uint64((*source.FileStats).Size()), source.NumOfBlocks, f.BlockSize, source.SourceURI, source.TargetAlias, bufferQ)
	}

	//Fill the Q with evenly ordered parts from the sources...
	//Since rage order in maps is not guranteed, get an array of the keys.
	keys := make([]string, len(f.FilesInfo))
	i := 0
	for k := range f.FilesInfo {
		keys[i] = k
		i++
	}
	for i := 0; i < f.TotalNumberOfBlocks; i++ {
		for _, sourceName := range keys {
			if i < len(parts[sourceName]) {
				Qcopy <- parts[sourceName][i]
			}
		}
	}

	close(Qcopy)
	partsQ = &Qcopy

	return

}
