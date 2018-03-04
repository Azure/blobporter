package sources

import (
	"log"
	"os"
	"sync"

	"fmt"

	"io"

	"github.com/Azure/blobporter/internal"
	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/util"
)

////////////////////////////////////////////////////////////
///// FileSystemSource
////////////////////////////////////////////////////////////

// FileSystemSource  Contructs blocks queue and implements data readers
type FileSystemSource struct {
	filesInfo           map[string]FileInfo
	totalNumberOfBlocks int
	totalSize           uint64
	blockSize           uint64
	numOfPartitions     int
	includeMD5          bool
	handlePool          *internal.FileHandlePool
}

//FileSystemSourceParams parameters used to create a new instance of multi-file source pipeline
type FileSystemSourceParams struct {
	SourceParams
	SourcePatterns  []string
	BlockSize       uint64
	TargetAliases   []string
	NumOfPartitions int
}

const maxNumOfHandlesPerFile int = 4

//ExecuteReader implements ExecuteReader from the pipeline.SourcePipeline Interface.
//For each file the reader will maintain a open handle from which data will be read.
// This implementation uses partitions (group of parts that can be read sequentially).
func (f *FileSystemSource) ExecuteReader(partitionsQ chan pipeline.PartsPartition, partsQ chan pipeline.Part, readPartsQ chan pipeline.Part, id int, wg *sync.WaitGroup) {
	var err error
	var partition pipeline.PartsPartition

	var ok bool
	var fileURI string
	var fileHandle *os.File
	var bytesRead int
	defer wg.Done()
	for {
		partition, ok = <-partitionsQ

		if !ok {
			for _, finfo := range f.filesInfo {
				err = f.handlePool.CloseHandles(finfo.SourceURI)
				if err != nil {
					log.Fatal(fmt.Errorf("error closing handle for file:%v. Error:%v", finfo.SourceURI, err))
				}
			}
			return // no more blocks of file data to be read
		}

		//check if the partition is empty, as this may happen with small files
		if len(partition.Parts) == 0 {
			continue
		}

		var part pipeline.Part
		for pip := 0; pip < len(partition.Parts); pip++ {
			part = partition.Parts[pip]

			fileURI = f.filesInfo[part.SourceURI].SourceURI

			if fileHandle == nil {
				if fileHandle, err = f.handlePool.GetHandle(fileURI); err != nil {
					log.Fatal(fmt.Errorf(" error while opening the file.\nError:%v ", err))
				}
			}

			if pip == 0 {
				fileHandle.Seek(partition.Offset, io.SeekStart)
			}

			part.GetBuffer()

			if _, err = fileHandle.Read(part.Data); err != nil && err != io.EOF {
				log.Fatal(fmt.Errorf(" error while reading the file.\nError:%v ", err))
			}

			util.PrintfIfDebug("ExecuteReader -> blockid:%v toread:%v name:%v read:%v ", part.BlockID, part.BytesToRead, part.TargetAlias, bytesRead)

			if f.includeMD5 {
				part.MD5()
			}

			readPartsQ <- part
		}

		//return handle
		if err = f.handlePool.ReturnHandle(fileURI, fileHandle); err != nil {
			log.Fatal(fmt.Errorf(" error returning the handle to the pool.\nPath: %v error:%v ", fileURI, err))
		}

		fileHandle = nil
	}
}

//GetSourcesInfo implements GetSourcesInfo from the pipeline.SourcePipeline Interface.
//Returns an an array of SourceInfo with the name, alias and size of the files to be transferred.
func (f *FileSystemSource) GetSourcesInfo() []pipeline.SourceInfo {

	sources := make([]pipeline.SourceInfo, len(f.filesInfo))
	var i = 0
	for _, file := range f.filesInfo {
		sources[i] = pipeline.SourceInfo{SourceName: file.SourceURI, TargetAlias: file.TargetAlias, Size: uint64(file.FileStats.Size())}
		i++
	}

	return sources
}

//createPartsFromSource helper that creates an empty (no data) slice of parts from a source.
func createPartsFromSource(size uint64, sourceNumOfBlocks int, blockSize uint64, sourceURI string, targetAlias string, bufferQ chan []byte) []pipeline.Part {
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

//ConstructBlockInfoQueue implements ConstructBlockInfoQueue from the pipeline.SourcePipeline Interface.
// this implementation uses partitions to group parts into a set that can be read sequentially.
// This is to avoid Window's memory pressure when calling SetFilePointer numerous times on the same handle
func (f *FileSystemSource) ConstructBlockInfoQueue(blockSize uint64) (partitionsQ chan pipeline.PartsPartition, partsQ chan pipeline.Part, numOfBlocks int, size uint64) {
	numOfBlocks = f.totalNumberOfBlocks
	size = f.totalSize
	allPartitions := make([][]pipeline.PartsPartition, len(f.filesInfo))
	//size of the queue is equal to the number of partitions times the number of files to transfer.
	//a lower value will block as this method is called before readers start
	partitionsQ = make(chan pipeline.PartsPartition, f.numOfPartitions*len(f.filesInfo))
	partsQ = nil
	bufferQ := pipeline.NewBytesBufferChan(uint64(blockSize))
	pindex := 0
	maxpartitionNumber := 0
	for _, source := range f.filesInfo {
		partitions := pipeline.ConstructPartsPartition(f.numOfPartitions, source.FileStats.Size(), int64(blockSize), source.SourceURI, source.TargetAlias, bufferQ)
		allPartitions[pindex] = partitions
		if len(partitions) > maxpartitionNumber {
			maxpartitionNumber = len(partitions)
		}
		pindex++
	}

	for fi := 0; fi < maxpartitionNumber; fi++ {
		for _, partition := range allPartitions {
			if len(partition) > fi {
				partitionsQ <- partition[fi]
			}
		}
	}

	close(partitionsQ)

	return

}
