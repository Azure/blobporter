package sources

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
	NumOfPartitions     int
}

//FileInfo Contains the metadata associated with a file to be transferred
type FileInfo struct {
	FileStats   *os.FileInfo
	SourceURI   string
	TargetAlias string
	NumOfBlocks int
}

// NewMultiFile creates a new MultiFilePipeline.
// If the sourcePattern results in a single file, the targetAlias, if set, will be used as the target name.
// Otherwise the full original file name will be used instead.
func NewMultiFile(sourcePatterns []string, blockSize uint64, targetAliases []string, numOfPartitions int) pipeline.SourcePipeline {
	var files []string
	var err error
	//get files from patterns
	for i := 0; i < len(sourcePatterns); i++ {
		var sourceFiles []string
		if sourceFiles, err = filepath.Glob(sourcePatterns[i]); err != nil {
			log.Fatal(err)
		}
		files = append(files, sourceFiles...)
	}

	if len(files) == 0 {
		log.Fatal(fmt.Errorf("The pattern(s) %v did not match any files", fmt.Sprint(sourcePatterns)))
	}
	totalNumberOfBlocks := 0
	var totalSize uint64
	fileInfos := make(map[string]FileInfo, len(files))
	useTargetAlias := len(targetAliases) == len(files)
	for f := 0; f < len(files); f++ {
		var fileStat os.FileInfo
		var sName string

		if fileStat, err = os.Stat(files[f]); err != nil {
			log.Fatalf("Error: %v", err)
		}

		if fileStat.Size() == 0 {
			log.Fatalf("Empty files are not allowed. The file %v is empty", files[f])
		}

		numOfBlocks := util.GetNumberOfBlocks(uint64(fileStat.Size()), blockSize)
		totalSize = totalSize + uint64(fileStat.Size())
		totalNumberOfBlocks = totalNumberOfBlocks + numOfBlocks

		//use the param instead of the original filename only when  single file
		//transfer occurs.
		if useTargetAlias {
			sName = targetAliases[f]
		} else {
			sName = fileStat.Name()
		}

		fileInfo := FileInfo{FileStats: &fileStat, SourceURI: files[f], TargetAlias: sName, NumOfBlocks: numOfBlocks}
		fileInfos[files[f]] = fileInfo
	}

	return MultiFilePipeline{FilesInfo: fileInfos, TotalNumberOfBlocks: totalNumberOfBlocks, BlockSize: blockSize, TotalSize: totalSize, NumOfPartitions: numOfPartitions}
}

//ExecuteReader implements ExecuteReader from the pipeline.SourcePipeline Interface.
//For each file the reader will maintain a open handle from which data will be read.
// This implementation uses partitions (group of parts that can be read sequentially).
func (f MultiFilePipeline) ExecuteReader(partitionsQ chan pipeline.PartsPartition, partsQ chan pipeline.Part, readPartsQ chan pipeline.Part, id int, wg *sync.WaitGroup) {
	fileHandles := make(map[string]*os.File, len(f.FilesInfo))
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
			for _, fh := range fileHandles {
				fh.Close()
			}
			return // no more blocks of file data to be read
		}

		var part pipeline.Part
		for pip := 0; pip < len(partition.Parts); pip++ {
			part = partition.Parts[pip]

			fileURI = f.FilesInfo[part.SourceURI].SourceURI
			fileHandle = fileHandles[fileURI]

			if fileHandle == nil {
				if fileHandle, err = os.Open(fileURI); err != nil {
					fmt.Printf("Error while opening the file %v /n", err)
					log.Fatal(err)
				}
				fileHandles[fileURI] = fileHandle
			}

			if pip == 0 {

				fileHandle.Seek(partition.Offset, io.SeekStart)
			}
			part.GetBuffer()

			if _, err = fileHandle.Read(part.Data); err != nil && err != io.EOF {
				fmt.Printf("Error while reading the file %v /n", err)
				log.Fatal(err)
			}

			if util.Verbose {
				fmt.Printf("OKR|R|%v|%v|%v|%v/n", part.BlockID, bytesRead, part.TargetAlias, part.BytesToRead)
			}
			readPartsQ <- part
		}
	}
}

//GetSourcesInfo implements GetSourcesInfo from the pipeline.SourcePipeline Interface.
//Returns an an array of SourceInfo with the name, alias and size of the files to be transferred.
func (f MultiFilePipeline) GetSourcesInfo() []pipeline.SourceInfo {

	sources := make([]pipeline.SourceInfo, len(f.FilesInfo))
	var i = 0
	for _, file := range f.FilesInfo {
		sources[i] = pipeline.SourceInfo{SourceName: file.SourceURI, TargetAlias: file.TargetAlias, Size: uint64((*file.FileStats).Size())}
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
func (f MultiFilePipeline) ConstructBlockInfoQueue(blockSize uint64) (partitionsQ chan pipeline.PartsPartition, partsQ chan pipeline.Part, numOfBlocks int, size uint64) {
	numOfBlocks = f.TotalNumberOfBlocks
	size = f.TotalSize
	allPartitions := make([][]pipeline.PartsPartition, len(f.FilesInfo))
	//size of the queue is equal to the number of partitions times the number of files to transfer.
	//a lower value will block as this method is called before readers start
	partitionsQ = make(chan pipeline.PartsPartition, f.NumOfPartitions*len(f.FilesInfo))
	partsQ = nil
	bufferQ := pipeline.NewBytesBufferChan(uint64(blockSize))
	pindex := 0
	maxpartitionNumber := 0
	for _, source := range f.FilesInfo {
		partitions := pipeline.ConstructPartsPartition(f.NumOfPartitions, (*source.FileStats).Size(), int64(blockSize), source.SourceURI, source.TargetAlias, bufferQ)
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
