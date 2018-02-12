package sources

import (
	"log"
	"os"
	"sync"

	"path/filepath"

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

//FileInfo Contains the metadata associated with a file to be transferred
type FileInfo struct {
	FileStats   *os.FileInfo
	SourceURI   string
	TargetAlias string
	NumOfBlocks int
}

//FileSystemSourceParams parameters used to create a new instance of multi-file source pipeline
type FileSystemSourceParams struct {
	SourcePatterns   []string
	BlockSize        uint64
	TargetAliases    []string
	NumOfPartitions  int
	MD5              bool
	FilesPerPipeline int
	KeepDirStructure bool
}

// NewFileSystemSourcePipeline creates a new MultiFilePipeline.
// If the sourcePattern results in a single file and the targetAlias is set, the alias will be used as the target name.
// Otherwise the full original file name will be used.
func NewFileSystemSourcePipeline(params *FileSystemSourceParams) []pipeline.SourcePipeline {
	var files []string
	var err error
	//get files from patterns
	for i := 0; i < len(params.SourcePatterns); i++ {
		var sourceFiles []string
		if sourceFiles, err = filepath.Glob(params.SourcePatterns[i]); err != nil {
			log.Fatal(err)
		}
		files = append(files, sourceFiles...)
	}

	if params.FilesPerPipeline <= 0 {
		log.Fatal(fmt.Errorf("Invalid operation. The number of files per batch must be greater than zero"))
	}

	if len(files) == 0 {
		log.Fatal(fmt.Errorf("The pattern(s) %v did not match any files", fmt.Sprint(params.SourcePatterns)))
	}

	numOfBatches := (len(files) + params.FilesPerPipeline - 1) / params.FilesPerPipeline
	pipelines := make([]pipeline.SourcePipeline, numOfBatches)
	numOfFilesInBatch := params.FilesPerPipeline
	filesSent := len(files)
	start := 0
	for b := 0; b < numOfBatches; b++ {
		var targetAlias []string

		if filesSent < numOfFilesInBatch {
			numOfFilesInBatch = filesSent
		}
		start = b * numOfFilesInBatch

		if len(params.TargetAliases) == len(files) {
			targetAlias = params.TargetAliases[start : start+numOfFilesInBatch]
		}
		pipelines[b] = newMultiFilePipeline(files[start:start+numOfFilesInBatch],
			targetAlias,
			params.BlockSize,
			params.NumOfPartitions,
			params.MD5,
			params.KeepDirStructure)
		filesSent = filesSent - numOfFilesInBatch
	}

	return pipelines
}

const maxNumOfHandlesPerFile int = 4

func newMultiFilePipeline(files []string, targetAliases []string, blockSize uint64, numOfPartitions int, md5 bool, keepDirStructure bool) pipeline.SourcePipeline {
	totalNumberOfBlocks := 0
	var totalSize uint64
	var err error
	fileInfos := make(map[string]FileInfo)
	useTargetAlias := len(targetAliases) == len(files)
	for f := 0; f < len(files); f++ {
		var fileStat os.FileInfo
		var sName string

		if fileStat, err = os.Stat(files[f]); err != nil {
			log.Fatalf("Error: %v", err)
		}

		//directories are not allowed... so skipping them
		if fileStat.IsDir() {
			continue
		}

		if fileStat.Size() == 0 {
			log.Fatalf("Empty files are not allowed. The file %v is empty", files[f])
		}
		numOfBlocks := int(uint64(fileStat.Size())+(blockSize-1)) / int(blockSize)
		totalSize = totalSize + uint64(fileStat.Size())
		totalNumberOfBlocks = totalNumberOfBlocks + numOfBlocks

		//use the param instead of the original filename only when
		//the number of targets matches the number files to transfer
		if useTargetAlias {
			sName = targetAliases[f]
		} else {
			sName = fileStat.Name()
			if keepDirStructure {
				sName = files[f]
			}

		}

		fileInfo := FileInfo{FileStats: &fileStat, SourceURI: files[f], TargetAlias: sName, NumOfBlocks: numOfBlocks}
		fileInfos[files[f]] = fileInfo
	}

	handlePool := internal.NewFileHandlePool(maxNumOfHandlesPerFile, internal.Read, false)

	return &FileSystemSource{filesInfo: fileInfos,
		totalNumberOfBlocks: totalNumberOfBlocks,
		blockSize:           blockSize,
		totalSize:           totalSize,
		numOfPartitions:     numOfPartitions,
		includeMD5:          md5,
		handlePool:          handlePool,
	}
}

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
		partitions := pipeline.ConstructPartsPartition(f.numOfPartitions, (*source.FileStats).Size(), int64(blockSize), source.SourceURI, source.TargetAlias, bufferQ)
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
