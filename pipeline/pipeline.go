package pipeline

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"log"
	"sync"
	"time"

	"math"

	"bytes"

	"github.com/Azure/blobporter/util"
)

//NewBytesBufferChan creates a channel with 'n' slices of []bytes.
//'n' is the bufferQCapacity. If the bufferQCapacity times bufferSize is greater than 1 GB
// 'n' is limited to a value that meets the constraint.
func NewBytesBufferChan(bufferSize uint64) chan []byte {

	bufferQCapacity := util.GB / bufferSize

	c := make(chan []byte, bufferQCapacity)
	var index uint64
	//only preallocated a quarter of the capacity.

	for index = 0; index < uint64(math.Ceil(float64(bufferQCapacity)*.25)); index++ {
		c <- make([]byte, bufferSize)
	}
	return c
}

//SourcePipeline operations that abstract the creation of the empty and read parts channels.
type SourcePipeline interface {
	ConstructBlockInfoQueue(blockSize uint64) (partitionQ chan PartsPartition, partsQ chan Part, numOfBlocks int, Size uint64)
	ExecuteReader(partitionQ chan PartsPartition, partsQ chan Part, readPartsQ chan Part, id int, wg *sync.WaitGroup)
	GetSourcesInfo() []SourceInfo
}

//SourceInfo TODO
type SourceInfo struct {
	SourceName  string
	Size        uint64
	TargetAlias string
}

//TargetPipeline operations that abstract how parts a written and processed to a given target
type TargetPipeline interface {
	PreProcessSourceInfo(source *SourceInfo) (err error)
	CommitList(listInfo *TargetCommittedListInfo, numberOfBlocks int, targetName string) (msg string, err error)
	WritePart(part *Part) (duration time.Duration, startTime time.Time, numOfRetries int, err error)
	ProcessWrittenPart(result *WorkerResult, listInfo *TargetCommittedListInfo) (requeue bool, err error)
}

// WorkerResult represents the result of a single block upload
type WorkerResult struct {
	BlockSize               int
	Result                  string
	WorkerID                int
	ItemID                  string
	DuplicateOfBlockOrdinal int
	Ordinal                 int
	Offset                  uint64
	SourceURI               string
	NumberOfBlocks          int
	TargetName              string
	Stats                   *WorkerResultStats
}

//WorkerResultStats stats at the worker level.
type WorkerResultStats struct {
	Duration         time.Duration
	StartTime        time.Time
	Retries          int
	CumWriteDuration time.Duration
	NumOfWrites      int64
}

//TargetCommittedListInfo contains a list parts that have been written to a target.
type TargetCommittedListInfo struct {
	List interface{}
}

// MD5ToBlockID simple lookup table mapping an MD5 string to a blockID
var MD5ToBlockID = make(map[string]int)

// MD5ToBlockIDLock a lock for the map
var MD5ToBlockIDLock sync.RWMutex

// Part description of and data for a block of the source
type Part struct {
	Offset                  uint64
	BlockSize               uint32
	BytesToRead             uint32
	Data                    []byte // The data for the block.  Can be nil if not yet read from the source
	BlockID                 string
	DuplicateOfBlockOrdinal int    // -1 if not a duplicate of another, already read, block.
	Ordinal                 int    // sequentially assigned at creation time to enable chunk ordering (0,1,2)
	md5Value                string // internal copy of computed MD5, initially empty string
	SourceURI               string
	TargetAlias             string
	NumberOfBlocks          int
	BufferQ                 chan []byte
}

// StorageAccountCredentials a central location for account info.
type StorageAccountCredentials struct {
	AccountName string // short name of the storage account.  e.g., mystore
	AccountKey  string // Base64-encoded storage account key
}

//PartsPartition represents a set of parts that can be read sequentially
//starting at the partition's Offset
type PartsPartition struct {
	Offset          int64
	NumOfParts      int
	TotalNumOfParts int64
	TotalSize       int64
	PartitionSize   int64
	Parts           []Part
}

//createPartsInPartition creates the parts in the partition arithmetically.
func createPartsInPartition(partitionSize int64, partitionOffSet int64, ordinalStart int, sourceNumOfBlocks int, blockSize int64, sourceURI string, targetAlias string, bufferQ chan []byte) (parts []Part, ordinal int, numOfPartsInPartition int) {
	var bytesLeft = partitionSize
	curFileOffset := partitionOffSet
	numOfPartsInPartition = int((partitionSize + blockSize - 1) / blockSize)
	parts = make([]Part, numOfPartsInPartition)
	ordinal = ordinalStart
	for i := 0; i < numOfPartsInPartition; i++ {
		var partSize = blockSize
		if bytesLeft < blockSize { // last is a short block
			partSize = bytesLeft
		}
		fp := NewPart(uint64(curFileOffset), uint32(partSize), ordinal, sourceURI, targetAlias)

		fp.NumberOfBlocks = sourceNumOfBlocks
		fp.BufferQ = bufferQ
		fp.BlockSize = uint32(blockSize)

		parts[i] = *fp
		curFileOffset = curFileOffset + blockSize
		bytesLeft = bytesLeft - partSize
		ordinal++
	}

	return
}

//ConstructPartsPartition creates a slice of PartsPartition with a len of numberOfPartitions.
func ConstructPartsPartition(numberOfPartitions int, size int64, blockSize int64, sourceURI string, targetAlias string, bufferQ chan []byte) []PartsPartition {
	//bsib := uint64(blockSize)
	numOfBlocks := int((size + blockSize - 1) / blockSize)

	if numOfBlocks > util.MaxBlockCount { // more than 50,000 blocks needed, so can't work
		var minBlkSize = (size + util.MaxBlockCount - 1) / util.MaxBlockCount
		log.Fatalf("Block size is too small, minimum block size for this file would be %d bytes", minBlkSize)
	}

	Partitions := make([]PartsPartition, numberOfPartitions)
	//the size of the partition needs to be a multiple (blockSize * int) to make sure all but the last part/block
	//are the same size
	partitionSize := (int64(size) / int64(numberOfPartitions) / blockSize) * blockSize

	var bytesLeft = size
	var parts []Part
	var numOfPartsInPartition int
	var partOrdinal int
	for p := 0; p < numberOfPartitions; p++ {
		poffSet := int64(int64(p) * partitionSize)
		if p == numberOfPartitions-1 {
			partitionSize = int64(bytesLeft)
		}
		partition := PartsPartition{TotalNumOfParts: int64(numOfBlocks), TotalSize: size, Offset: poffSet, PartitionSize: partitionSize}
		parts, partOrdinal, numOfPartsInPartition = createPartsInPartition(partitionSize, poffSet, partOrdinal, numOfBlocks, blockSize, sourceURI, targetAlias, bufferQ)

		partition.Parts = parts
		partition.NumOfParts = numOfPartsInPartition
		Partitions[p] = partition

		bytesLeft = bytesLeft - int64(partitionSize)
	}

	return Partitions
}

//ConstructPartsQueue  constructs a slice of parts calculated arithmetically from blockSize and size.
func ConstructPartsQueue(size uint64, blockSize uint64, sourceURI string, targetAlias string, bufferQ chan []byte) (parts []Part, numOfBlocks int) {
	var bsib = blockSize
	numOfBlocks = int((size + (bsib - 1)) / bsib)

	if numOfBlocks > util.MaxBlockCount { // more than 50,000 blocks needed, so can't work
		var minBlkSize = (size + util.MaxBlockCount - 1) / util.MaxBlockCount
		log.Fatalf("Block size is too small, minimum block size for this file would be %d bytes", minBlkSize)
	}

	parts = make([]Part, numOfBlocks)

	var curFileOffset uint64
	var bytesLeft = size
	bsbu64 := blockSize

	for i := 0; i < numOfBlocks; i++ {
		var chunkSize = bsbu64
		if bytesLeft < bsbu64 { // last is a short block
			chunkSize = bytesLeft
		}

		fp := NewPart(curFileOffset, uint32(chunkSize), i, sourceURI, targetAlias)
		fp.NumberOfBlocks = numOfBlocks
		fp.BufferQ = bufferQ
		fp.BlockSize = uint32(blockSize)
		parts[i] = *fp
		curFileOffset = curFileOffset + bsbu64
		bytesLeft = bytesLeft - chunkSize
	}

	return
}

//NewPart represents a block of data to be read from the source and written to the target
func NewPart(offset uint64, bytesCount uint32, ordinal int, sourceURI string, targetAlias string) *Part {

	var idStr = fmt.Sprintf("%016x", offset)

	return &Part{
		Offset:                  offset,
		BytesToRead:             bytesCount,
		Data:                    nil,
		BlockID:                 base64.StdEncoding.EncodeToString([]byte(idStr)),
		Ordinal:                 ordinal,
		SourceURI:               sourceURI,
		TargetAlias:             targetAlias,
		DuplicateOfBlockOrdinal: -1}
}

//NewBuffer TODO
func (p *Part) NewBuffer() *bytes.Buffer {
	return bytes.NewBuffer(p.Data)
}

//ToString prints friendly format.
func (p *Part) ToString() string {
	str := fmt.Sprintf("  [FileChunk(%s):(Offset=%v,Size=%vB)]\n", p.BlockID, p.Offset, p.BytesToRead)
	return str
}

//GetBuffer sets the part's buffer (p.Data) to slice of bytes of size BytesToRead.
//The slice is read from channel of pre-allocated buffers. If the channel is empty a new slice is allocated.
func (p *Part) GetBuffer() {

	//only retrive from the buffer if channel is available
	if p.BufferQ == nil {
		return
	}

	select {
	case p.Data = <-p.BufferQ:
	default:
		p.Data = make([]byte, p.BlockSize)
	}

	p.Data = p.Data[:p.BytesToRead]
}

//ReturnBuffer adds part's buffer to channel so it can be reused.
func (p *Part) ReturnBuffer() {

	//only return to the buffer if channel is available
	if p.BufferQ == nil {
		p.Data = nil
		return
	}

	select {
	case p.BufferQ <- p.Data:
	default:
		//avoids blocking when the channel is full...
	}

	p.Data = nil
}

//IsMD5Computed TODO
func (p *Part) IsMD5Computed() bool {
	return p.md5Value != ""
}

// MD5  returns computed MD5 for this block or empty string if no data yet.
func (p *Part) MD5() string {
	// haven't yet computed the MD5, and have data to do so
	if p.md5Value == "" && p.Data != nil {
		h := md5.New()

		h.Write(p.Data)
		p.md5Value = base64.StdEncoding.EncodeToString(h.Sum(nil))
	}
	return p.md5Value
}

// LookupMD5DupeOrdinal finds the ordinal of a block which has the same data as this one. If none, then -1.
func (p *Part) LookupMD5DupeOrdinal() (ordinal int) {

	if p.Data == nil { // error, somehow don't have any data yet
		return -1
	}

	var md5Value = p.MD5()
	var dupOfBlock = -1

	MD5ToBlockIDLock.Lock()
	alias, keyExists := MD5ToBlockID[md5Value]

	if !keyExists { //TODO: small quirk, since default integer is 0, might miss dup where first two blocks are the same
		MD5ToBlockID[md5Value] = p.Ordinal // haven't seen this hash before, so remember this block ordinal
		dupOfBlock = -1
	} else {
		dupOfBlock = alias
	}
	MD5ToBlockIDLock.Unlock()
	ordinal = dupOfBlock
	return
}
