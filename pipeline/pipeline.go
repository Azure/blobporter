package pipeline

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
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log"
	"sync"
	"time"

	"math"

	"github.com/Azure/blobporter/util"
)

//NewBytesBufferChan creates a channel with 'n' slices of []bytes.
//'n' is the bufferQCapacity. If the bufferQCapacity times bufferSize is greater than 1 GB
// 'n' is limited to a value that meets the constraint.
func NewBytesBufferChan(bufferSize uint64) *chan []byte {

	bufferQCapacity := util.GB / bufferSize

	c := make(chan []byte, bufferQCapacity)
	var index uint64
	//only preallocated a quarter of the capacity.

	for index = 0; index < uint64(math.Ceil(float64(bufferQCapacity)*.25)); index++ {
		c <- make([]byte, bufferSize)
	}
	return &c
}

//SourcePipeline operations that abstract the creation of the empty and read parts channels.
type SourcePipeline interface {
	ConstructBlockInfoQueue(blockSize uint64) (partsQ *chan Part, numOfBlocks int, Size uint64)
	ExecuteReader(partsQ *chan Part, readPartsQ *chan Part, id int, wg *sync.WaitGroup)
	GetSourcesInfo() []string
}

//TargetPipeline operations that abstract how parts a written and processed to a given target
type TargetPipeline interface {
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
	BufferQ                 *chan []byte
}

// StorageAccountCredentials a central location for account info.
type StorageAccountCredentials struct {
	AccountName string // short name of the storage account.  e.g., mystore
	AccountKey  string // Base64-encoded storage account key
}

//ConstructPartsQueue  TODO
func ConstructPartsQueue(size uint64, blockSize uint64, sourceURI string, targetAlias string) (partsQ *chan Part, numOfBlocks int, commmitList []string) {
	var bsib = blockSize
	numOfBlocks = int((size + (bsib - 1)) / bsib)

	if numOfBlocks > util.MaxBlockCount { // more than 50,000 blocks needed, so can't work
		var minBlkSize = (size + util.MaxBlockCount - 1) / util.MaxBlockCount
		log.Fatalf("Block size is too small, minimum block size for this file would be %d bytes", minBlkSize)
	}

	//Qcopy := make(chan Part, numOfBlocks)
	Qcopy := make(chan Part)

	var curFileOffset uint64
	var bytesLeft = size
	bsbu64 := blockSize

	var curCommitList = make([]string, numOfBlocks)
	commmitList = curCommitList

	bufferQ := NewBytesBufferChan(blockSize)

	go func() {
		for i := 0; i < numOfBlocks; i++ {
			var chunkSize = bsbu64
			if bytesLeft < bsbu64 { // last is a short block
				chunkSize = bytesLeft
			}

			fp := NewPart(curFileOffset, uint32(chunkSize), i, sourceURI, targetAlias)
			fp.NumberOfBlocks = numOfBlocks
			fp.BufferQ = bufferQ
			fp.BlockSize = uint32(blockSize)
			Qcopy <- *fp // put it on the channel for the Readers

			curFileOffset = curFileOffset + bsbu64
			bytesLeft = bytesLeft - chunkSize
			curCommitList[i] = string(i)
		}

		close(Qcopy) // indicate end of the block list
	}()
	partsQ = &Qcopy

	return
}

//NewPart TODO
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

//ClonePart TODO
func (p *Part) ClonePart() *Part {

	return &Part{
		Offset:                  p.Offset,
		BytesToRead:             p.BytesToRead,
		Data:                    p.Data,
		BlockID:                 p.BlockID,
		Ordinal:                 p.Ordinal,
		BlockSize:               p.BlockSize,
		BufferQ:                 p.BufferQ,
		SourceURI:               p.SourceURI,
		TargetAlias:             p.TargetAlias,
		NumberOfBlocks:          p.NumberOfBlocks,
		md5Value:                p.md5Value,
		DuplicateOfBlockOrdinal: p.DuplicateOfBlockOrdinal}

}

//ToString prints friendly format.
func (p *Part) ToString() string {
	str := fmt.Sprintf("  [FileChunk(%s):(Offset=%v,Size=%vB)]\n", p.BlockID, p.Offset, p.BytesToRead)
	return str
}

//GetBuffer sets the part's buffer (p.Data) to slice of bytes of size BytesToRead.
//The slice is read from channel of pre-allocated buffers. If the channel is empty a new slice is allocated.
func (p *Part) GetBuffer() {

	//if the channel is empty allocate more memory to avoid blocking the reader
	if len(*p.BufferQ) == 0 {

		p.Data = make([]byte, p.BlockSize)
	} else {
		p.Data = <-(*p.BufferQ)
	}

	p.Data = p.Data[:p.BytesToRead]

}

//ReturnBuffer adds part's buffer to channel so it can be reused.
func (p *Part) ReturnBuffer() {
	p.Data = p.Data[0:0]
	//return only if space in the buffer exists to avoid blocking
	if len(*p.BufferQ) < cap(*p.BufferQ) {
		(*p.BufferQ) <- p.Data
	}
}

// MD5  returns computed MD5 for this block or empty string if no data yet.
func (p *Part) MD5() string {
	// haven't yet computed the MD5, and have data to do so
	if p.md5Value == "" && p.Data != nil {
		h := md5.New()
		h.Write(p.Data)
		p.md5Value = hex.EncodeToString(h.Sum(nil))
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
