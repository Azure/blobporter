package pipeline

import (
	"testing"

	"os"

	"github.com/Azure/blobporter/util"
	"github.com/stretchr/testify/assert"
)

var accountName = os.Getenv("ACCOUNT_NAME")
var accountKey = os.Getenv("ACCOUNT_KEY")
var blockSize = uint64(4 * util.MiByte)
var numOfReaders = 10
var numOfWorkers = 10

const (
	containerName1 = "bptest"
	containerName2 = "bphttptest"
)

func TestCreatePartsInPartition(t *testing.T) {
	var partitionSize int64 = 500
	ordinalStart := 1
	sourceNumOfBlocks := 10
	var blockSize int64 = 100
	parts, ordinal, numOfPartsInPartition := createPartsInPartition(partitionSize, 0, ordinalStart, sourceNumOfBlocks, blockSize, "SA", "TA", nil)

	assert.Equal(t, 5, len(parts))
	assert.Equal(t, 6, ordinal)
	assert.Equal(t, 5, numOfPartsInPartition)
}

func TestCreatePartsInPartitionOver(t *testing.T) {
	var partitionSize int64 = 501
	ordinalStart := 1
	sourceNumOfBlocks := 10
	var blockSize int64 = 100
	parts, ordinal, numOfPartsInPartition := createPartsInPartition(partitionSize, 0, ordinalStart, sourceNumOfBlocks, blockSize, "SA", "TA", nil)

	assert.Equal(t, 6, len(parts))
	assert.Equal(t, 7, ordinal)
	assert.Equal(t, 6, numOfPartsInPartition)
}

func TestCreatePartsInPartitionOfSize1(t *testing.T) {
	var partitionSize int64 = 1
	ordinalStart := 1
	sourceNumOfBlocks := 10
	var blockSize int64 = 100
	parts, ordinal, numOfPartsInPartition := createPartsInPartition(partitionSize, 0, ordinalStart, sourceNumOfBlocks, blockSize, "SA", "TA", nil)

	assert.Equal(t, 1, len(parts))
	assert.Equal(t, 2, ordinal)
	assert.Equal(t, 1, numOfPartsInPartition)
}

func TestConstructPartitionExact(t *testing.T) {
	partitionNumber := 10
	var size int64 = 10000
	var blockSize int64 = 100
	sourceURI := "S1"
	targetAlias := "TA"
	partitions := ConstructPartsPartition(partitionNumber, size, blockSize, sourceURI, targetAlias, nil)

	assert.Equal(t, partitionNumber, len(partitions))

	var offSet int64 = 1000
	var i int64
	var calcSize int64
	var calcSize2 int64
	j := 0
	for _, p := range partitions {
		assert.Equal(t, 10, p.NumOfParts)
		assert.Equal(t, offSet*i, p.Offset)
		assert.Equal(t, (size+blockSize-1)/blockSize, p.TotalNumOfParts)
		assert.Equal(t, size, p.TotalSize)
		assert.Equal(t, (size+int64(partitionNumber)-1)/int64(partitionNumber), p.PartitionSize)
		assert.Equal(t, ((size+int64(partitionNumber)-1)/int64(partitionNumber))/blockSize, int64(len(p.Parts)))
		i++
		calcSize = calcSize + p.PartitionSize

		for _, pip := range p.Parts {
			calcSize2 = calcSize2 + int64(pip.BytesToRead)
			assert.Equal(t, j, pip.Ordinal)
			j++
		}
	}

	assert.Equal(t, calcSize, size)
	assert.Equal(t, calcSize2, size)
}

//10,1283641361,4194304

func TestConstructPartitionOver(t *testing.T) {
	partitionNumber := 10
	var size int64 = 10001
	var blockSize int64 = 100
	sourceURI := "S1"
	targetAlias := "TA"
	partitions := ConstructPartsPartition(partitionNumber, size, blockSize, sourceURI, targetAlias, nil)

	assert.Equal(t, partitionNumber, len(partitions))

	var offSet int64 = 1000
	var i int64
	k := 0
	var calcSize int64
	var calcSize2 int64
	for j := 0; j < len(partitions); j++ {
		p := partitions[j]
		if j < len(partitions)-1 {
			assert.Equal(t, 10, p.NumOfParts)
			assert.Equal(t, offSet*i, p.Offset)
			assert.Equal(t, (size+blockSize-1)/blockSize, p.TotalNumOfParts)
			assert.Equal(t, size, p.TotalSize)
			assert.Equal(t, (size)/int64(partitionNumber), p.PartitionSize)
		} else {
			assert.Equal(t, 11, p.NumOfParts)
			assert.Equal(t, offSet*i, p.Offset)
			assert.Equal(t, (size+blockSize-1)/blockSize, p.TotalNumOfParts)
			assert.Equal(t, size, p.TotalSize)
			assert.Equal(t, int64(1001), p.PartitionSize)
		}
		calcSize = calcSize + p.PartitionSize
		i++

		for _, pip := range p.Parts {
			calcSize2 = calcSize2 + int64(pip.BytesToRead)
			assert.Equal(t, k, pip.Ordinal)
			k++
		}

	}
	assert.Equal(t, size, calcSize2)
	assert.Equal(t, size, calcSize)
}
func TestConstructPartitionUnder(t *testing.T) {
	partitionNumber := 10
	var size int64 = 9999
	var blockSize int64 = 100
	sourceURI := "S1"
	targetAlias := "TA"
	partitions := ConstructPartsPartition(partitionNumber, size, blockSize, sourceURI, targetAlias, nil)
	partitionSize := ((size) / int64(partitionNumber) / blockSize) * blockSize
	assert.Equal(t, partitionNumber, len(partitions))

	//offSet := size / int64(partitionNumber)
	var i int64
	var calcSize int64
	var calcSize2 int64
	k := 0
	pread := 0
	totalNumOfParts := (size + blockSize - 1) / blockSize
	for j := 0; j < len(partitions); j++ {
		p := partitions[j]
		if j < len(partitions)-1 {
			assert.Equal(t, int(partitionSize/blockSize), p.NumOfParts)
			assert.Equal(t, partitionSize*i, p.Offset)
			assert.Equal(t, totalNumOfParts, p.TotalNumOfParts)
			assert.Equal(t, size, p.TotalSize)
			assert.Equal(t, partitionSize, p.PartitionSize)
			pread = pread + p.NumOfParts
		} else {
			assert.Equal(t, int(totalNumOfParts-int64(pread)), p.NumOfParts)
			assert.Equal(t, partitionSize*i, p.Offset)
			assert.Equal(t, totalNumOfParts, p.TotalNumOfParts)
			assert.Equal(t, size, p.TotalSize)
			assert.Equal(t, size-partitionSize*i, p.PartitionSize)
		}
		calcSize = calcSize + p.PartitionSize
		i++

		for _, pip := range p.Parts {
			calcSize2 = calcSize2 + int64(pip.BytesToRead)
			assert.Equal(t, k, pip.Ordinal)
			k++
		}
	}

	assert.Equal(t, size, calcSize)
	assert.Equal(t, size, calcSize2)
}
