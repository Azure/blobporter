package sources

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/util"
)

//SourceDefinition TODO
type SourceDefinition struct {
	Names           []string
	Size            uint64
	NumberOfSources int
}

//ParseSourceDefinitions TODO
func ParseSourceDefinitions(def string) ([]SourceDefinition, error) {
	data := strings.Split(def, ";")
	defs := make([]SourceDefinition, len(data))
	for s := 0; s < len(data); s++ {
		def, err := newSourceDefinition(data[s])
		if err != nil {
			return nil, err
		}
		defs[s] = *def
	}
	return defs, nil
}

func newSourceDefinition(def string) (*SourceDefinition, error) {
	data := strings.Split(def, ":")

	if len(data) != 2 {
		return nil, fmt.Errorf("Invalid format: %s\nThe source definition must be [Size]:[NumOfSources]", data)
	}

	size, err := util.ByteCountFromSizeString(data[0])
	if err != nil {
		return nil, fmt.Errorf("Invalid format. The source definition must be [Size]:[NumOfSources]. The size is invalid. Error:%v", err)
	}
	var numOfSrcs int
	numOfSrcs, err = strconv.Atoi(data[1])
	if err != nil {
		return nil, fmt.Errorf("Invalid format. The source definition must be [Size]:[NumOfSources]. Failed to parse the number of sources. Error:%v", err)
	}
	names := make([]string, numOfSrcs)
	for n := 0; n < numOfSrcs; n++ {
		names[n] = fmt.Sprintf("%s%v.dat", strings.Replace(def, ":", "_", 1), time.Now().Nanosecond())
	}

	return &SourceDefinition{Size: size, NumberOfSources: numOfSrcs, Names: names}, nil
}

func (d *SourceDefinition) getSourceInfo() []pipeline.SourceInfo {
	infos := make([]pipeline.SourceInfo, len(d.Names))
	for i, n := range d.Names {
		infos[i] = pipeline.SourceInfo{SourceName: n, TargetAlias: n, Size: d.Size}
	}
	return infos
}

//PerfSourcePipeline TODO
type PerfSourcePipeline struct {
	definitions []SourceDefinition
	blockSize   uint64
	dataBlock   []byte
	includeMD5  bool
}

//PerfSourceParams TODO
type PerfSourceParams struct {
	SourceParams
	Definitions []SourceDefinition
	BlockSize   uint64
}

//newPerfSourcePipeline TODO
func newPerfSourcePipeline(params PerfSourceParams) []pipeline.SourcePipeline {
	ssps := make([]pipeline.SourcePipeline, 1)
	ssp := PerfSourcePipeline{
		definitions: params.Definitions,
		blockSize:   params.BlockSize,
		includeMD5:  params.CalculateMD5}
	ssp.setSharedDataBlock()
	ssps[0] = &ssp
	return ssps
}

//ConstructBlockInfoQueue TODO
func (s *PerfSourcePipeline) ConstructBlockInfoQueue(blockSize uint64) (partitionQ chan pipeline.PartsPartition, partsQ chan pipeline.Part, numOfBlocks int, size uint64) {
	allParts := make([][]pipeline.Part, len(s.definitions))
	//disable memory buffer for parts (bufferQ == nil)
	var bufferQ chan []byte
	largestNumOfParts := 0
	for i, source := range s.definitions {
		srcParts := make([]pipeline.Part, 0)
		for _, name := range source.Names {
			size = size + source.Size
			parts, sourceNumOfBlocks := pipeline.ConstructPartsQueue(source.Size, blockSize, name, name, bufferQ)
			srcParts = append(srcParts, parts...)
			numOfBlocks = numOfBlocks + sourceNumOfBlocks
		}
		allParts[i] = srcParts
		if largestNumOfParts < len(srcParts) {
			largestNumOfParts = len(srcParts)
		}
	}

	partsQ = make(chan pipeline.Part, numOfBlocks)

	for i := 0; i < largestNumOfParts; i++ {
		for _, ps := range allParts {
			if i < len(ps) {
				partsQ <- ps[i]
			}
		}
	}

	close(partsQ)

	return
}

//ExecuteReader TODO
func (s *PerfSourcePipeline) ExecuteReader(partitionQ chan pipeline.PartsPartition, partsQ chan pipeline.Part, readPartsQ chan pipeline.Part, id int, wg *sync.WaitGroup) {
	var blocksHandled = 0
	defer wg.Done()
	for {
		p, ok := <-partsQ

		if !ok {
			return // no more blocks of file data to be read
		}
		if s.includeMD5 {
			p.MD5()
		}

		p.Data = s.dataBlock

		if len(s.dataBlock) != int(p.BytesToRead) {
			small := s.dataBlock[:p.BytesToRead]
			p.Data = small
		}

		readPartsQ <- p
		blocksHandled++
	}
}

//GetSourcesInfo TODO
func (s *PerfSourcePipeline) GetSourcesInfo() []pipeline.SourceInfo {
	srcInfo := make([]pipeline.SourceInfo, 0)
	for _, def := range s.definitions {
		srcInfo = append(srcInfo, def.getSourceInfo()...)
	}
	return srcInfo
}

var data = []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
var block []byte

func (s *PerfSourcePipeline) setSharedDataBlock() {
	s.dataBlock = s.getDataBlock(s.blockSize)
}

func (s *PerfSourcePipeline) getDataBlock(size uint64) []byte {
	b := make([]byte, size)
	ln := len(data)
	for i := 0; i < len(b); i++ {
		b[i] = data[rand.Intn(ln)]
	}

	return b
}
