package targets

import (
	"github.com/Azure/blobporter/util"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/Azure/blobporter/pipeline"
)

////////////////////////////////////////////////////////////
///// File Target
////////////////////////////////////////////////////////////

//MultiFile represents an OS file(s) target
type MultiFile struct {
	Container       string
	NumberOfHandles int
	OverWrite       bool
	sync.Mutex
	fileHandlesMan *fileHandlePool
}

//NewMultiFile creates a new multi file target and 'n' number of handles for concurrent writes to a file.
func NewMultiFile(overwrite bool, numberOfHandles int) pipeline.TargetPipeline {

	//return &MultiFile{FileHandles: make(map[string]chan *os.File), NumberOfHandles: numberOfHandles, OverWrite: overwrite}
	//fhm := newFileHandlePool(numberOfHandles, overwrite)
	fhm := newfileHandlePool(int(maxFileHandlesInCache),numberOfHandles, overwrite)

	return &MultiFile{NumberOfHandles: numberOfHandles,
		fileHandlesMan: fhm,
		OverWrite:      overwrite}
}

//PreProcessSourceInfo implementation of PreProcessSourceInfo from the pipeline.TargetPipeline interface.
//Passthrough no need to pre-process for a file target.
func (t *MultiFile) PreProcessSourceInfo(source *pipeline.SourceInfo, blockSize uint64) (err error) {
	return nil
}

//CommitList implements CommitList from the pipeline.TargetPipeline interface.
//For a file download a final commit is not required and this implementation closes all the filehandles.
func (t *MultiFile) CommitList(listInfo *pipeline.TargetCommittedListInfo, numberOfBlocks int, targetName string) (msg string, err error) {
	msg = fmt.Sprintf("\rFile Saved:%v, Parts: %d",
		targetName, numberOfBlocks)
	err = t.fileHandlesMan.closeHandles(targetName)
	return
}

//ProcessWrittenPart implements ProcessWrittenPart from the pipeline.TargetPipeline interface.
//Passthrough implementation as no post-written-processing is required (e.g. maintain a list) when files are downloaded.
func (t *MultiFile) ProcessWrittenPart(result *pipeline.WorkerResult, listInfo *pipeline.TargetCommittedListInfo) (requeue bool, err error) {
	return false, nil
}

func (t *MultiFile) loadHandle(part *pipeline.Part) (*os.File, error) {
	s:=time.Now()
	defer util.PrintfIfDebug("loadHandle-> name:%v  duration:%v", part.TargetAlias, time.Now().Sub(s))
	return t.fileHandlesMan.getHandle(part.TargetAlias)
}

func (t *MultiFile) closeOrKeepHandle(part *pipeline.Part, fh *os.File) error {
	s:=time.Now()
	defer util.PrintfIfDebug("closeOrKeepHandle-> name:%v  duration:%v", part.TargetAlias, time.Now().Sub(s))
	return t.fileHandlesMan.returnHandle(part.TargetAlias, fh)
}

//WritePart implements WritePart from the pipeline.TargetPipeline interface.
//Writes to a local file using a filehandle received from a channel.
func (t *MultiFile) WritePart(part *pipeline.Part) (duration time.Duration, startTime time.Time, numOfRetries int, err error) {
	var fh *os.File

	if fh, err = t.loadHandle(part); err != nil {
		log.Fatal(fmt.Errorf("Failed to load the handle: %v", err))
	}
	startTime = time.Now()
	
	if _, err = fh.WriteAt((*part).Data, int64((*part).Offset)); err != nil {
		log.Fatal(fmt.Errorf("Failed to write data: %v", err))
	}
	
	duration = time.Now().Sub(startTime)

	if err = t.closeOrKeepHandle(part, fh); err != nil {
		log.Fatal(fmt.Errorf("Failed to close or keep the handle: %v", err))
	}
	
	return
}
