package targets

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Azure/blobporter/internal"
	"github.com/Azure/blobporter/util"

	"github.com/Azure/blobporter/pipeline"
)

////////////////////////////////////////////////////////////
///// FileSystemTarget
////////////////////////////////////////////////////////////

//FileSystemTarget represents an OS file(s) target
type FileSystemTarget struct {
	fileHandlesMan *internal.FileHandlePool
}

//NewFileSystemTargetPipeline creates a new multi file target and 'n' number of handles for concurrent writes to a file.
func NewFileSystemTargetPipeline(overwrite bool, numberOfHandles int) pipeline.TargetPipeline {

	fhm := internal.NewFileHandlePool(numberOfHandles, internal.Write, overwrite)

	return &FileSystemTarget{
		fileHandlesMan: fhm}
}

//PreProcessSourceInfo implementation of PreProcessSourceInfo from the pipeline.TargetPipeline interface.
//Passthrough no need to pre-process for a file target.
func (t *FileSystemTarget) PreProcessSourceInfo(source *pipeline.SourceInfo, blockSize uint64) (err error) {
	return nil
}

//CommitList implements CommitList from the pipeline.TargetPipeline interface.
//For a file download a final commit is not required and this implementation closes all the filehandles.
func (t *FileSystemTarget) CommitList(listInfo *pipeline.TargetCommittedListInfo, numberOfBlocks int, targetName string) (msg string, err error) {
	msg = fmt.Sprintf("\rFile Saved:%v, Parts: %d",
		targetName, numberOfBlocks)
	err = t.fileHandlesMan.CloseHandles(targetName)
	return
}

//ProcessWrittenPart implements ProcessWrittenPart from the pipeline.TargetPipeline interface.
//Passthrough implementation as no post-written-processing is required (e.g. maintain a list) when files are downloaded.
func (t *FileSystemTarget) ProcessWrittenPart(result *pipeline.WorkerResult, listInfo *pipeline.TargetCommittedListInfo) (requeue bool, err error) {
	return false, nil
}

func (t *FileSystemTarget) loadHandle(part *pipeline.Part) (*os.File, error) {
	s := time.Now()
	defer util.PrintfIfDebug("loadHandle-> name:%v  duration:%v", part.TargetAlias, time.Now().Sub(s))
	return t.fileHandlesMan.GetHandle(part.TargetAlias)
}

func (t *FileSystemTarget) closeOrKeepHandle(part *pipeline.Part, fh *os.File) error {
	s := time.Now()
	defer util.PrintfIfDebug("closeOrKeepHandle-> name:%v  duration:%v", part.TargetAlias, time.Now().Sub(s))
	return t.fileHandlesMan.ReturnHandle(part.TargetAlias, fh)
}

//WritePart implements WritePart from the pipeline.TargetPipeline interface.
//Writes to a local file using a filehandle received from a channel.
func (t *FileSystemTarget) WritePart(part *pipeline.Part) (duration time.Duration, startTime time.Time, numOfRetries int, err error) {
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
