package targets

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
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
	FileHandles     map[string]chan *os.File
	NumberOfHandles int
	OverWrite       bool
	sync.Mutex
}

//NewMultiFile creates a new multi file target and 'n' number of handles for concurrent writes to a file.
func NewMultiFile(overwrite bool, numberOfHandles int) pipeline.TargetPipeline {

	return &MultiFile{FileHandles: make(map[string]chan *os.File), NumberOfHandles: numberOfHandles, OverWrite: overwrite}
}

//PreProcessSourceInfo implementation of PreProcessSourceInfo from the pipeline.TargetPipeline interface.
//Passthrough no need to pre-process for a file target.
func (t *MultiFile) PreProcessSourceInfo(source *pipeline.SourceInfo) (err error) {
	t.Lock()
	defer t.Unlock()

	var fh *os.File

	path := filepath.Dir(source.TargetAlias)

	if path != "" {
		err = os.MkdirAll(path, 0777)

		if err != nil {
			return
		}
	}

	if fh, err = os.Create(source.TargetAlias); os.IsExist(err) {
		if !t.OverWrite {
			return fmt.Errorf("The file already exists and file overwrite is disabled")
		}
		if err = os.Remove(source.TargetAlias); err != nil {
			return err
		}

		if fh, err = os.Create(source.TargetAlias); err != nil {
			return err
		}
	}

	fhQ := make(chan *os.File, t.NumberOfHandles)

	for i := 0; i < t.NumberOfHandles; i++ {
		if fh, err = os.OpenFile(source.TargetAlias, os.O_WRONLY, os.ModeAppend); err != nil {
			log.Fatal(err)
		}
		fhQ <- fh
	}
	t.FileHandles[source.TargetAlias] = fhQ

	return nil
}

//CommitList implements CommitList from the pipeline.TargetPipeline interface.
//For a file download a final commit is not required and this implementation closes all the filehandles.
func (t *MultiFile) CommitList(listInfo *pipeline.TargetCommittedListInfo, numberOfBlocks int, targetName string) (msg string, err error) {

	close(t.FileHandles[targetName])
	for {

		fh, ok := <-t.FileHandles[targetName]

		if !ok {
			break
		}

		fh.Close()
	}

	msg = fmt.Sprintf("\rFile Saved:%v, Parts: %d",
		targetName, numberOfBlocks)
	err = nil
	return
}

//ProcessWrittenPart implements ProcessWrittenPart from the pipeline.TargetPipeline interface.
//Passthrough implementation as no post-written-processing is required (e.g. maintain a list) when files are downloaded.
func (t *MultiFile) ProcessWrittenPart(result *pipeline.WorkerResult, listInfo *pipeline.TargetCommittedListInfo) (requeue bool, err error) {
	return false, nil
}

//WritePart implements WritePart from the pipeline.TargetPipeline interface.
//Writes to a local file using a filehandle received from a channel.
func (t *MultiFile) WritePart(part *pipeline.Part) (duration time.Duration, startTime time.Time, numOfRetries int, err error) {
	startTime = time.Now()

	fh := <-t.FileHandles[part.TargetAlias]
	if _, err := fh.WriteAt((*part).Data, int64((*part).Offset)); err != nil {
		log.Fatal(err)
	}
	duration = time.Now().Sub(startTime)

	t.FileHandles[part.TargetAlias] <- fh

	return
}
