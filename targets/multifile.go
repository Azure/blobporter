package targets

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Azure/blobporter/pipeline"
	"github.com/Azure/blobporter/util"
)

////////////////////////////////////////////////////////////
///// File Target
////////////////////////////////////////////////////////////

//MultiFile represents an OS file(s) target
type MultiFile struct {
	Container       string
	FileHandles     sync.Map
	NumberOfHandles int
	OverWrite       bool
	sync.Mutex
}

//NewMultiFile creates a new multi file target and 'n' number of handles for concurrent writes to a file.
func NewMultiFile(overwrite bool, numberOfHandles int) pipeline.TargetPipeline {

	//return &MultiFile{FileHandles: make(map[string]chan *os.File), NumberOfHandles: numberOfHandles, OverWrite: overwrite}
	return &MultiFile{NumberOfHandles: numberOfHandles, OverWrite: overwrite}
}

//PreProcessSourceInfo implementation of PreProcessSourceInfo from the pipeline.TargetPipeline interface.
//Passthrough no need to pre-process for a file target.
func (t *MultiFile) PreProcessSourceInfo(source *pipeline.SourceInfo, blockSize uint64) (err error) {
	return nil
}

func (t *MultiFile) createFileIfNotExists(targetAlias string) error {
	var fh *os.File
	var err error

	path := filepath.Dir(targetAlias)

	if path != "" {
		err = os.MkdirAll(path, 0777)

		if err != nil {
			return err
		}
	}
	defer fh.Close()
	if fh, err = os.Create(targetAlias); os.IsExist(err) {
		if !t.OverWrite {
			return fmt.Errorf("The file already exists and file overwrite is disabled")
		}
		if err = os.Remove(targetAlias); err != nil {
			return err
		}

		if fh, err = os.Create(targetAlias); err != nil {
			return err
		}

	}

	return nil
}

//CommitList implements CommitList from the pipeline.TargetPipeline interface.
//For a file download a final commit is not required and this implementation closes all the filehandles.
func (t *MultiFile) CommitList(listInfo *pipeline.TargetCommittedListInfo, numberOfBlocks int, targetName string) (msg string, err error) {

	value, ok := t.FileHandles.Load(targetName)
	util.PrintfIfDebug("CommitList -> targetname:%v listinfo:%+v\n", targetName, *listInfo)

	if !ok {
		return "", nil
	}

	fhq := value.(chan *os.File)
	close(fhq)
	for {

		fh, ok := <-fhq

		if !ok {
			break
		}
		err = fh.Close()

		if err != nil {
			return fmt.Sprintf("Closing handle for target:%v failed.", targetName), err
		}
	}

	msg = fmt.Sprintf("\rFile Saved:%v, Parts: %d",
		targetName, numberOfBlocks)
	err = nil
	return
}

//ProcessWrittenPart implements ProcessWrittenPart from the pipeline.TargetPipeline interface.
//Passthrough implementation as no post-written-processing is required (e.g. maintain a list) when files are downloaded.
func (t *MultiFile) ProcessWrittenPart(result *pipeline.WorkerResult, listInfo *pipeline.TargetCommittedListInfo) (requeue bool, err error) {
	//fmt.Printf("WrittenPart->%+v \n", *result)
	return false, nil
}

func (t *MultiFile) loadHandle(part *pipeline.Part) (*os.File, error) {
	var fh *os.File
	var fhQ chan *os.File
	var err error

	//if this is a small file open the handle but not leave open
	if part.NumberOfBlocks == 1 {
		if err = t.createFileIfNotExists(part.TargetAlias); err != nil {
			return nil, err
		}
		if fh, err = os.OpenFile(part.TargetAlias, os.O_WRONLY, os.ModeAppend); err != nil {
			return nil, err
		}
		return fh, nil
	}

	t.Lock()
	value, _ := t.FileHandles.Load(part.TargetAlias)
	if value == nil {
		fhQ = make(chan *os.File, t.NumberOfHandles)
		if err = t.createFileIfNotExists(part.TargetAlias); err != nil {
			return nil, err
		}
		for i := 0; i < t.NumberOfHandles; i++ {
			if fh, err = os.OpenFile(part.TargetAlias, os.O_WRONLY, os.ModeAppend); err != nil {
				return nil, err
			}
			fhQ <- fh
		}
		t.FileHandles.Store(part.TargetAlias, fhQ)

	} else {
		fhQ = value.(chan *os.File)
	}
	t.Unlock()

	fh = <-fhQ
	return fh, nil
}

func (t *MultiFile) closeOrKeepHandle(part *pipeline.Part, fh *os.File) error {
	if part.NumberOfBlocks == 1 {
		return fh.Close()
	}

	t.Lock()
	defer t.Unlock()
	value, ok := t.FileHandles.Load(part.TargetAlias)

	if ok {
		fhq := value.(chan *os.File)
		fhq <- fh
		t.FileHandles.Store(part.TargetAlias, fhq)
		return nil
	}

	return fmt.Errorf("File handle channel not found in the map")

}

//WritePart implements WritePart from the pipeline.TargetPipeline interface.
//Writes to a local file using a filehandle received from a channel.
func (t *MultiFile) WritePart(part *pipeline.Part) (duration time.Duration, startTime time.Time, numOfRetries int, err error) {
	startTime = time.Now()
	var fh *os.File

	if fh, err = t.loadHandle(part); err != nil {
		log.Fatal(fmt.Errorf("Failed to load the handle: %v", err))
	}

	if _, err = fh.WriteAt((*part).Data, int64((*part).Offset)); err != nil {
		log.Fatal(fmt.Errorf("Failed to write data: %v", err))
	}

	if err = t.closeOrKeepHandle(part, fh); err != nil {
		log.Fatal(fmt.Errorf("Failed to close or keep the handle: %v", err))
	}
	duration = time.Now().Sub(startTime)

	return
}
