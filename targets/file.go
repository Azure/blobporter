package targets

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Azure/blobporter/pipeline"
)

////////////////////////////////////////////////////////////
///// File Target
////////////////////////////////////////////////////////////

//File represents a file target
type File struct {
	Creds          *pipeline.StorageAccountCredentials
	Container      string
	TargetFileName string
	FileHandles    chan *os.File
	FileStat       *os.FileInfo
}

//NewFile creates a new file target and 'n' number of handles for concurrent writes to a file.
func NewFile(targetFileName string, overwrite bool, numberOfHandles int) pipeline.TargetPipeline {
	var fh *os.File
	var err error

	if fh, err = os.Create(targetFileName); os.IsExist(err) {
		if !overwrite {
			log.Fatal("The file already exists and file overwrite is disabled")
		}
		if err = os.Remove(targetFileName); err != nil {
			log.Fatal(err)
		}
		if fh, err = os.Create(targetFileName); err != nil {
			log.Fatal(err)
		}
	}

	var fileStat os.FileInfo

	if fileStat, err = os.Stat(targetFileName); err != nil {
		log.Fatal(err)
	}

	fhQ := make(chan *os.File, numberOfHandles)

	for i := 0; i < numberOfHandles; i++ {
		fhQ <- fh

		if fh, err = os.OpenFile(targetFileName, os.O_WRONLY, os.ModeAppend); err != nil {
			log.Fatal(err)
		}
	}

	return File{FileHandles: fhQ, FileStat: &fileStat, TargetFileName: targetFileName}
}

//CommitList implements CommitList from the pipeline.TargetPipeline interface.
//For a file download a final commit is not required and this implementation closes all the filehandles.
func (t File) CommitList(listInfo *pipeline.TargetCommittedListInfo, numberOfBlocks int, targetName string) (msg string, err error) {

	close(t.FileHandles)

	for {

		fh, ok := <-t.FileHandles

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
func (t File) ProcessWrittenPart(result *pipeline.WorkerResult, listInfo *pipeline.TargetCommittedListInfo) (requeue bool, err error) {
	return false, nil
}

//WritePart implements WritePart from the pipeline.TargetPipeline interface.
//Writes to a local file using a filehandle received from a channel.
func (t File) WritePart(part *pipeline.Part) (duration time.Duration, startTime time.Time, numOfRetries int, err error) {
	startTime = time.Now()
	fh := <-t.FileHandles
	if _, err := fh.WriteAt((*part).Data, int64((*part).Offset)); err != nil {
		log.Fatal(err)
	}
	duration = time.Now().Sub(startTime)

	t.FileHandles <- fh

	return
}
