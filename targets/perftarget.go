package targets

import (
	"time"

	"github.com/Azure/blobporter/pipeline"
)

////////////////////////////////////////////////////////////
///// Perf Target
////////////////////////////////////////////////////////////

//PerfTarget TODO
type PerfTarget struct {
}

//NewPerfTargetPipeline creates a new Azure Block target
func NewPerfTargetPipeline() pipeline.TargetPipeline {
	return &PerfTarget{}
}

//PreProcessSourceInfo TODO.
func (t *PerfTarget) PreProcessSourceInfo(source *pipeline.SourceInfo, blockSize uint64) (err error) {
	return nil
}

//CommitList TODO
func (t *PerfTarget) CommitList(listInfo *pipeline.TargetCommittedListInfo, NumberOfBlocks int, targetName string) (msg string, err error) {
	msg = "Perf test committed"
	err = nil
	return
}

//ProcessWrittenPart TODO
func (t *PerfTarget) ProcessWrittenPart(result *pipeline.WorkerResult, listInfo *pipeline.TargetCommittedListInfo) (requeue bool, err error) {
	requeue = false
	err = nil
	return
}

//WritePart TODO
func (t *PerfTarget) WritePart(part *pipeline.Part) (duration time.Duration, startTime time.Time, numOfRetries int, err error) {

	s := time.Now()
	numOfRetries = 0
	err = nil
	startTime = s
	duration = time.Now().Sub(s)

	return
}
