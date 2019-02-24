package transfer

import (
	"log"
	"sync"
	"time"

	"github.com/Azure/blobporter/internal"

	"github.com/Azure/blobporter/pipeline"
)

// Worker represents a worker routine that transfers data to a target and commits the list if applicable.
type Worker struct {
	workerID    int
	workerQueue chan pipeline.Part
	workerWG          *sync.WaitGroup
	committerWG       *sync.WaitGroup
	commitListHandler *commitListHandler
	dupeLevel         DupeCheckLevel
}

const (
	//CommitEvent TODO
	CommitEvent internal.EventName = "commit"
	//BufferEvent TODO
	BufferEvent = "buffer"
	//DataWrittenEvent TODO
	DataWrittenEvent = "data-written"
	//WrittenPartEvent TODO
	WrittenPartEvent = "written-part"
)

// newWorker creates a new instance of upload Worker
//func newWorker(workerID int, workerQueue chan pipeline.Part, resultQueue chan pipeline.WorkerResult, wg *sync.WaitGroup, d DupeCheckLevel) Worker {
func newWorker(workerID int, workerQueue chan pipeline.Part, commit *commitListHandler,
	workerWG *sync.WaitGroup, committerWG *sync.WaitGroup, d DupeCheckLevel) Worker {

	return Worker{
		workerQueue: workerQueue,
		workerID:    workerID,
		workerWG:    workerWG,
		committerWG: committerWG,
		commitListHandler: commit,
		dupeLevel:         d}

}

// startWorker starts a worker that reads from the worker queue channel. Which contains the read parts from the source.
// Calls the target's WritePart implementation and sends the result to the results channel.
func (w *Worker) startWorker(target pipeline.TargetPipeline) {
	var tb pipeline.Part
	var ok bool
	var okc bool
	var commitReq commitInfo
	var duration time.Duration
	var startTime time.Time
	var retries int
	var err error
	var t = target
	bufferSize := int(float64(len(w.workerQueue)) / float64(cap(w.workerQueue)) * 100)
	var doneWQ bool
	for {
		//wr := pipeline.WorkerResult{}

		select {
		case commitReq, okc = <-w.commitListHandler.commitRequests():
			if !okc {
				defer w.committerWG.Done()
				return
			}

			if _, err = t.CommitList(commitReq.list, int(commitReq.numOfBlocks), commitReq.targetName); err != nil {
				log.Fatal(err)
			}

			internal.EventSink.AddSumEvent(internal.Worker, CommitEvent, "", float64(1))

			if err = w.commitListHandler.trackCommitted(commitReq.targetName); err != nil {
				log.Fatal(err)
			}

		case tb, ok = <-w.workerQueue:

			if !ok { // Work queue has been closed, so done.  Signal only one done
				if !doneWQ {
					w.workerWG.Done()
					doneWQ = true
				}
				continue
			}

			checkForDuplicateChunk(&tb, w.dupeLevel)

			if tb.DuplicateOfBlockOrdinal >= 0 {
				// This block is a duplicate of another, so don't upload it.
				// Instead, just reflect it (with it's "duplicate" status)
				// onwards in the completion channel
				w.commitListHandler.addWorkerResult(w.workerID, &tb, time.Now(), 0, "Success", 0)
				continue
			}
			if duration, startTime, retries, err = t.WritePart(&tb); err == nil {
				tb.ReturnBuffer()
				w.commitListHandler.addWorkerResult(w.workerID, &tb, startTime, duration, "Success", retries)
				internal.EventSink.AddSumEvent(internal.Worker, DataWrittenEvent, "", float64(tb.BytesToRead))
				internal.EventSink.AddSumEvent(internal.Worker, WrittenPartEvent, "", float64(1))

			} else {
				log.Fatal(err)
			}
		}
		bufferSize = int(float64(len(w.workerQueue)) / float64(cap(w.workerQueue)) * 100)
		internal.EventSink.AddEvent(internal.Worker, BufferEvent, "", internal.EventData{Value: bufferSize})

	}

}
