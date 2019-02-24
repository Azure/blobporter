package transfer

import (
	"log"
	"sync"
	"time"

	"github.com/Azure/blobporter/internal"
	"github.com/Azure/blobporter/pipeline"
)

type commitListHandler struct {
	commitInfos map[string]commitInfo
	tracker *internal.TransferTracker
	target  pipeline.TargetPipeline
	resultQ       chan pipeline.WorkerResult
	commitInfoReq chan commitInfo
	wg            *sync.WaitGroup
	done          chan bool
}

type commitInfo struct {
	list        *pipeline.TargetCommittedListInfo
	numOfBlocks int32
	targetName  string
}

const (
	//WriteDurationEvent TODO
	WriteDurationEvent internal.EventName = "write-duration"
)

func newCommitListHandler(tracker *internal.TransferTracker, target pipeline.TargetPipeline) *commitListHandler {

	commitInfos := make(map[string]commitInfo)
	resultQ := make(chan pipeline.WorkerResult, 1000)
	commitInfoReq := make(chan commitInfo, 1000)
	c := commitListHandler{
		commitInfos:   commitInfos,
		tracker:       tracker,
		target:        target,
		resultQ:       resultQ,
		commitInfoReq: commitInfoReq,
		wg:            &sync.WaitGroup{},
	}
	c.wg.Add(1)
	go func() {
		defer close(commitInfoReq)
		defer c.wg.Done()
		var requeue bool
		var err error
		for {

			result, ok := <-resultQ

			if !ok {
				return
			}

			cinfo := commitInfos[result.SourceURI]
			if cinfo.list == nil {
				cinfo.list = &pipeline.TargetCommittedListInfo{}
			}
			if requeue, err = target.ProcessWrittenPart(&result, cinfo.list); err != nil {
				log.Fatal(err)
			}

			if requeue {
				resultQ <- result
				continue
			}

			cinfo.numOfBlocks++
			cinfo.targetName = result.TargetName
			commitInfos[result.SourceURI] = cinfo

			internal.EventSink.AddSumEvent(internal.CommitListHandler, WriteDurationEvent, result.TargetName, float64(result.Stats.Duration))

			if cinfo.numOfBlocks == int32(result.NumberOfBlocks) {
				commitInfoReq <- cinfo
			}
		}

	}()
	return &c
}
func (c *commitListHandler) setDone() {
	close(c.resultQ)
}
func (c *commitListHandler) Wait() {
	c.wg.Wait()
}
func (c *commitListHandler) commitRequests() chan commitInfo {
	return c.commitInfoReq
}

func (c *commitListHandler) trackCommitted(targetName string) error {
	if c.tracker != nil {
		return c.tracker.TrackFileTransferComplete(targetName)
	}
	return nil
}

func (c *commitListHandler) addWorkerResult(workerID int, tb *pipeline.Part, startTime time.Time, d time.Duration, status string, retries int) pipeline.WorkerResult {

	var wStats = pipeline.WorkerResultStats{
		StartTime: startTime,
		Duration:  d,
		Retries:   retries}

	tb.Data = nil
	tb.BufferQ = nil

	wr := pipeline.WorkerResult{
		Stats:                   &wStats,
		BlockSize:               int(tb.BytesToRead),
		Result:                  status,
		WorkerID:                workerID,
		ItemID:                  tb.BlockID,
		DuplicateOfBlockOrdinal: tb.DuplicateOfBlockOrdinal,
		Ordinal:                 tb.Ordinal,
		Offset:                  tb.Offset,
		SourceURI:               tb.SourceURI,
		NumberOfBlocks:          tb.NumberOfBlocks,
		TargetName:              tb.TargetAlias}

	c.resultQ <- wr

	return wr
}
