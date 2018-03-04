package internal

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type fileStatus struct {
	sourcename string
	size       int64
	source     string
	status     TransferStatus
	tid        string
}

//TransferStatus TODO
type TransferStatus int

const (
	//None No status avaiable.
	None = iota
	//Started Indicates that the file was included in the transfer.
	Started
	//Completed Indicates that the file was transferred succesfully.
	Completed
	//Ignored Indicates that the file was ignored - e.g. empty files...
	Ignored
)

func (t TransferStatus) String() string {
	switch t {
	case None:
		return "0"
	case Started:
		return "1"
	case Completed:
		return "2"
	case Ignored:
		return "3"
	}

	return ""
}

func parseTransferStatus(val string) (TransferStatus, error) {
	var ts int
	var err error
	if ts, err = strconv.Atoi(val); err != nil {
		return None, fmt.Errorf("Invalid transfer status, %v, err:%v", val, err)
	}
	switch ts {
	case None:
		return None, nil
	case Completed:
		return Completed, nil
	case Started:
		return Started, nil
	case Ignored:
		return Ignored, nil
	}
	return None, fmt.Errorf("Invalid transfer status, %v", val)
}

func newLogEntry(line string) (*fileStatus, error) {
	f := fileStatus{}

	if line == summaryheader {
		return nil, fmt.Errorf("Invalid transfer status file. The file contains information from a completed transfer.\nUse a new file name")
	}

	tokens := strings.Split(line, "\t")

	if len(tokens) < 5 {
		return nil, fmt.Errorf("Invalid log entry. Less than 5 tokens were found. Value:%v ", line)
	}

	f.sourcename = tokens[1]
	var size int64
	var err error
	if size, err = strconv.ParseInt(tokens[3], 10, 64); err != nil {
		return nil, err
	}

	f.size = size
	var status TransferStatus

	status, err = parseTransferStatus(tokens[2])

	f.status = status

	f.tid = tokens[4]

	if err != nil {
		return nil, err
	}

	return &f, nil
}

//Timestamp, sourcename, status, size, id
const formatString = "%s\t%s\t%s\t%v\t%s\t"

func (f *fileStatus) toLogEntry() string {
	return fmt.Sprintf(formatString, time.Now().Format(time.RFC3339Nano), f.sourcename, f.status, f.size, f.tid)
}

func (f *fileStatus) key() string {
	return fmt.Sprintf("%s%v%v", f.sourcename, f.size, f.status)
}

type transferCompletedRequest struct {
	duration time.Duration
	response chan error
}

type fileTransferredRequest struct {
	fileName string
	response chan error
}

type isInLogRequest struct {
	fileName string
	size     int64
	status   TransferStatus
	response chan isInLogResponse
}
type isInLogResponse struct {
	inlog bool
	err   error
}

//TransferTracker TODO
type TransferTracker struct {
	id                 string
	loghandle          *os.File
	restoredStatus     map[string]fileStatus
	currentStatus      map[string]fileStatus
	isInLogReq         chan isInLogRequest
	fileTransferredReq chan fileTransferredRequest
	complete           chan error
	//transferCompletedReq chan transferCompletedRequest
}

//NewTransferTracker TODO
func NewTransferTracker(logPath string) (*TransferTracker, error) {
	var loghandle *os.File
	var err error
	load := true

	if loghandle, err = os.OpenFile(logPath, os.O_APPEND|os.O_RDWR, os.ModePerm); err != nil {
		if os.IsNotExist(err) {
			if loghandle, err = os.Create(logPath); err != nil {
				return nil, err
			}
			load = false
		} else {
			return nil, err
		}
	}

	tt := TransferTracker{loghandle: loghandle,
		restoredStatus:     make(map[string]fileStatus),
		currentStatus:      make(map[string]fileStatus),
		isInLogReq:         make(chan isInLogRequest, 500),
		fileTransferredReq: make(chan fileTransferredRequest, 500),
		complete:           make(chan error),
		id:                 fmt.Sprintf("%v_%s", time.Now().Nanosecond(), logPath),
	}
	if load {
		err = tt.load()
		if err != nil {
			return nil, err
		}
	}
	tt.startTracker()

	return &tt, err
}

//IsTransferredAndTrackIfNot returns true if the file was previously transferred. If not, track/log that the transferred
//was started.
func (t *TransferTracker) IsTransferredAndTrackIfNot(name string, size int64) (bool, error) {
	req := isInLogRequest{fileName: name,
		size: size, status: Completed,
		response: make(chan isInLogResponse),
	}
	t.isInLogReq <- req

	resp := <-req.response

	return resp.inlog, resp.err
}

//TrackFileTransferComplete TODO
func (t *TransferTracker) TrackFileTransferComplete(name string) error {
	req := fileTransferredRequest{fileName: name,
		response: make(chan error),
	}
	t.fileTransferredReq <- req

	return <-req.response
}

//TrackTransferComplete TODO
func (t *TransferTracker) TrackTransferComplete() error {
	t.closeChannels()
	return <-t.complete
}

func (t *TransferTracker) isInLog(name string, size int64, status TransferStatus) bool {
	fs := fileStatus{sourcename: name, size: size, status: status}

	_, ok := t.restoredStatus[fs.key()]

	return ok
}

func (t *TransferTracker) load() error {
	scanner := bufio.NewScanner(t.loghandle)
	for scanner.Scan() {
		line := scanner.Text()
		s, err := newLogEntry(line)

		if err != nil {
			return err
		}

		t.restoredStatus[s.key()] = *s
	}

	return scanner.Err()
}

const summaryheader = "----------------------------------------------------------"

func (t *TransferTracker) writeSummary() error {
	t.loghandle.Write([]byte(fmt.Sprintf("%v\n", summaryheader)))
	t.loghandle.Write([]byte(fmt.Sprintf("Transfer Completed----------------------------------------\n")))
	t.loghandle.Write([]byte(fmt.Sprintf("Start Summary---------------------------------------------\n")))
	t.loghandle.Write([]byte(fmt.Sprintf("Last Transfer ID:%s\n", t.id)))
	t.loghandle.Write([]byte(fmt.Sprintf("Date:%v\n", time.Now().Format(time.UnixDate))))

	tsize, f, err := t.writeSummaryEntries(t.currentStatus)
	if err != nil {
		return err
	}
	tsize2, f2, err := t.writeSummaryEntries(t.restoredStatus)
	if err != nil {
		return err
	}

	t.loghandle.Write([]byte(fmt.Sprintf("Transferred Files:%v\tTotal Size:%d\n", f+f2, tsize+tsize2)))
	t.loghandle.Write([]byte(fmt.Sprintf("End Summary-----------------------------------------------\n")))

	return t.loghandle.Close()
}
func (t *TransferTracker) writeSummaryEntries(entries map[string]fileStatus) (size int64, n int, err error) {
	for _, entry := range entries {
		if entry.status == Completed {
			size = size + entry.size
			_, err = t.loghandle.Write([]byte(fmt.Sprintf("File:%s\tSize:%v\tTID:%s\n", entry.sourcename, entry.size, entry.tid)))
			if err != nil {
				return
			}
			n++
		}
	}

	return
}
func (t *TransferTracker) writeStartedEntry(name string, size int64) error {

	var status TransferStatus = Started
	if size == 0 {
		status = Ignored
	}

	fs := fileStatus{sourcename: name, size: size, status: status, tid: t.id}
	t.currentStatus[name] = fs

	line := fmt.Sprintf("%v\n", fs.toLogEntry())
	_, err := t.loghandle.Write([]byte(line))
	return err
}

func (t *TransferTracker) writeCompleteEntry(name string) error {
	fs, ok := t.currentStatus[name]

	if !ok {
		return fmt.Errorf("The current status tracker is not consistent. Started entry was not found")
	}

	fs.status = Completed

	line := fmt.Sprintf("%v\n", fs.toLogEntry())
	_, err := t.loghandle.Write([]byte(line))
	t.currentStatus[name] = fs
	return err
}

func (t *TransferTracker) startTracker() {

	go func() {
		for {
			select {
			case incReq, ok := <-t.isInLogReq:
				if !ok {
					break
				}

				inlog := t.isInLog(incReq.fileName, incReq.size, Completed)
				resp := isInLogResponse{inlog: inlog}
				if !inlog {
					resp.err = t.writeStartedEntry(incReq.fileName, incReq.size)
				}

				incReq.response <- resp
			case ftReq, ok := <-t.fileTransferredReq:
				if !ok {
					t.complete <- t.writeSummary()
					return
				}
				ftReq.response <- t.writeCompleteEntry(ftReq.fileName)
			}
		}
	}()
}

func (t *TransferTracker) closeChannels() {
	close(t.isInLogReq)
	close(t.fileTransferredReq)
}

//SourceFilter TODO
type SourceFilter interface {
	IsTransferredAndTrackIfNot(name string, size int64) (bool, error)
}
