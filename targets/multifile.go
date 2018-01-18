package targets

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Azure/blobporter/util"

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
	fileHandlesMan *fileHandleManager
}

//NewMultiFile creates a new multi file target and 'n' number of handles for concurrent writes to a file.
func NewMultiFile(overwrite bool, numberOfHandles int) pipeline.TargetPipeline {

	//return &MultiFile{FileHandles: make(map[string]chan *os.File), NumberOfHandles: numberOfHandles, OverWrite: overwrite}
	fhm := newFileHandlerManager(numberOfHandles, true, overwrite)
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
	err = t.fileHandlesMan.closeCacheHandles(targetName)
	return
}

//ProcessWrittenPart implements ProcessWrittenPart from the pipeline.TargetPipeline interface.
//Passthrough implementation as no post-written-processing is required (e.g. maintain a list) when files are downloaded.
func (t *MultiFile) ProcessWrittenPart(result *pipeline.WorkerResult, listInfo *pipeline.TargetCommittedListInfo) (requeue bool, err error) {
	return false, nil
}

func (t *MultiFile) loadHandle(part *pipeline.Part) (*os.File, error) {
	return t.fileHandlesMan.getHandle(part.TargetAlias)
}

func (t *MultiFile) closeOrKeepHandle(part *pipeline.Part, fh *os.File) error {
	return t.fileHandlesMan.returnHandle(part.TargetAlias, fh)
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

type fileHandleManager struct {
	sync.Mutex
	fileHandlesQMap     sync.Map
	initTracker         sync.Map
	cachedFileHandles   int
	cacheEnabled        bool
	overwriteEnabled    bool
	numOfHandlesPerFile int
}

const maxFileHandlesInCache = 600

func newFileHandlerManager(numOfHandlesPerFile int, cacheEnabled bool, overwriteEnabled bool) *fileHandleManager {
	return &fileHandleManager{
		numOfHandlesPerFile: numOfHandlesPerFile,
		cacheEnabled:        cacheEnabled,
		overwriteEnabled:    overwriteEnabled}
}

func (h *fileHandleManager) getHandle(path string) (*os.File, error) {
	var fh *os.File
	var err error
	var isInit bool

	h.Lock()
	_, isInit = h.initTracker.Load(path)

	if !isInit {
		fh, err = h.initFile(path)
		if err != nil {
			return nil, err
		}
	}
	h.initTracker.Store(path, true)
	h.Unlock()

	if !h.cacheEnabled {
		//we don't have handle from the initialization, so open a new one.
		if fh != nil {
			if fh, err = os.OpenFile(path, os.O_WRONLY, os.ModeAppend); err != nil {
				return nil, err
			}
		}
		return fh, nil
	}

	//try to get it from the cache
	var fhQ chan *os.File
	var ok bool
	var val interface{}
	val, ok = h.fileHandlesQMap.Load(path)
	if ok {
		fhQ = val.(chan *os.File)
		fh := <-fhQ
		return fh, nil
	}

	//at this point we need to create a new handle and create the pool handles if not full if it hasn't be created.
	if fh, err = os.OpenFile(path, os.O_WRONLY, os.ModeAppend); err != nil {
		return nil, err
	}

	//the pool is full return the handle
	if h.cachedFileHandles+h.numOfHandlesPerFile > maxFileHandlesInCache {
		util.PrintfIfDebug("getHandle -> Cache is full: %v\n", h.cachedFileHandles+h.numOfHandlesPerFile)
		return fh, nil
	}

	//prime the pool with additional handles
	if !isInit {
		fhQ = make(chan *os.File, h.numOfHandlesPerFile)
		h.cachedFileHandles = h.cachedFileHandles + 1
		for i := 1; i < h.numOfHandlesPerFile; i++ {
			var fhi *os.File
			if fhi, err = os.OpenFile(path, os.O_WRONLY, os.ModeAppend); err != nil {
				return nil, err
			}
			fhQ <- fhi
			h.cachedFileHandles = h.cachedFileHandles + 1
		}

		h.fileHandlesQMap.Store(path, fhQ)
	}

	return fh, nil

}

func (h *fileHandleManager) returnHandle(path string, fileHandle *os.File) error {

	if !h.cacheEnabled {
		if fileHandle != nil {
			return fileHandle.Close()
		}
	}
	var fhq chan *os.File
	var val interface{}
	var ok bool

	val, ok = h.fileHandlesQMap.Load(path)
	if ok {
		fhq = val.(chan *os.File)
		fhq <- fileHandle
		h.fileHandlesQMap.Store(path, fhq)
		return nil
	}

	//not found in the map, which is the case when the cache is at capacity
	return fileHandle.Close()
}

func (h *fileHandleManager) closeCacheHandles(path string) error {

	val, ok := h.fileHandlesQMap.Load(path)
	if !ok {
		return nil
	}
	fhq := val.(chan *os.File)
	close(fhq)
	c := 0
	for {

		fh, ok := <-fhq

		if !ok {
			break
		}
		err := fh.Close()
		if err != nil {
			return err
		}
		c++
	}

	h.cachedFileHandles = h.cachedFileHandles - c
	h.fileHandlesQMap.Delete(path)

	return nil
}

//creates the directory structure if one doesn't exists. Creates a file checking existance while honoring the overwrite flag.
func (h *fileHandleManager) initFile(filePath string) (*os.File, error) {
	var fh *os.File
	var err error

	path := filepath.Dir(filePath)

	if path != "" {
		err = os.MkdirAll(path, 0777)

		if err != nil {
			return nil, err
		}
	}

	if _, err = os.Stat(filePath); os.IsExist(err) || !h.overwriteEnabled {
		return nil, fmt.Errorf("The file already exists and file overwrite is disabled")
	}

	if fh, err = os.Create(filePath); os.IsExist(err) {
		if err = os.Remove(filePath); err != nil {
			return nil, err
		}

		if fh, err = os.Create(filePath); err != nil {
			return nil, err
		}
	}

	return fh, nil
}
