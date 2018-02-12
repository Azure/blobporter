package internal

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

//There're two components here: poolHandle and the handle factory.
//A pool is an asynchronous request/response worker that runs on a single go-routine and keeps file handles for each file.
//The number of file handles is constraint by the max number of handlers in cache (maxFileHandlesInCache) and the max number of handles per file (numOfHandlesPerFile).
//When the max number handles is reached file handles will be closed until space is available. The handle factory opens the file handles and initializes the
//target file in case the folder structure and file need to be created. Since the factory tracks if a file has been initailized
//, i.e. created or truncated at the beginning of the transfer, only one instance of the factory is created.

const maxFileHandlesInCache int = 600

//HandleMode TODO
type HandleMode int

const (
	//Read read only file handles
	Read HandleMode = iota
	//Write write and append file handles
	Write
)

//FileHandlePool TODO
type FileHandlePool struct {
	maxCacheSize      int
	maxHandlesPerFile int
	factory           *handleFactory
	fileHandles       map[string][]*os.File
	overwrite         bool
	channels          poolChannels
	mode              HandleMode
}
type poolChannels struct {
	handleReq chan poolRequest
	closeReq  chan poolCloseRequest
	returnReq chan poolReturnRequest
}
type poolCloseRequest struct {
	path string
	err  chan error
}
type poolReturnRequest struct {
	path   string
	handle *os.File
}
type poolRequest struct {
	path     string
	response chan poolResponse
}

type poolResponse struct {
	handle *os.File
	err    error
}

//NewFileHandlePool TODO
func NewFileHandlePool(maxHandlesPerFile int, mode HandleMode, overwrite bool) *FileHandlePool {
	pool := FileHandlePool{
		maxCacheSize:      maxFileHandlesInCache,
		maxHandlesPerFile: maxHandlesPerFile,
		factory:           newhandleFactory(mode, overwrite),
		fileHandles:       make(map[string][]*os.File),
		overwrite:         overwrite,
		mode:              mode,
		channels: poolChannels{
			handleReq: make(chan poolRequest, 100),
			closeReq:  make(chan poolCloseRequest, 100),
			returnReq: make(chan poolReturnRequest, 100),
		},
	}

	pool.startPool()

	return &pool
}

//GetHandle TODO
func (f *FileHandlePool) GetHandle(path string) (*os.File, error) {
	respChan := make(chan poolResponse, 1)
	req := poolRequest{path: path, response: respChan}
	f.channels.handleReq <- req
	resp := <-respChan
	return resp.handle, resp.err
}

//ReturnHandle TODO
func (f *FileHandlePool) ReturnHandle(path string, handle *os.File) error {
	select {
	case f.channels.returnReq <- poolReturnRequest{handle: handle, path: path}:
	default:
		//close the handle if channel is full
		return handle.Close()
	}
	return nil
}

//CloseHandles TODO
func (f *FileHandlePool) CloseHandles(path string) error {
	respChan := make(chan error, 1)
	req := poolCloseRequest{path: path, err: respChan}
	f.channels.closeReq <- req
	err := <-respChan
	return err
}

const fileHandleCacheDebug = "BP_FHC_DBG"

func (f *FileHandlePool) startPool() {

	oc := 0 //num of open requests
	cc := 0 //num of close requests
	cm := 0 //num of cache misses
	ch := 0 //num of cache hits
	dbg := os.Getenv(fileHandleCacheDebug)
	go func() {
		for {

			if dbg == "1" {
				fmt.Printf("\rOpen Count:%v Close Count:%v FH:%v Hits:%v Misses:%v", oc, cc, len(f.fileHandles), ch, cm)
			}

			select {
			case req, ok := <-f.channels.handleReq:
				var err error
				var fh *os.File
				if !ok {
					break
				}

				fhq, exists := f.fileHandles[req.path]

				if !exists {
					//initialize the slice of file handles
					fhq = make([]*os.File, f.maxHandlesPerFile)
					fhq = fhq[0:0]
				}

				if len(fhq) == 0 {
					nfh, nerr := f.factory.getHandle(req.path)
					oc++
					if nerr != nil {
						resp := poolResponse{handle: nil, err: nerr}

						select {
						case req.response <- resp:
						default:
						}
						break
					}

					fhq = fhq[0:1]
					fhq[0] = nfh
					cm++
				} else {
					ch++
				}

				i := len(fhq) - 1
				fh = fhq[i]
				fhq = fhq[0:i]

				resp := poolResponse{handle: fh, err: err}

				select {
				case req.response <- resp:
				default:
				}

				//only persist the q already exists or if the max open of files is not reached
				if exists || (len(f.fileHandles)*f.maxHandlesPerFile < f.maxCacheSize) {
					f.fileHandles[req.path] = fhq
				}

			case ret := <-f.channels.returnReq:
				var err error

				if fhq, exists := f.fileHandles[ret.path]; exists {

					if len(fhq) < cap(fhq) {
						//increase len
						fhq = fhq[0 : len(fhq)+1]
						fhq[len(fhq)-1] = ret.handle
						f.fileHandles[ret.path] = fhq
						break
					}
				}
				err = ret.handle.Close()
				cc++

				if err != nil {
					log.Fatalf("The handle failed close. Err:%v", err)
				}

			case close := <-f.channels.closeReq:
				var err error
				if fhq, ok := f.fileHandles[close.path]; ok {
					for _, fh := range fhq {
						err = fh.Close()
						cc++
						if err != nil {
							break
						}
					}
					delete(f.fileHandles, close.path)
				}

				select {
				case close.err <- err:
				default:
				}
			}
		}
	}()
}

type handleFactory struct {
	init       map[string]bool
	factoryReq chan factoryRequest
	mode       HandleMode
}

type factoryRequest struct {
	path     string
	response chan factoryResponse
}

type factoryResponse struct {
	handle *os.File
	err    error
}

func newhandleFactory(mode HandleMode, overwrite bool) *handleFactory {
	reqChan := make(chan factoryRequest, 100)
	fact := handleFactory{
		init:       make(map[string]bool),
		factoryReq: reqChan,
		mode:       mode,
	}

	fact.startFactory(overwrite)

	return &fact
}
func (h *handleFactory) getHandle(path string) (*os.File, error) {
	responseChan := make(chan factoryResponse, 1)

	h.factoryReq <- factoryRequest{
		path:     path,
		response: responseChan}

	resp := <-responseChan

	return resp.handle, resp.err

}
func (h *handleFactory) startFactory(overwrite bool) {
	go func() {
		for {
			req, ok := <-h.factoryReq

			if !ok {
				return
			}

			_, exists := h.init[req.path]
			var fh *os.File
			var err error
			switch h.mode {
			case Read:
				fh, err = os.OpenFile(req.path, os.O_RDONLY, os.ModeAppend)
			case Write:
				if !exists {
					fh, err = h.initFile(req.path, overwrite)
				} else {
					fh, err = os.OpenFile(req.path, os.O_WRONLY, os.ModeAppend)
				}
			default:
				panic(fmt.Sprintf("Invalid handle mode:%v", h.mode))
			}

			select {
			case req.response <- factoryResponse{handle: fh, err: err}:
			default:
			}

			h.init[req.path] = true

		}
	}()
}

func (h *handleFactory) initFile(filePath string, overwrite bool) (*os.File, error) {
	var fh *os.File
	var err error

	path := filepath.Dir(filePath)

	if path != "" {
		err = os.MkdirAll(path, 0777)

		if err != nil {
			return nil, err
		}
	}

	if _, err = os.Stat(filePath); os.IsExist(err) || !overwrite {
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
