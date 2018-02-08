package targets

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

type fileHandleFactory struct {
	cacheSize      int
	maxCacheSize   int
	handleProvider handleProvider
	handleReq      chan factoryRequest
	closeReq       chan factoryCloseRequest
	returnReq      chan factoryReturnRequest
	fileHandles    map[string]*os.File
	overwrite      bool
}
type factoryCloseRequest struct {
	path string
	err  chan error
}
type factoryReturnRequest struct {
	path   string
	handle *os.File
}
type factoryRequest struct {
	path     string
	response chan factoryResponse
}

type factoryResponse struct {
	handle *os.File
	err    error
}

func (f *fileHandleFactory) startFactory() {

	go func() {
		for {
			select {
			case req, ok := <-f.handleReq:
				var err error

				if !ok {
					break
				}

				fh, exists := f.fileHandles[req.path]

				if !exists {
					responseChan := make(chan handleProviderResponse, 1)
					f.handleProvider.handleReq <- handleProviderRequest{path: req.path,
						response: responseChan}
					resp := <-responseChan
					fh = resp.handle
					err = resp.err
				} else {
					delete(f.fileHandles, req.path)
				}

				resp := factoryResponse{handle: fh, err: err}

				select {
				case req.response <- resp:
				default:
				}

			case ret := <-f.returnReq:
				var err error
				if f.cacheSize < f.maxCacheSize {
					f.fileHandles[ret.path] = ret.handle
					f.cacheSize++
				} else {
					err = ret.handle.Close()

					if err != nil {
						log.Fatalf("The handle failed close. Err:%v", err)
					}
				}

			case close := <-f.closeReq:
				var err error
				if fh, ok := f.fileHandles[close.path]; ok {

					err = fh.Close()
					delete(f.fileHandles, close.path)
					f.cacheSize--
				}

				select {
				case close.err <- err:
				default:
				}

			}
		}
	}()
}

type poolHandlerManager struct {
	factories []fileHandleFactory
	handleReq chan factoryRequest
	returnReq chan factoryReturnRequest
}

func newpoolHandlerManager(numOfHandlesPerFile int, numberOfHandlersInCache int, overwrite bool) *poolHandlerManager {
	factories := make([]fileHandleFactory, numOfHandlesPerFile)
	handleReq := make(chan factoryRequest, 100)
	returnReq := make(chan factoryReturnRequest, 100)
	handleProvider := newhandleProvider()
	handleProvider.startProvider(overwrite)

	for i := 0; i < numOfHandlesPerFile; i++ {
		//close request is unique for factory as handles need to close in all of them
		closeReq := make(chan factoryCloseRequest, 100)

		factory := fileHandleFactory{
			maxCacheSize:   numberOfHandlersInCache / numOfHandlesPerFile,
			overwrite:      overwrite,
			handleReq:      handleReq,
			returnReq:      returnReq,
			fileHandles:    make(map[string]*os.File),
			handleProvider: handleProvider,
			closeReq:       closeReq}

		factory.startFactory()

		factories[i] = factory
	}

	return &poolHandlerManager{factories: factories, handleReq: handleReq, returnReq: returnReq}
}
func (p *poolHandlerManager) getHandle(path string) (*os.File, error) {
	respChan := make(chan factoryResponse, 1)
	req := factoryRequest{path: path, response: respChan}

	p.handleReq <- req

	response := <-respChan

	return response.handle, response.err
}

func (p *poolHandlerManager) returnHandle(path string, fileHandle *os.File) error {
	retrequest := factoryReturnRequest{path: path, handle: fileHandle}
	p.returnReq <- retrequest
	return nil
}

func (p *poolHandlerManager) closeCacheHandles(path string) error {

	for _, factory := range p.factories {
		errch := make(chan error, 1)
		closerequest := factoryCloseRequest{path: path, err: errch}
		factory.closeReq <- closerequest
		err := <-errch

		if err != nil {
			return err
		}
	}
	return nil
}

type handleProvider struct {
	init      map[string]bool
	handleReq chan handleProviderRequest
}

type handleProviderRequest struct {
	path     string
	response chan handleProviderResponse
}

type handleProviderResponse struct {
	handle *os.File
	err    error
}

func newhandleProvider() handleProvider {
	reqChan := make(chan handleProviderRequest, 100)
	return handleProvider{
		init:      make(map[string]bool),
		handleReq: reqChan,
	}
}

func (h *handleProvider) startProvider(overwrite bool) {
	go func() {
		for {
			req, ok := <-h.handleReq

			if !ok {
				return
			}

			_, exists := h.init[req.path]
			var fh *os.File
			var err error
			if !exists {
				fh, err = h.initFile(req.path, overwrite)
			} else {
				fh, err = os.OpenFile(req.path, os.O_WRONLY, os.ModeAppend)
			}

			select {
			case req.response <- handleProviderResponse{handle: fh, err: err}:
			default:
			}

			h.init[req.path] = true

		}
	}()
}

func (h *handleProvider) initFile(filePath string, overwrite bool) (*os.File, error) {
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
