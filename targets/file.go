package targets

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Azure/blobporter/pipeline"
)

// blobporter Tool
//
// Copyright (c) Microsoft Corporation
//
// All rights reserved.
//
// MIT License
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.
//

////////////////////////////////////////////////////////////
///// File Target
////////////////////////////////////////////////////////////

//File TODO
type File struct {
	Creds          *pipeline.StorageAccountCredentials
	Container      string
	TargetFileName string
	FileHandles    *chan *os.File
	FileStat       *os.FileInfo
}

//NewFile TODO
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

	return File{FileHandles: &fhQ, FileStat: &fileStat, TargetFileName: targetFileName}
}

//CommitList TODO
func (t File) CommitList(listInfo *pipeline.TargetCommittedListInfo, numberOfBlocks int, targetName string) (msg string, err error) {

	//Close all file handles..
	close((*t.FileHandles))

	for {

		fh, ok := <-(*t.FileHandles)

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

//ProcessWrittenPart TODO
func (t File) ProcessWrittenPart(result *pipeline.WorkerResult, listInfo *pipeline.TargetCommittedListInfo) (requeue bool, err error) {
	return false, nil
}

//WritePart TODO
func (t File) WritePart(part *pipeline.Part) (duration time.Duration, startTime time.Time, numOfRetries int, err error) {
	startTime = time.Now()
	fh := <-(*t.FileHandles)
	if _, err := fh.WriteAt((*part).Data, int64((*part).Offset)); err != nil {
		log.Fatal(err)
	}
	duration = time.Now().Sub(startTime)

	(*part).ReturnBuffer()

	(*t.FileHandles) <- fh

	return
}
