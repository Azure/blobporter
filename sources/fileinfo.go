package sources

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

type fileInfoProvider struct {
	params *FileSystemSourceParams
}

type fileInfoResponse struct {
	fileInfos        map[string]FileInfo
	totalNumOfBlocks int64
	totalSize        int64
	err              error
}

//FileInfo Contains the metadata associated with a file to be transferred
type FileInfo struct {
	FileStats   os.FileInfo
	SourceURI   string
	TargetAlias string
	NumOfBlocks int
}

func newfileInfoProvider(params *FileSystemSourceParams) *fileInfoProvider {
	return &fileInfoProvider{params: params}
}

func (f *fileInfoProvider) listSourcesInfo() <-chan fileInfoResponse {
	ret := make(chan fileInfoResponse, 1)
	finfos := make(map[string]FileInfo)
	var tblocks int64
	var tsize int64

	walkFunc := func(path string, n int, info *FileInfo) (bool, error) {
		tblocks += int64(info.NumOfBlocks)
		tsize += info.FileStats.Size()
		finfos[info.SourceURI] = *info
		if (n+1)%f.params.FilesPerPipeline == 0 { //n counter is zero based
			ret <- fileInfoResponse{fileInfos: finfos, totalNumOfBlocks: tblocks, totalSize: tsize}
			finfos = make(map[string]FileInfo)
			tblocks = 0
			tsize = 0
		}

		return false, nil
	}

	go func() {
		defer close(ret)
		for _, pattern := range f.params.SourcePatterns {
			if err := f.walkPattern(pattern, walkFunc); err != nil {
				ret <- fileInfoResponse{err: err}
				break
			}
		}

		if len(finfos) > 0 {
			ret <- fileInfoResponse{fileInfos: finfos, totalNumOfBlocks: tblocks, totalSize: tsize}
		}
	}()

	return ret
}

//newFileInfoForTransfer if the file must be included in the transfer, i.e. if the transfer is not using a tracker
//or tracker indicates that it hasn't been transferred, the method will return a FileInfo.
func (f *fileInfoProvider) newFileInfoForTransfer(sourceURI string, fileStat os.FileInfo, alias string) (*FileInfo, error) {

	//directories are not allowed...
	if fileStat.IsDir() {
		return nil, nil
	}

	if include, err := f.includeInTransfer(sourceURI, fileStat.Size()); err != nil || !include {
		return nil, err
	}

	numOfBlocks := int(fileStat.Size()+int64(f.params.BlockSize-1)) / int(f.params.BlockSize)

	if numOfBlocks == 0 {
		numOfBlocks = 1
	}

	targetName := fileStat.Name()

	if f.params.KeepDirStructure {
		targetName = sourceURI
	}

	if alias != "" {
		targetName = alias
	}
	return &FileInfo{FileStats: fileStat, SourceURI: sourceURI, TargetAlias: targetName, NumOfBlocks: numOfBlocks}, nil
}

func (f *fileInfoProvider) fileInfoRecursive(fileName string) ([]string, []os.FileInfo, error) {

	fsstat, err := os.Stat(fileName)

	if err != nil {
		return nil, nil, err
	}

	names := make([]string, 1)
	finfos := make([]os.FileInfo, 1)

	finfos[0] = fsstat
	names[0] = fileName

	if !fsstat.IsDir() {
		return names, finfos, nil
	}

	children, err := ioutil.ReadDir(fileName)

	if err != nil {
		return nil, nil, err
	}

	for fi := 0; fi < len(children); fi++ {
		childName, childStat, err := f.fileInfoRecursive(filepath.Join(fileName, children[fi].Name()))

		if err != nil {
			return nil, nil, err
		}

		finfos = append(finfos, childStat...)

		names = append(names, childName...)
	}

	return names, finfos, nil
}
func (f *fileInfoProvider) includeInTransfer(name string, size int64) (bool, error) {
	var err error
	var transferred bool

	if f.params.Tracker != nil {

		if transferred, err = f.params.Tracker.IsTransferredAndTrackIfNot(name, size); err != nil {
			return false, err
		}
	}

	return !transferred, nil
}
func (f *fileInfoProvider) walkPattern(pattern string, walkFunc func(path string, n int, info *FileInfo) (bool, error)) error {
	matches, err := filepath.Glob(pattern)

	if err != nil {
		return err
	}

	if len(matches) == 0 {
		return fmt.Errorf(" the pattern %v did not match any files", pattern)
	}

	useAlias := len(f.params.SourcePatterns) == 1 && len(f.params.TargetAliases) == len(matches)
	n := 0
	for m := 0; m < len(matches); m++ {

		names, fsinfos, err := f.fileInfoRecursive(matches[m])

		if err != nil {
			return err
		}

		for fi := 0; fi < len(fsinfos); fi++ {

			alias := ""

			if useAlias && n < len(f.params.TargetAliases) {
				alias = f.params.TargetAliases[n]
			}

			info, err := f.newFileInfoForTransfer(names[fi], fsinfos[fi], alias)

			if err != nil {
				return err
			}

			if info != nil {

				stop, err := walkFunc(names[fi], n, info)

				if err != nil {
					return err
				}
				n++
				if stop {
					return nil
				}
			}

		}

	}

	return nil
}
