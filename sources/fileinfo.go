package sources

import (
	"fmt"
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

func (f *fileInfoProvider) newFileInfo(sourceURI string, fileStat os.FileInfo, alias string) (*FileInfo, error) {

	//directories are not allowed...
	if fileStat.IsDir() {
		return nil, nil
	}

	if fileStat.Size() == 0 {
		return nil, fmt.Errorf("Empty files are not allowed. The file %v is empty", fileStat.Name())
	}

	numOfBlocks := int(fileStat.Size()+int64(f.params.BlockSize-1)) / int(f.params.BlockSize)

	targetName := fileStat.Name()

	if f.params.KeepDirStructure {
		targetName = sourceURI
	}

	if alias != "" {
		targetName = alias
	}
	return &FileInfo{FileStats: fileStat, SourceURI: sourceURI, TargetAlias: targetName, NumOfBlocks: numOfBlocks}, nil
}

func (f *fileInfoProvider) walkPattern(pattern string, walkFunc func(path string, n int, info *FileInfo) (bool, error)) error {
	matches, err := filepath.Glob(pattern)

	if err != nil {
		return err
	}

	if len(matches) == 0 {
		return fmt.Errorf(" the pattern %v did not match any files", pattern)
	}

	include := true
	useAlias := len(f.params.SourcePatterns) == 1 && len(f.params.TargetAliases) == len(matches)
	n := 0
	for fi := 0; fi < len(matches); fi++ {
		fsinfo, err := os.Stat(matches[fi])

		if err != nil {
			return err
		}

		if f.params.Tracker != nil {
			var transferred bool
			if transferred, err = f.params.Tracker.IsTransferredAndTrackIfNot(matches[fi], fsinfo.Size()); err != nil {
				return err
			}

			include = !transferred
		}

		alias := ""

		if useAlias {
			alias = f.params.TargetAliases[fi]
		}

		if include {
			info, err := f.newFileInfo(matches[fi], fsinfo, alias)

			if err != nil {
				return err
			}

			if info != nil {

				stop, err := walkFunc(matches[fi], n, info)

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
