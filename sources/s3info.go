package sources

import (
	"log"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/Azure/blobporter/internal"
	"github.com/Azure/blobporter/pipeline"
	"github.com/minio/minio-go"
)

//S3Params parameters used to create a new instance of a S3 source pipeline
type S3Params struct {
	SourceParams
	Bucket          string
	Endpoint        string
	Prefixes        []string
	PreSignedExpMin int
	AccessKey       string
	SecretKey       string
}

const defaultExpHours = 4

type s3InfoProvider struct {
	params   *S3Params
	s3client *minio.Client
}

func newS3InfoProvider(params *S3Params) (*s3InfoProvider, error) {
	s3client, err := minio.New(params.Endpoint, params.AccessKey, params.SecretKey, true)

	if err != nil {
		log.Fatalln(err)
	}

	return &s3InfoProvider{params: params, s3client: s3client}, nil

}

func (s *s3InfoProvider) toSourceInfo(obj *minio.ObjectInfo) (*pipeline.SourceInfo, error) {
	exp := time.Duration(s.params.PreSignedExpMin) * time.Minute

	u, err := s.s3client.PresignedGetObject(s.params.Bucket, obj.Key, exp, url.Values{})

	if err != nil {
		return nil, err
	}

	targetAlias := obj.Key
	if !s.params.KeepDirStructure {
		targetAlias = path.Base(obj.Key)
	}

	return &pipeline.SourceInfo{
		SourceName:  u.String(),
		Size:        uint64(obj.Size),
		TargetAlias: targetAlias}, nil

}

func (s *s3InfoProvider) listObjects(filter internal.SourceFilter) <-chan ObjectListingResult {
	sources := make(chan ObjectListingResult, 2)
	go func() {
		list := make([]pipeline.SourceInfo, 0)
		bsize := 0

		for _, prefix := range s.params.Prefixes {
			// Create a done channel to control 'ListObjects' go routine.
			done := make(chan struct{})
			defer close(done)
			for object := range s.s3client.ListObjects(s.params.Bucket, prefix, true, done) {
				if object.Err != nil {
					sources <- ObjectListingResult{Err: object.Err}
					return
				}
				include := true

				if s.params.SourceParams.UseExactNameMatch {
					include = object.Key == prefix
				}

				isfolder := strings.HasSuffix(object.Key, "/") && object.Size == 0

				transferred, err := filter.IsTransferredAndTrackIfNot(object.Key, object.Size)

				if err != nil {
					sources <- ObjectListingResult{Err: err}
					return
				}

				if include && !isfolder && !transferred {
					si, err := s.toSourceInfo(&object)

					if err != nil {
						sources <- ObjectListingResult{Err: err}
						return
					}
					list = append(list, *si)

					if bsize++; bsize == s.params.FilesPerPipeline {
						sources <- ObjectListingResult{Sources: list}
						list = make([]pipeline.SourceInfo, 0)
						bsize = 0
					}
				}

			}

			if bsize > 0 {
				sources <- ObjectListingResult{Sources: list}
				list = make([]pipeline.SourceInfo, 0)
				bsize = 0
			}
		}
		close(sources)
	}()

	return sources
}
