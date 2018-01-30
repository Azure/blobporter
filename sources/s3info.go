package sources

import (
	"fmt"
	"log"
	"net/url"
	"path"
	"time"

	"github.com/minio/minio-go"

	"github.com/Azure/blobporter/pipeline"
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

//getSourceInfo gets a list of SourceInfo that represent the list of objects returned by the service
// based on the provided criteria (bucket/prefix). If the exact match flag is set, then a specific match is
// performed instead of the prefix. Marker semantics are also honored so a complete list is expected
func (s *s3InfoProvider) getSourceInfo() ([]pipeline.SourceInfo, error) {

	objLists, err := s.getObjectLists()
	if err != nil {
		return nil, err
	}

	exp := time.Duration(s.params.PreSignedExpMin) * time.Minute

	sourceURIs := make([]pipeline.SourceInfo, 0)
	for prefix, objList := range objLists {

		for _, obj := range objList {

			include := true

			if s.params.SourceParams.UseExactNameMatch {
				include = obj.Key == prefix
			}

			if include {

				var u *url.URL
				u, err = s.s3client.PresignedGetObject(s.params.Bucket, obj.Key, exp, url.Values{})

				if err != nil {
					return nil, err
				}

				targetAlias := obj.Key
				if !s.params.KeepDirStructure {
					targetAlias = path.Base(obj.Key)
				}
				
				sourceURIs = append(sourceURIs, pipeline.SourceInfo{
					SourceName:  u.String(),
					Size:        uint64(obj.Size),
					TargetAlias: targetAlias})

			}
		}
	}

	if len(sourceURIs) == 0 {
		nameMatchMode := "prefix"
		if s.params.UseExactNameMatch {
			nameMatchMode = "name"
		}
		return nil, fmt.Errorf(" the %v %s did not match any object key(s) ", nameMatchMode, s.params.Prefixes)
	}

	return sourceURIs, nil

}
func (s *s3InfoProvider) getObjectLists() (map[string][]minio.ObjectInfo, error) {
	listLength := 1

	if len(s.params.Prefixes) > 1 {
		listLength = len(s.params.Prefixes)
	}

	listOfLists := make(map[string][]minio.ObjectInfo, listLength)

	for _, prefix := range s.params.Prefixes {
		list := make([]minio.ObjectInfo, 0)

		// Create a done channel to control 'ListObjects' go routine.
		done := make(chan struct{})

		defer close(done)
		for object := range s.s3client.ListObjects(s.params.Bucket, prefix, true, done) {
			if object.Err != nil {
				return nil, object.Err
			}
			fmt.Printf("Object from s3 %+v\n", object)
			list = append(list, object)
		}

		listOfLists[prefix] = list
	}

	return listOfLists, nil

}
