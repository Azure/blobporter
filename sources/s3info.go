package sources

import (
	"fmt"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/Azure/blobporter/pipeline"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

//S3Params parameters used to create a new instance of a S3 source pipeline
type S3Params struct {
	SourceParams
	Bucket           string
	Region           string
	Endpoint         string
	Prefixes         []string
	ExpNumberOfHours int
}

const defaultExpHours = 4

type s3InfoProvider struct {
	params *S3Params
	s3svc  *s3.S3
}

func newS3InfoProvider(params *S3Params) (*s3InfoProvider, error) {
	creds := credentials.NewEnvCredentials()

	_, err := creds.Get()

	if err != nil {
		return nil, err
	}

	var sess *session.Session

	sess, err = session.NewSession(&aws.Config{
		//Endpoint: aws.String(params.Endpoint),
		Region:      aws.String(params.Region),
		Credentials: creds})

	if err != nil {
		return nil, err
	}

	// Create S3 service client
	s3svc := s3.New(sess)

	return &s3InfoProvider{params: params, s3svc: s3svc}, nil
}

//getSourceInfo gets a list of SourceInfo that represent the list of objects returned by the service
// based on the provided criteria (bucket/prefix). If the exact match flag is set, then a specific match is
// performed instead of the prefix. Marker semantics are also honored so a complete list is expected
func (s *s3InfoProvider) getSourceInfo() ([]pipeline.SourceInfo, error) {
	var err error

	//wire up the param...
	//exp := 4

	var objLists []s3.ListObjectsOutput
	objLists, err = s.getObjectLists()
	if err != nil {
		return nil, err
	}

	sourceURIs := make([]pipeline.SourceInfo, 0)
	var sourceURI string
	for _, objList := range objLists {

		for _, obj := range objList.Contents {

			include := true
			if s.params.SourceParams.UseExactNameMatch {
				include = obj.Key == objList.Prefix
			}

			if include {
				request, _ := s.s3svc.GetObjectRequest(&s3.GetObjectInput{
					Bucket: aws.String(s.params.Bucket),
					Key:    obj.Key,
				})

				sourceURI, err = request.Presign(240 * time.Minute)

				if err != nil {
					return nil, err
				}

				targetAlias := *obj.Key
				if !s.params.KeepDirStructure {
					targetAlias = path.Base(*obj.Key)
				}

				sourceURIs = append(sourceURIs, pipeline.SourceInfo{
					SourceName:  sourceURI,
					Size:        uint64(*obj.Size),
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

func (s *s3InfoProvider) getObjectLists() ([]s3.ListObjectsOutput, error) {
	var err error
	listLength := 1

	if len(s.params.Prefixes) > 1 {
		listLength = len(s.params.Prefixes)
	}

	listOfLists := make([]s3.ListObjectsOutput, listLength)

	for i, prefix := range s.params.Prefixes {
		var list *s3.ListObjectsOutput
		input := &s3.ListObjectsInput{
			Bucket:  aws.String(s.params.Bucket),
			MaxKeys: aws.Int64(1000),
			Prefix:  aws.String(prefix)}

		for {

			var output *s3.ListObjectsOutput

			if output, err = s.s3svc.ListObjects(input); err != nil {
				return nil, err
			}
			if list == nil {
				list = output
			} else {
				(*list).Contents = append((*list).Contents, output.Contents...)
			}

			if *output.Marker == "" {
				break
			}

			input.Marker = output.NextMarker
		}

		listOfLists[i] = *list
	}

	return listOfLists, nil

}
