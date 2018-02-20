package internal

import (
	"fmt"
	"runtime"
)

//ProgramVersion blobporter version
const ProgramVersion = "0.6.09"

//HTTPClientTimeout  HTTP client timeout when reading from HTTP sources and try timeout for blob storage operations.
var HTTPClientTimeout = 90

//UserAgentStr value included in the user-agent header when calling the blob API
var UserAgentStr = fmt.Sprintf("%s/%s/%s", "BlobPorter", ProgramVersion, runtime.GOARCH)
