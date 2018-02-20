package util

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"net/url"
)

// Verbose mode active?
var Verbose = false

//BufferQCapacity number of pre-allocated buffers
const BufferQCapacity = 50

//LargeBlockSizeMax maximum block size
const LargeBlockSizeMax = 100 * MB

//LargeBlockAPIVersion API version that supports large block blobs
const LargeBlockAPIVersion = "2016-05-31"

//MiByte bytes in one MiB
const MiByte = 1048576

//MaxBlockCount the maximum number of blocks in a blob
const MaxBlockCount = 50000 // no more than this many blob blocks permitted

///////////////////////////////////////////////////////////////////
//  Storage sizes -- print and scan bytes, and sizes suffixed with KB,MB,GB,TB
//////////////////////////////////////////////////////////////////
const (
	KB = uint64(1024)
	MB = 1024 * KB
	GB = 1024 * MB
	TB = 1024 * GB

	KBF = float64(KB)
	MBF = float64(MB)
	GBF = float64(GB)
	TBF = float64(TB)
)

//PrintSize formats a string with a compact respresentation of a byte count
func PrintSize(bytes uint64) string {
	var str = "0"
	var zeroTrim = true
	var suffix = "GiB"

	if bytes > GB { // most common case
		str = fmt.Sprintf("%0.1f", float64(bytes)/GBF) //TODO: rounding
	} else if bytes <= KB {
		str = fmt.Sprintf("%d", bytes)
		suffix = "KiB"
	} else if bytes < MB {
		str = fmt.Sprintf("%d", bytes+KB-1)
		suffix = "B"
	} else { // if bytes < GB {
		str = fmt.Sprintf("%0.1f", float64(bytes)/MBF)
		suffix = "MiB"
	}

	if zeroTrim {
		if strings.HasSuffix(str, ".0") {
			str = str[:len(str)-2] // drop it
		}
	}

	return str + suffix + fmt.Sprintf(" (%v)", bytes)
}

// ByteCountFromSizeString accepts byte count, or integer suffixed with B, KB, MB, GB.
// ... Return the corresponding count of bytes.
func ByteCountFromSizeString(sizeStr string) (uint64, error) {
	sstr := strings.TrimSpace(sizeStr)
	var scaler uint64 = 1
	suffixCount := 2

	if strings.HasSuffix(sstr, "GB") {
		scaler = 1024 * 1024 * 1024
	} else if strings.HasSuffix(sstr, "MB") {
		scaler = 1024 * 1024
	} else if strings.HasSuffix(sstr, "KB") {
		scaler = 1024
	} else if strings.HasSuffix(sstr, "B") {
		suffixCount = 1
	} else {
		suffixCount = 0 // bare integer, no suffix
	}

	sstr = sstr[:len(sstr)-suffixCount] // drop suffix, if any
	res, err := strconv.ParseUint(sstr, 10, 64)
	if err != nil {
		res = 0
	}
	res = res * scaler
	return res, err
}

///////////////////////////////////////////////////////////////////
//  Flags-related Helpers -- processing command line options
///////////////////////////////////////////////////////////////////

// TODO: really should be processed according to Linux conventions where one character flags have a single dash and longer names have 2.
// ... e.g. -n vs. --name_space
// ... Go's flags package lets you define '--xyz' as an option, but fails to parse it correctly.  So current behavior is that -x and
// ... --x are treated equivalently.  This allows for the case of normal Linux conventions, but doesn't enforce it.

//StringVarAlias  string commandline option
func StringVarAlias(varPtr *string, shortflag string, longflag string, defaultVal string, description string) {
	flag.StringVar(varPtr, shortflag, defaultVal, description)
	flag.StringVar(varPtr, longflag, defaultVal, "")
}

//PrintUsageDefaults  print commandline usage options
func PrintUsageDefaults(shortflag string, longflag string, defaultVal string, description string) {
	defaultMsg := ""
	if defaultVal != "" {
		defaultMsg = fmt.Sprintf("\n\tDefault value: %v", defaultVal)
	}
	fmt.Fprintln(os.Stderr, fmt.Sprintf("-%v, --%v :\n\t%v%v", shortflag, longflag, description, defaultMsg))
}

//ListFlag represents a multivalue flag
type ListFlag []string

//String joints all values of the flag
func (lst *ListFlag) String() string {
	return strings.Join(*lst, " ")
}

//Set adds a new value to the values list
func (lst *ListFlag) Set(value string) error {
	*lst = append(*lst, value)
	return nil
}

//StringListVarAlias  string commandline option
func StringListVarAlias(varPtr *ListFlag, shortflag string, longflag string, defaultVal string, description string) {
	flag.Var(varPtr, shortflag, description)
	flag.Var(varPtr, longflag, "")
}

//IntVarAlias  int commandline option
func IntVarAlias(varPtr *int, shortflag string, longflag string, defaultVal int, description string) {
	flag.IntVar(varPtr, shortflag, defaultVal, description)
	flag.IntVar(varPtr, longflag, defaultVal, "")
}

//Uint64VarAlias  uint64 commandline option
func Uint64VarAlias(varPtr *uint64, shortflag string, longflag string, defaultVal uint64, description string) {
	flag.Uint64Var(varPtr, shortflag, defaultVal, description)
	flag.Uint64Var(varPtr, longflag, defaultVal, "")
}

//BoolVarAlias bool commandline option
func BoolVarAlias(varPtr *bool, shortflag string, longflag string, defaultVal bool, description string) {
	flag.BoolVar(varPtr, shortflag, defaultVal, description)
	flag.BoolVar(varPtr, longflag, defaultVal, "")
}

///////////////////////////////////////////////////////////////////
// Retriable execution of a function
///////////////////////////////////////////////////////////////////

const retryLimit = 100                            // max retries for an operation in retriableOperation
const retrySleepDuration = time.Millisecond * 200 // Retry wait interval in retriableOperation

//RetriableOperation executes a function, retrying up to "retryLimit" times and waiting "retrySleepDuration" between attempts
func RetriableOperation(operation func(r int) error) (duration time.Duration, startTime time.Time, numOfRetries int) {
	var err error
	var retries int
	t0 := time.Now()

	for {
		if retries >= retryLimit {
			fmt.Print("Max number of retries was exceeded.\n")

			if !Verbose {
				handleExceededRetries(err)
			} else {
				panic(err)
			}
		}
		if err = operation(retries); err == nil {
			t1 := time.Now()
			duration = t1.Sub(t0)
			startTime = t1
			numOfRetries = retries
			return
		}
		retries++

		PrintfIfDebug("RetriableOperation -> |%v|%v", retries, err)

		time.Sleep(retrySleepDuration)
	}
}

//AskUser places a yes/no question to the user provided by the stdin
func AskUser(question string) bool {
	fmt.Printf(question)
	for {
		var input string
		n, err := fmt.Scanln(&input)
		if n < 1 || err != nil {
			fmt.Println("invalid input")
		}
		input = strings.ToLower(input)
		switch input {
		case "y":
			return true
		case "n":
			return false
		default:
			fmt.Printf("Invalid response.\n")
			fmt.Printf(question)
		}
	}
}

//isValidContainerName is true if the name of the container is valid, false if not
func isValidContainerName(name string) bool {
	if len(name) < 3 {
		return false
	}
	expr := "^[a-z0-9]+([-]?[a-z0-9]){1,63}$"
	valid := regexp.MustCompile(expr)
	resp := valid.MatchString(name)
	if !resp {
		fmt.Printf("The name provided for the container is invalid, it must conform the following rules:\n")
		fmt.Printf("1. Container names must start with a letter or number, and can contain only letters, numbers, and the dash (-) character.\n")
		fmt.Printf("2. Every dash (-) character must be immediately preceded and followed by a letter or number; consecutive dashes are not permitted in container names.\n")
		fmt.Printf("3. All letters in a container name must be lowercase.\n")
		fmt.Printf("4. Container names must be from 3 through 63 characters long.\n")
	}
	return resp
}

func handleExceededRetries(err error) {
	errMsg := fmt.Sprintf("The number of retries has exceeded the maximum allowed.\nError: %v\nSuggestion:%v\n", err.Error(), getSuggestion(err))
	log.Fatal(errMsg)
}
func getSuggestion(err error) string {
	switch {
	case strings.Contains(err.Error(), "ErrorMessage=The specified blob or block content is invalid"):
		return "Try using a different container or upload and then delete a small blob with the same name."
	case strings.Contains(err.Error(), "Client.Timeout"):
		return "Try increasing the timeout using the -s option or reducing the number of workers and readers, options: -r and -g"
	case strings.Contains(err.Error(), "too many open files"):
		return "Try reducing the number of sources or batch size"
	default:
		return ""
	}
}

//PrintfIfDebug TODO
func PrintfIfDebug(format string, values ...interface{}) {
	if Verbose {
		msg := fmt.Sprintf(format, values...)
		fmt.Printf("%v\t%s\n", time.Now(), msg)
	}
}

//GetFileNameFromURL returns last part of URL (filename)
func GetFileNameFromURL(sourceURI string) (string, error) {

	purl, err := url.Parse(sourceURI)

	if err != nil {
		return "", err
	}

	parts := strings.Split(purl.Path, "/")

	if len(parts) == 0 {
		return "", fmt.Errorf("Invalid URL file was not found in the path")
	}

	return parts[len(parts)-1], nil
}
