package util

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

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Verbose mode active?
var Verbose = false

//MiByte bytes in one MiB
const MiByte = 1048576
const MaxBlockCount = 50000 // no more than this many blob blocks permitted

const scriptVersion = "2016.11.09.A" // version number to show in help
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

//printSize - format a string with a compact respresentation of a byte count
func PrintSize(bytes uint64) string {
	var str = "0"
	var zeroTrim = true
	var suffix = "GB"

	if bytes > GB { // most common case
		str = fmt.Sprintf("%0.1f", float64(bytes)/GBF) //TODO: rounding
	} else if bytes <= KB {
		str = fmt.Sprintf("%d", bytes)
		suffix = "KB"
	} else if bytes < MB {
		str = fmt.Sprintf("%d", bytes+KB-1)
		suffix = "B"
	} else { // if bytes < GB {
		str = fmt.Sprintf("%0.1f", float64(bytes)/MBF)
		suffix = "MB"
	}

	if zeroTrim {
		if strings.HasSuffix(str, ".0") {
			str = str[:len(str)-2] // drop it
		}
	}

	return str + suffix + fmt.Sprintf(" (%v)", bytes)
}

// ByteCountFromSizeString - accept byte count, or integer suffixed with B, KB, MB, GB.
// ... Return the corresponding count of bytes.
func ByteCountFromSizeString(sizeStr string) (uint64, error) {
	sstr := strings.TrimSpace(sizeStr)
	var scaler uint64 = 1
	var suffixCount int = 2

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

// StringVarAlias - string commandline option
func StringVarAlias(varPtr *string, shortflag string, longflag string, defaultVal string, description string) {
	flag.StringVar(varPtr, shortflag, defaultVal, description)
	flag.StringVar(varPtr, longflag, defaultVal, description+" [Same as -"+shortflag+"]")
}

// IntVarAlias - int commandline option
func IntVarAlias(varPtr *int, shortflag string, longflag string, defaultVal int, description string) {
	flag.IntVar(varPtr, shortflag, defaultVal, description)
	flag.IntVar(varPtr, longflag, defaultVal, description+" [Same as -"+shortflag+"]")
}

// Uint64VarAlias - uint64 commandline option
func Uint64VarAlias(varPtr *uint64, shortflag string, longflag string, defaultVal uint64, description string) {
	flag.Uint64Var(varPtr, shortflag, defaultVal, description)
	flag.Uint64Var(varPtr, longflag, defaultVal, description+" [Same as -"+shortflag+"]")
}

// BoolVarAlias - Boolean commandline option
func BoolVarAlias(varPtr *bool, shortflag string, longflag string, defaultVal bool, description string) {
	flag.BoolVar(varPtr, shortflag, defaultVal, description)
	flag.BoolVar(varPtr, longflag, defaultVal, description+" [Same as -"+shortflag+"]")
}

///////////////////////////////////////////////////////////////////
// Retriable execution of a function -- used for Azure Storage requests
///////////////////////////////////////////////////////////////////

const retryLimit = 20                             // max retries for an operation in retriableOperation
const retrySleepDuration = time.Millisecond * 200 // Retry wait interval in retriableOperation

// RetriableOperation - execute the function, retrying up to "retryLimit" times
func RetriableOperation(operation func(r int) error) {
	var err error
	var retries int

	for {
		if retries >= retryLimit {
			fmt.Print("Max number of retries exceeded.")
			panic(err)
		}

		if err = operation(retries); err == nil {
			return
		}
		retries++

		if Verbose {
			fmt.Printf(" R %v ", retries)
			fmt.Println(err.Error())
		}
		time.Sleep(retrySleepDuration)
	}
}

///////////////////////////////////////////////////////////////////
