# BlobPorter
Parallel blob copier.

[![Build Status](https://travis-ci.com/Azure/blobporter?branch=master)](https://travis-ci.com/Azure/blobporter)

## Introduction

Getting Started

Installation:

Linux

Windows


## Examples:
Single file upload to block blob storage.

`./blobporter -f /datadrive/myfile.tar -c mycontainer`

Download from blob storage.

`./blobporter -f mydownloadedfile.tar -c mycontainer -n /datadrive/myfile.tar -t blob-file`

Multi file upload – upload all files that match the pattern.

`./blobporter -f "/datadrive/*.tar" -c mycontainer`

Transfer from a HTTP source to block blob storage.

`./blobporter -f "http://mysource/file.bam"  -c mycontainer -n file.bam -t http-block`


Download a HTTP source, the container option is not required.

`./blobporter -f "http://mysource/file.bam"  -n /datadrive/file.bam -t http-file`


# Command Options

- `-a` string  
  `--account_name` string  
Storage account name (e.g. mystorage). Can also be specified via the ACCOUNT_NAME environment variable.

- `-b` string  
`--block_size` string  
Desired size of each blob block. 
Can be specified an integer byte count or integer suffixed with B, KB, MB, or GB (default "4MB").

- `-c` string  
`--container_name` string  
Container name (e.g. `mycontainer`)

- `-d` string  
`--dup_check_level` string    
Desired level of effort to detect duplicate data blocks to minimize upload size.
Must be one of None, ZeroOnly, Full (default "None")

- `-f` *string*  
`--file` string
URL, file or files (e.g. /data/*.gz) to upload. \nDestination file for download.


- `-g` int  
`--concurrent_workers` int
Number of threads for parallel upload

- `-k` string  
`--account_key` string
Storage account key string
(e.g. `4Rr8CpUM9Y/3k/SqGSr/oZcLo3zNU6aIo32NVzda4EJj0hjS2Jp7NVLAD3sFp7C67z/i7Rfbrpu5VHgcmOShTg==`).
Can also be specified via the ACCOUNT_KEY environment variable.

- `-n` string
`--blob_name` string
Blob name (e.g. myblob.txt)

- `-r` int
`--concurrent_readers` int
Number of threads for parallel reading of the input file

- `-v`  
`--verbose `
Display verbose output.

# Contributors
- Jesus Aguilar
- Shawn Elliott
