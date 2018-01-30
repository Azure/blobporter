# BlobPorter

[![Build Status](https://travis-ci.org/Azure/blobporter.svg?branch=master)](https://travis-ci.org/Azure/blobporter)
[![Go Report Card](https://goreportcard.com/badge/github.com/Azure/blobporter)](https://goreportcard.com/report/github.com/Azure/blobporter)

## Introduction

BlobPorter is a data transfer tool for Azure Blob Storage that maximizes throughput through concurrent reads and writes that can scale up and down independently.

![](img/bptransfer.png?raw=true)

Sources and targets are decoupled, this design enables the composition of various transfer scenarios.

| From/To          | Azure Block Blob | Azure Page Blob  | File (Download) |
| ---------------  | -----------------| -----------------|-----------------|
| File (Upload)    | Yes              | Yes              | NA              |
| HTTP/HTTPS*      | Yes              | Yes              | Yes             |
| Azure Block Blob | Yes              | Yes              | Yes             |
| Azure Page Blob  | Yes              | Yes              | Yes             |
| S3 Endpoint      | Yes              | Yes              | No              |

*\*   The HTTP/HTTPS source must support HTTP byte ranges and return the file size as a response to a HTTP HEAD request.*


## Getting Started

### Linux

Download, extract and set permissions:

```bash
wget -O bp_linux.tar.gz https://github.com/Azure/blobporter/releases/download/v0.5.25/bp_linux.tar.gz
tar -xvf bp_linux.tar.gz linux_amd64/blobporter
chmod +x ~/linux_amd64/blobporter
cd ~/linux_amd64
```

Set environment variables:

```bash
export ACCOUNT_NAME=<STORAGE_ACCOUNT_NAME>
export ACCOUNT_KEY=<STORAGE_ACCOUNT_KEY>
```

>Note: You can also set these values via options

### Windows

Download [BlobPorter.exe](https://github.com/Azure/blobporter/releases/download/v0.5.25/bp_windows.zip)

Set environment variables (if using the command prompt):

```batchfile
set ACCOUNT_NAME=<STORAGE_ACCOUNT_NAME>
set ACCOUNT_KEY=<STORAGE_ACCOUNT_KEY>
```

Set environment variables (if using PowerShell):

```PowerShell
$env:ACCOUNT_NAME="<STORAGE_ACCOUNT_NAME>"
$env:ACCOUNT_KEY="<STORAGE_ACCOUNT_KEY>"
```

## Examples

### Upload to Azure Block Blob Storage

Single file upload:

`./blobporter -f /datadrive/myfile.tar -c mycontainer -n myfile.tar`

>Note: If the container does not exist, it will be created.

Upload all files that match the pattern:

`./blobporter -f "/datadrive/*.tar" -c mycontainer`

You can also specify a list of files or patterns explicitly:

`./blobporter -f "/datadrive/*.tar" -f "/datadrive/readme.md" -f "/datadrive/log" -c mycontainer`

If you want to rename multiple files, you can use the -n option:

`./blobporter -f /datadrive/f1.tar -f /datadrive/f2.md -n b1 -n b2 -c mycontainer`

### Upload to Azure Page Blob Storage

Same as uploading to block blob storage, but with the transfer definiton (-t option) set to file-pageblob.

For example, a single file upload to page blob:

`./blobporter -f /datadrive/mydisk.vhd -c mycontainer -n mydisk.vhd -t file-pageblob`

>Note: The file size and block size must be a multiple of 512 (bytes). The maximum block size is 4MB.

### Upload from an S3 compatible endpoint to Azure Blob Storage

You can upload data directly from an S3 compatible endpoint. First you must specify the access and secret keys via environment variables.

```bash
export S3_ACCESS_KEY=<YOUR ACCESS KEY>
export S3_SECRET_KEY=<YOUR_SECRET_KEY>
```

Then you can specify an S3 URI, with the following format:

[HOST]/[BUCKET]/[PREFIX]

For example:

`./blobporter -f s3://mys3api.com/mybucket/mydata -c froms3 -t s3-blockblob -p`

>Note: For better performance, consider running this tranfer from a VM running in the same region as source or the target. Data is uploaded as it is downloaded from the source, therefore the transfer is bound to the bandwidth of the VM for performance.

### Synchronously Copy data between Azure Blob Storage targets and sources

You can synchronously transfer data between Azure Storage accounts, containers and blob types.

First, you must set the account key of the source storage account.

```bash
export SOURCE_ACCOUNT_KEY=<YOUR  KEY>
```

Then you can specify the URI of the source. Prefixes are supported.

`./blobporter -f "https://mysourceaccount.blob.core.windows.net/container/myblob"  -c mycontainer -t blob-blockblob`

>Note: For better performance, consider running this tranfer from a VM running in the same region as source or the target. Data is uploaded as it is downloaded from the source, therefore the transfer is bound to the bandwidth of the VM for performance.

### Upload from an HTTP/HTTPS source to Azure Blob Storage

To block blob storage:

`./blobporter -f "http://mysource/file.bam"  -c mycontainer -n file.bam -t http-blockblob`

To page blob storage:

`./blobporter -f "http://mysource/my.vhd"  -c mycontainer -n my.vhd -t http-pageblob`

>Note: For better performance, consider running this tranfer from a VM running in the same region as source or the target. Data is uploaded as it is downloaded from the source, therefore the transfer is bound to the bandwidth of the VM for performance.

### Download from Azure Blob Storage

From blob storage to a local file, the source can be a page or block blob:

`./blobporter  -c mycontainer -n file.bam -t blob-file`

You can use the -n option to specify a prefix. All blobs that match the prefix will be downloaded.
The following will download all blobs in the container that start with `f`

`./blobporter  -c mycontainer -n f -t blob-file`

Without the -n option all files in the container will be downloaded.

`./blobporter  -c mycontainer -t blob-file`

By default files are downloaded to the same directory where you are running blobporter. If you want to keep the same directory structure of the storage account use the -p option.

`./blobporter -p -c mycontainer -t blob-file`

### Download a file via HTTP to a local file

`./blobporter -f "http://mysource/file.bam"  -n /datadrive/file.bam -t http-file`

>Note: The ACCOUNT_NAME and ACCOUNT_KEY environment variables are not required in this scenario.

## Command Options

- `-f`, `--source_file` *string* URL, Azure Blob or S3 Endpoint, file or files (e.g. /data/*.gz) to upload.

- `-c`, `--container_name` *string* container name (e.g. `mycontainer`).

- `-n`, `--blob_name` *string* blob name (e.g. myblob.txt) or prefix for download scenarios.

- `-g`, `--concurrent_workers` *int* number of routines for parallel upload.

- `-r`, `--concurrent_readers` *int* number of routines for parallel reading of the input.

- `-b`, `--block_size` *string* desired size of each blob block. Can be specified as an integer byte count or integer suffixed with B, KB or MB (default "4MB", maximum "100MB").

  - The block size could have a significant memory impact. If you are using large blocks reduce the number of readers and workers (-r and -g options) to reduce the memory pressure during the transfer.

  - For files larger than 200GB, this parameter must be set to a value higher than 4MB. The minimum block size is defined by the following formula:

    - `Minimum Block Size = File Size / 50000`

  - The maximum block size is 100MB

- `-a`, `--account_name` *string* storage account name (e.g. mystorage). Can also be specified via the ACCOUNT_NAME environment variable.

- `-k`, `--account_key` *string* storage account key string (e.g. `4Rr8CpUM9Y/3k/SqGSr/oZcLo3zNU6aIo32NVzda4EJj0hjS2Jp7NVLAD3sFp7C67z/i7Rfbrpu5VHgcmOShTg==`). Can also be specified via the ACCOUNT_KEY environment variable.

- `-s`, `--http_timeout` *int* HTTP client timeout in seconds. Default value is 600s.

- `-d`, `--dup_check_level` *string* desired level of effort to detect duplicate data blocks to minimize upload size. Must be one of None, ZeroOnly, Full (default "None")

- `-t`, `--transfer_type` *string*  defines the source and target of the transfer. Must be one of file-blockblob, file-pageblob, http-blockblob, http-pageblob, blob-file, pageblock-file (alias of blob-file), blockblob-file (alias of blob-file), http-file, blob-pageblob, blob-blockblob, s3-pageblob and s3-blockblob.

- `m`, `--compute_blockmd5` *bool* if present or true, block level MD5 has will be computed and included as a header when the block is sent to blob storage. Default is false.

- `q`, `--quiet_mode` *bool* if present or true, the progress indicator is not displayed. The files to transfer, errors, warnings and transfer completion summary is still displayed.

- `x`, `--files_per_transfer` *int* number of files in a batch transfer. Default is 200.

- `h`, `--handles_per_file` *int* number of open handles for concurrent reads and writes per file. Default is 2.

- `p`, `--keep_directories` *bool* if set blobs are downloaded or uploaded keeping the directory structure from the source. Not applicable when the source is a HTTP endpoint.

## Performance Considerations

By default, BlobPorter creates 5 readers and 8 workers for each core on the computer. You can overwrite these values by using the options -r (number of readers) and -g (number of workers). When overriding these options there are few considerations:

- If during the transfer the buffer level is constant at 000%, workers could be waiting for data. Consider increasing the number of readers. If the level is 100% the opposite applies; increasing the number of workers could help.

- In BlobPorter, each reader or worker correlates to one goroutine. Goroutines are lightweight and a Go program can create a high number of goroutines, however, there's a point where the overhead of context switching impacts overall performance. Increase these values in small increments, e.g. 5.

- For transfers from fast disks (SSD) or HTTP sources reducing the number readers or workers could provide better performance than the default values. Reduce these values if you want to minimize resource utilization. Lowering these numbers reduces contention and the likelihood of experiencing throttling conditions.

- Transfers can be batched. Each batch transfer will concurrently read and transfer up to 500 files (default value) from the source. The batch size can be modified using the -x option.

- Blobs smaller than the block size are transferred in a single operation. With relatively small files (<32MB) performance may be better if you set a block size equal to the size of the files. Setting the number of workers and readers to the number of files could yield performance gains.

## Issues and Feedback

If you have a question or find a bug, open a new issue in this repository. BlobPorter is an OSS project maintained by the contributors.

## Contribute

If you would like to become an active contributor to this project please follow the instructions provided in [Microsoft Azure Projects Contribution Guidelines](http://azure.github.io/guidelines/).

-----
This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
