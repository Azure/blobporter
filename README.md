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
| Azure Block Blob | Yes**            | Yes**            | Yes             |
| Azure Page Blob  | Yes**            | Yes**            | Yes             |

*\*   The HTTP/HTTPS source must support HTTP byte ranges and return the file size as a response to a HTTP HEAD request.*

*\*\* Using the Blob URL with a valid SAS Token with read access .*

## Getting Started

### Linux

Download, extract and set permissions:

```bash
wget -O bp_linux.tar.gz https://github.com/Azure/blobporter/releases/download/v0.5.01/bp_linux.tar.gz
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

Download [BlobPorter.exe](https://github.com/Azure/blobporter/releases/download/v0.5.01/bp_windows.zip)

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

### Upload from an HTTP/HTTPS source to Azure Blob Storage

To block blob storage:

`./blobporter -f "http://mysource/file.bam"  -c mycontainer -n file.bam -t http-blockblob`

To page blob storage:

`./blobporter -f "http://mysource/file.bam"  -c mycontainer -n file.bam -t http-pageblob`

You can use this approach to transfer data between Azure Storage accounts and blob types - e.g. transfer a blob from one account to another or from a page blob to block blob.

The source is a page blob with a SAS token and the target is block blob:

`./blobporter -f "https://myaccount.blob.core.windows.net/vhds/my.vhd?st=2015-03-23T03%3A59%3A00Z&se=2015-03-24T03%3A59%3A00Z&sp=rl&sv=2015-12-11&sr=b&sig=123rfdAsYyqOxxOGe28%3Fp4H6r5reR8pdSBdlchi64wgs3D"  -c mycontainer -n my.vhd -t http-blockblob`

>Note: In HTTP/HTTPS to blob transfers, data is downloaded and uploaded as it is received without disk IO.

### Download from Azure Blob Storage

From blob storage to a local file, the source can be a page or block blob:

`./blobporter  -c mycontainer -n file.bam -t blob-file`

You can use the -n option to specify a prefix. All blobs that match the prefix will be downloaded.
The following will download all blobs in the container that start with `f`

`./blobporter  -c mycontainer -n f -t blob-file`

Without the -n option all files in the container will be downloaded.

`./blobporter  -c mycontainer -t blob-file`

### Download a file via HTTP to a local file

`./blobporter -f "http://mysource/file.bam"  -n /datadrive/file.bam -t http-file`

>Note: The ACCOUNT_NAME and ACCOUNT_KEY environment variables are not required in this scenario.

## Command Options

- `-f`, `--file` *string* URL, file or files (e.g. /data/*.gz) to upload. Destination file for download scenarios.

- `-c`, `--container_name` *string* container name (e.g. `mycontainer`).

- `-n`, `--blob_name` *string* blob name (e.g. myblob.txt) or prefix for download scenarios.

- `-g`, `--concurrent_workers` *int* number of routines for parallel upload.

- `-r`, `--concurrent_readers` *int* number of routines for parallel reading of the input.

- `-b`, `--block_size` *string* desired size of each blob block. Can be specified as an integer byte count or integer suffixed with B, KB or MB (default "4MB", maximum "100MB").

  - *Note:* For files larger than 200GB, this parameter must be set to a value higher than 4MB. The minimum block size is defined by the following formula:

    - `Minimum Block Size = File Size / 50000`

  - The maximum block size is 100MB

- `-a`, `--account_name` *string* storage account name (e.g. mystorage). Can also be specified via the ACCOUNT_NAME environment variable.

- `-k`, `--account_key` *string* storage account key string (e.g. `4Rr8CpUM9Y/3k/SqGSr/oZcLo3zNU6aIo32NVzda4EJj0hjS2Jp7NVLAD3sFp7C67z/i7Rfbrpu5VHgcmOShTg==`). Can also be specified via the ACCOUNT_KEY environment variable.

- `-s`, `--http_timeout` *int* HTTP client timeout in seconds. Default value is 30s.

- `-d`, `--dup_check_level` *string* desired level of effort to detect duplicate data blocks to minimize upload size. Must be one of None, ZeroOnly, Full (default "None")

- `-t`, `--transfer_type` *string*  defines the source and target of the transfer. Must be one of file-blockblob, file-pageblob, http-blockblob, http-pageblob, blob-file, pageblock-file (alias of blob-file), blockblob-file (alias of blob-file) or http-file

## Performance Considerations

By default, BlobPorter creates 6 readers and 9 workers for each core on the computer. You can overwrite these values by using the options -r (number of readers) and -g (number of workers). When overriding these options there are few considerations:

- If during the transfer the buffer level is constant at 000%, workers could be waiting for data. Consider increasing the number of readers. If the level is 100% the opposite applies; increasing the number of workers could help.

- In BlobPorter, each reader or worker correlates to one goroutine. Goroutines are lightweight and a Go program can create a high number of goroutines, however, there's a point where the overhead of context switching impacts overall performance. Increase these values in small increments, e.g. 5.

- For transfers from fast disks (SSD) or HTTP sources a lesser number readers or workers could provide the same performance than the default values. You could reduce these values if you want to minimize resource utilization. Lowering these numbers reduces contention and the likelihood of experiencing throttling conditions.

- In Linux, BlobPorter reduces the number of readers if the number of open files during the transfer is greater than 1024. Linux restricts the number of files open by a process and since each reader holds a handle to the file to transfer, you can reach this limit if you want transfer multiple files even with a relatively low number of readers. For example, if you have 10 readers and want to transfer more than 102 files you will reach this limit. In this case BlobPorter will issue a warning displaying the new number of readers. If the resulting number of readers impacts performance, consider running multiple instances of BlobPorter with a smaller source list.

## Contribute

If you would like to become an active contributor to this project please follow the instructions provided in [Microsoft Azure Projects Contribution Guidelines](http://azure.github.io/guidelines/).

-----
This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
