# BlobPorter

[![Build Status](https://travis-ci.org/Azure/blobporter.svg?branch=master)](https://travis-ci.org/Azure/blobporter)

## Introduction

BlobPorter is a data transfer tool for Azure Blob storage that maximizes throughput through concurrent reads and writes.

### Getting Started

#### Linux

Download, extract and set permissions:

```bash
wget -O bp_linux.tar.gz https://github.com/Azure/blobporter/releases/download/v0.3.01/bp_linux.tar.gz
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

#### Windows

Download [BlobPorter.exe](https://github.com/Azure/blobporter/releases/download/v0.3.01/bp_windows.zip)

Set environment variables (if using the command prompt):

```batchfile
set ACCOUNT_NAME=<STORAGE_ACCOUNT_NAME>
set ACCOUNT_KEY=<STORAGE_ACCOUNT_KEY>
```

Set environment variables (if using PowerShell):

```PowerShell
env$:ACCOUNT_NAME="<STORAGE_ACCOUNT_NAME>"
env$:ACCOUNT_KEY="<STORAGE_ACCOUNT_KEY>"
```

## Examples

Single file upload to Azure Blob Storage:

`./blobporter -f /datadrive/myfile.tar -c mycontainer -n myfile.tar`

Upload all files that match the pattern to Azure Blob Storage:

`./blobporter -f "/datadrive/*.tar" -c mycontainer`

You can also specify a list of files or patterns explicitly:

`./blobporter -f "/datadrive/*.tar" -f "/datadrive/readme.md" -f "/datadrive/log" -c mycontainer`

When the file list results in a known number of files and you want to upload them with a different name, you can use the -n option:

`./blobporter -f /datadrive/f1.tar -f /datadrive/f2.md -n b1 -n b2 -c mycontainer`

Transfer a file from an HTTP/HTTPS source to Azure Blob Storage:

`./blobporter -f "http://mysource/file.bam"  -c mycontainer -n file.bam -t http-blob`

Download a blob from Azure Blob Storage to a local file:

`./blobporter -f /datadrive/file.bam  -c mycontainer -n file.bam -t blob-file`

Download a file via HTTP to a local file:

`./blobporter -f "http://mysource/file.bam"  -n /datadrive/file.bam -t http-file`

## Command Options

- `-f` *string* or `--file` *string* URL, file or files (e.g. /data/*.gz) to upload. Destination file for download scenarios.

- `-c` *string* or `--container_name` *string* container name (e.g. `mycontainer`).

- `-n` *string* or `--blob_name` *string* blob name (e.g. myblob.txt).

- `-g` *int* or `--concurrent_workers` *int* number of routines for parallel upload.

- `-r` *int* or `--concurrent_readers` *int* number of routines for parallel reading of the input.

- `-b` *string* or `--block_size` *string* desired size of each blob block. Can be specified as an integer byte count or integer suffixed with B, KB or MB (default "4MB", maximum "100MB").

  - *Note:* For files larger than 200GB, this parameter must be set to a value higher than 4MB. The minimum block size is defined by the following formula:

  - `Minimum Block Size = File Size / 50000`

  - The maximum block size is 100MB

- `-a` *string* or `--account_name` *string* storage account name (e.g. mystorage). Can also be specified via the ACCOUNT_NAME environment variable.

- `-k` *string* or `--account_key` *string* storage account key string (e.g. `4Rr8CpUM9Y/3k/SqGSr/oZcLo3zNU6aIo32NVzda4EJj0hjS2Jp7NVLAD3sFp7C67z/i7Rfbrpu5VHgcmOShTg==`). Can also be specified via the ACCOUNT_KEY environment variable.

- `-d` *string* or `--dup_check_level` *string* desired level of effort to detect duplicate data blocks to minimize upload size. Must be one of None, ZeroOnly, Full (default "None")

## Performance Considerations

By default BlobPorter creates 9 readers and 6 workers for each core in the computer. You can overwrite these values by using the options -r (number of readers) and -g (number of workers). When overriding these options there are few considerations:

- If during the transfer the buffer level is constant at 000%, workers could be waiting for data. Consider increasing the number of readers. If the level is 100% the opposite applies; increasing the number of workers could help.

- BlobPorter uses GO's goroutines, each reader or worker correlates to one goroutine. Goroutines are lightweight and a GO program can have a high number of them. However, there's a point where the overhead of context switching impacts overall performance. Increase these values in small increments, e.g. 5.

- For transfers from fast disks (SSD) or HTTP sources a lesser number readers or workers could provide the same performance than the default values. You could reduce these values if you want to minimize resource utilization. Lowering these numbers reduces contention and the likelihood of experiencing throttling conditions.

- In Linux, BlobPorter reduces the number of readers if the number of files results in greater than 1024 handles. Linux restricts the number of files open by a process. Each reader holds a handle to the file to transfer. For example, you can reach this limit if you have 10 readers and want to transfer more than 102 files. In this case BlobPorter will issue a warning displaying the new number of readers. If the resulting number of readers impacts performance, consider running multiple instances of BlobPorter with a smaller source list.

## Contribute

If you would like to become an active contributor to this project please follow the instructions provided in [Microsoft Azure Projects Contribution Guidelines](http://azure.github.io/guidelines/).

-----
This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

This project has adopted the Microsoft Open Source Code of Conduct. For more information see the Code of Conduct FAQ or contact opencode@microsoft.com with any additional questions or comments.
