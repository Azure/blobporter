# BlobPorter

Parallel blob copier.

[![Build Status](https://travis-ci.com/Azure/blobporter.svg?token=Z5GQEwTGA6wT7qdrzXsm&branch=master)](https://travis-ci.com/Azure/blobporter)

## Introduction

Getting Started

Linux:

Download, extract and set permissions.

```bash
wget -O bp_linux.tar.gz https://github.com/Azure/blobporter/releases/download/v0.2.04/bp_linux.tar.gz
tar -xvf bp_linux.tar.gz linux_amd64/blobporter
chmod +x ~/linux_amd64/blobporter
cd ~/linux_amd64
```

Set environment variables.

```bash
export ACCOUNT_NAME=<STORAGE_ACCOUNT_NAME>
export ACCOUNT_KEY=<STORAGE_ACCOUNT_KEY>
```

>Note: You can also set these values via options

## Examples

Single file upload to Azure Blob Storage.

`./blobporter -f /datadrive/myfile.tar -c mycontainer`

Upload all files that match the pattern to Azure Blob Storage.

`./blobporter -f "/datadrive/*.tar" -c mycontainer`

Transfer a file via HTTP to Azure Blob Storage.

`./blobporter -f "http://mysource/file.bam"  -c mycontainer -n file.bam -t http-block`

Download a blob from Azure Blob Storage to a local file.

`./blobporter -f /datadrive/file.bam  -c mycontainer -n file.bam -t blob-file`

Download a file via HTTP to a local file.

`./blobporter -f "http://mysource/file.bam"  -n /datadrive/file.bam -t http-file`

## Command Options

- `-f` *string* or `--file` *string* URL, file or files (e.g. /data/*.gz) to upload. Destination file for download scenarios.

- `-c` *string* or `--container_name` *string* container name (e.g. `mycontainer`).

- `-n` *string* or `--blob_name` *string* blob name (e.g. myblob.txt).

- `-g` *int* or `--concurrent_workers` *int* number of routines for parallel upload.

- `-r` *int* or `--concurrent_readers` *int* number of routines for parallel reading of the input.

- `-b` *string* or `--block_size` *string* desired size of each blob block. Can be specified as an integer byte count or integer suffixed with B, KB or MB (default "4MB", maximum "100MB").

- - *Note:* For files larger than 200GB, this parameter must be set to a value higher than 4MB. The minimum block size is defined by the following formula:

- - `Minimum Block Size = File Size / 50000`

- `-a` *string* or `--account_name` *string* storage account name (e.g. mystorage). Can also be specified via the ACCOUNT_NAME environment variable.

- `-k` *string* or `--account_key` *string* storage account key string (e.g. `4Rr8CpUM9Y/3k/SqGSr/oZcLo3zNU6aIo32NVzda4EJj0hjS2Jp7NVLAD3sFp7C67z/i7Rfbrpu5VHgcmOShTg==`). Can also be specified via the ACCOUNT_KEY environment variable.

- `-d` *string* or `--dup_check_level` *string* desired level of effort to detect duplicate data blocks to minimize upload size. Must be one of None, ZeroOnly, Full (default "None")

## Contribute

If you would like to become an active contributor to this project please follow the instructions provided in [Microsoft Azure Projects Contribution Guidelines](http://azure.github.io/guidelines/).

-----
This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

This project has adopted the Microsoft Open Source Code of Conduct. For more information see the Code of Conduct FAQ or contact opencode@microsoft.com with any additional questions or comments.
