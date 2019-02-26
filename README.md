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

### What's new (0.6.20) ?

- Blob to Block Blobs transfers use the [Put Block from URL API](https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-from-url).
- Proxy support.
- Minor fixes and performance improvements.
- Upgrade to Go 1.11


## Documentation


* [Getting Started](http://blobporter.readthedocs.io/en/latest/gettingstarted.html)

  * [Linux](http://blobporter.readthedocs.io/en/latest/gettingstarted.html#linux)

  * [Windows](http://blobporter.readthedocs.io/en/latest/gettingstarted.html#windows)

  * [Command Options](http://blobporter.readthedocs.io/en/latest/gettingstarted.html#command-options)

* [Examples](http://blobporter.readthedocs.io/en/latest/examples.html)

  * [Upload to Azure Block Blob Storage](http://blobporter.readthedocs.io/en/latest/examples.html#upload-to-azure-block-blob-storage)

  * [Upload to Azure Page Blob Storage](http://blobporter.readthedocs.io/en/latest/examples.html#upload-to-azure-page-blob-storage)

  * [Transfer data from S3 to Azure Storage](http://blobporter.readthedocs.io/en/latest/examples.html#transfer-data-from-s3-to-azure-storage)

  * [Transfer data between Azure Storage accounts, containers and blob types](http://blobporter.readthedocs.io/en/latest/examples.html#transfer-data-between-azure-storage-accounts-containers-and-blob-types)

  * [Transfer from an HTTP/HTTPS source to Azure Blob Storage](http://blobporter.readthedocs.io/en/latest/examples.html#transfer-from-an-http-https-source-to-azure-blob-storage)

  * [Download from Azure Blob Storage](http://blobporter.readthedocs.io/en/latest/examples.html#download-from-azure-blob-storage)

  * [Download a file from a HTTP source](http://blobporter.readthedocs.io/en/latest/examples.html#download-a-file-from-a-http-source)

* [Resumable Transfers](http://blobporter.readthedocs.io/en/latest/resumabletrans.html)

* [Performance Considerations](http://blobporter.readthedocs.io/en/latest/perfmode.html)

    * [Best Practices](http://blobporter.readthedocs.io/en/latest/perfmode.html#best-practices)

    * [Performance Measurement Mode](http://blobporter.readthedocs.io/en/latest/perfmode.html#performance-measurement-mode)

## Issues and Feedback

If you have a question or find a bug, open a new issue in this repository. BlobPorter is an OSS project maintained by the contributors.

## Contribute

If you would like to become an active contributor to this project please follow the instructions provided in [Microsoft Azure Projects Contribution Guidelines](http://azure.github.io/guidelines/).

-----
This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
