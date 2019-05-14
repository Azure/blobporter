Examples
========


Upload to Azure Block Blob Storage
-----------------------------------

Single file upload:

::

    ./blobporter -f /datadrive/myfile.tar -c mycontainer

.. note:: BlobPorter will create the container if it doesn't exist.

Upload all files that match the pattern:

::

    ./blobporter -f "/datadrive/*.tar" -c mycontainer

You can also specify a list of files or patterns explicitly:

::

    ./blobporter -f "/datadrive/*.tar" -f "/datadrive/readme.md" -f "/datadrive/log" -c mycontainer

If you want to rename the target file name, you can use the -n option:

::

    ./blobporter -f /datadrive/f1.tar  -n newname.tar -c mycontainer

Upload to Azure Page Blob Storage
----------------------------------

Same as uploading to block blob storage, but with the transfer definiton (-t option) set to ``file-pageblob``.  For example, a single file upload to page blob:

::

    ./blobporter -f /datadrive/mydisk.vhd -c mycontainer -n mydisk.vhd -t file-pageblob


.. note:: The file size and block size must be a multiple of 512 (bytes). The maximum block size is 4MB.

Transfer data from S3 to Azure Storage
---------------------------------------

You can transfer data from an S3 compatible endpoint.

First you must specify the access and secret keys via environment variables.

::  

    export S3_ACCESS_KEY=<YOUR ACCESS KEY>
    export S3_SECRET_KEY=<YOUR_SECRET_KEY>

Then you can specify an S3 URI, with the following format:

::

    [HOST]/[BUCKET]/[PREFIX]

For example:

::

    ./blobporter -f s3://mys3api.com/mybucket/mydata -c froms3 -t s3-blockblob

.. note::

    BlobPorter will upload the data as it downloads it from the source.
    The performance of the transfer will be constraint by the bandwidth of the host running BlobPorter. Consider running this type of transfer from a Virtual Machine running in the same Azure region as the target or the source.

Transfer data between Azure Storage accounts, containers and blob types
-----------------------------------------------------------------------

First, you must set the account key of the source storage account.

::

    export SRC_ACCOUNT_KEY=<YOUR  KEY>


Then you can specify the URI of the source. The source could be a page, block or append blob. Prefixes are supported.

::

    ./blobporter -f "https://mysourceaccount.blob.core.windows.net/container/myblob" -c mycontainer -t blob-blockblob

.. note::

    BlobPorter won't use the bandwidth of the host for this scenario. BlobPorter uses the `Put Block From URL API <https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-from-url>`_.

Transfer from an HTTP/HTTPS source to Azure Blob Storage
--------------------------------------------------------

To block blob storage:

::

    ./blobporter -f "http://mysource/file.bam" -c mycontainer -n file.bam -t http-blockblob

To page blob storage:

::

    ./blobporter -f "http://mysource/my.vhd" -c mycontainer -n my.vhd -t http-pageblob

.. note::

    BlobPorter will upload the data as it downloads it from the source.
    The performance of the transfer will be constraint by the bandwidth of the host running BlobPorter. Consider running this type of transfer from a Virtual Machine running in the same Azure region as the target or the source.

Download from Azure Blob Storage
--------------------------------

For download scenarios, the source can be a page, append or block blob:

::

    ./blobporter -c mycontainer -n file.bam -t blob-file

You can use the -n option to specify a prefix. All blobs that match the prefix will be downloaded. 

The following will download all blobs in the container that start with f:

::

    ./blobporter -c mycontainer -n f -t blob-file


Without the -n option all files in the container will be downloaded.

::

    ./blobporter -c mycontainer -t blob-file


By default files are downloaded keeping the same directory structure as the remote source. 

If you want download to the same directory where you are running blobporter set -i option.

::

    ./blobporter -c mycontainer -t blob-file -i


For scenarios where blob endpoint is from a soverign cloud (e.g. China and Germany), Azure Gov or Azure Stack, you can specify the fully qualified domain name:

::

    ./blobporter -f "https://[ACCOUNT_NAME].[BASE_URL]/[CONTAINER_NAME]/[PREFIX]" -t blob-file

And the source account key, must be set via an environment variable. 

::

    export SOURCE_ACCOUNT_KEY=<YOUR  KEY>



Download a file from a HTTP source
----------------------------------

::

    ./blobporter -f "http://mysource/file.bam" -n /datadrive/file.bam -t http-file

.. note::

    The ACCOUNT_NAME and ACCOUNT_KEY environment variables are not required.
