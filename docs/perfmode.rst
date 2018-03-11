Performance Consideration
=========================

Best Practices
--------------


-   By default, BlobPorter creates 5 readers and 8 workers for each core on the computer. You can overwrite these values by using the options -r (number of readers) and -g (number of workers). When overriding these options there are few considerations:

    -   If during the transfer the buffer level is constant at 000%, workers could be waiting for data. Consider increasing the number of readers. If the level is 100% the opposite applies; increasing the number of workers could help.

    -   Each reader or worker correlates to one goroutine. Goroutines are lightweight and a Go program can create a high number of goroutines, however, there's a point where the overhead of context switching impacts overall performance. Increase these values in small increments, e.g. 5.

-   For transfers from fast disks (SSD) or HTTP sources reducing the number readers or workers could provide better performance than the default values. Reduce these values if you want to minimize resource utilization. Lowering these numbers reduces contention and the likelihood of experiencing throttling conditions.

-   Transfers can be batched. Each batch transfer will concurrently read and transfer up to 500 files (default value) from the source. The batch size can be modified using the -x option.

-   Blobs smaller than the block size are transferred in a single operation. With relatively small files (<32MB) performance may be better if you set a block size equal to the size of the files. Setting the number of workers and readers to the number of files could yield performance gains.

-   The block size can have a significant memory impact if set to a large value (e.g. 100MB). For large files, use a block size that is close to the minimum required for the transfer and reduce the number of workers (g option).

    The following table list the maximum file size for typical block sizes.

        =============== ===================
        Block Size (MB) Max File Size (GB)
        =============== ===================
        8               400        
        16              800
        32              1600
        64              3200
        =============== ===================        

Performance Measurement Mode
----------------------------

BlobPorter has a performance measurement mode that uploads random data generated in memory and measures the performance of the operation without the impact of disk i/o.
The performance mode for uploads could help you identify the potential upper limit of the data throughput that your environment can provide.

For example, the following command will upload 10 x 10GB files to a storage account.

::

    blobporter -f "1GB:10" -c perft -t perf-blockblob

You can also use this mode to see if increasing (or decreasing) the number of workers/writers (-g option) will have a potential impact.

::

    blobporter -f "1GB:10" -c perft -t perf-blockblob -g 20

Similarly, for downloads, you can simulate downloading data from a storage account without writing to disk. This mode could also help you fine-tune the number of readers (-r option) and get an idea of the maximum download throughput.

The following command downloads the data previously uploaded.

::

    export SRC_ACCOUNT_KEY=$ACCOUNT_KEY

..

::

    blobporter -f "https://myaccount.blob.core.windows.net/perft" -t blob-perf``


Then you can download the file to disk.

::

    blobporter -c perft -t blob-file

The performance difference will provide with a base-line of the impact of disk i/o.