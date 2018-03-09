================
Performance Mode
================

BlobPorter has a performance mode that uploads random data generated in memory and measures the performance of the operation without the impact of disk i/o.
The performance mode for uploads could help you identify the potential upper limit of the data-transfer throughput that your environment can provide.

For example, the following command will upload 10 x 10GB files to a storage account.

``blobporter -f "1GB:10" -c perft -t perf-blockblob``

You can also use this mode to see if increasing (or decreasing) the number of workers/writers (-g option) will have a potential impact.

``blobporter -f "1GB:10" -c perft -t perf-blockblob -g 20``

Similarly, for downloads, you can simulate downloading data from a storage account without writing to disk. This mode could also help you fine-tune the number of readers (-r option) and get an idea of the maximum download throughput.

The following command downloads the data previously uploaded.

``export SRC_ACCOUNT_KEY=$ACCOUNT_KEY`` 

``blobporter -f "https://myaccount.blob.core.windows.net/perft" -t blob-perf`` 

Then you can download the file to disk.

``blobporter -c perft -t blob-file``

The performance difference will you a measurement of the impact of disk i/o.