Performance Mode
======================================

If you want to maximize performance, and your source and target are public HTTP based end-points (Blob, S3, and HTTP), running the transfer in a high bandwidth environment such as a VM on the cloud, is strongly recommended.  This recommendation comes from the fact that blob to blob, S3 to blob or HTTP to blob transfers are bidirectional where BlobPorter downloads the data (without writing to disk) and uploads it as it is received. 

When running in the cloud, consider the region where the transfer VM ( where BlobPorter will be running), will be deployed. When possible, deploy the transfer VM in the same the same region as the target of the transfer.  Running in the same region as the target minimizes the transfer costs (egress from the VM to the target storage account) and the network performance impact (lower latency) for the upload operation.

For downloads or uploads of multiple or large files the disk i/o could be the constraining resource that slows down the transfer. And often identifying if this is the case, is a cumbersome process.  But if done, it could lead to informed decisions about the environment where BlobPorter runs.

To help with this indentification process, BlobPorter has a performance mode that uploads random data generated in memory and measures the performance of the operation without the impact of disk i/o.
The performance mode for uploads could help you identify the potential upper limit of throughput that the network and the target storage account can provide.   

For example, the following command will upload 10 x 10GB files to a storage account.

```
blobporter -f "1GB:10" -c perft -t perf-blockblob
```

You can also use this mode to see if increasing (or decreasing) the number of workers/writers (-g option) will have a potential impact.

```
blobporter -f "1GB:10" -c perft -t perf-blockblob -g 20
```

Similarly, for downloads, you can simulate downloading data from a storage account without writing to disk. This mode could also help you fine-tune the number of readers (-r option) and get an idea of the maximum download throughput.

The following command will download the data we previously uploaded.

```
export SRC_ACCOUNT_KEY=$ACCOUNT_KEY
blobporter -f "https://myaccount.blob.core.windows.net/perft" -t blob-perf 
```

Then you can try downloading to disk.

```
blobporter -c perft -t blob-file 
```

If the performance difference is significant then you can conclude that disk i/o is the bottleneck, at which point you can consider an SSD backed VM.


