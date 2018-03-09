===============
Command Options
===============

 -f, --source_file          (string) URL, Azure Blob or S3 Endpoint, file or files (e.g. /data/\*.gz) to upload.
 -c, --container_name       (string) Container name (e.g. mycontainer).
 -n, --blob_name            (string) Blob name (e.g. myblob.txt) or prefix for download scenarios.
 -g, --concurrent_workers   (int) Number of go-routines for parallel upload.
 -r, --concurrent_readers   (int) Number of go-routines for parallel reading of the input.
 -b, --block_size           (string) Desired size of each blob block. 
                            Can be specified as an integer byte count or integer suffixed with B, KB or MB (default /"4MB/", maximum /"100MB/"). 
                            The block size could have a significant memory impact. 
                            If you are using large blocks reduce the number of readers and workers (-r and -g options) to reduce the memory pressure during the transfer.
                            For files larger than 200GB, this parameter must be set to a value higher than 4MB. 
                            The minimum block size is defined by the following formula:   
                            Minimum Block Size = File Size / 50000
                            The maximum block size is 100MB

 -a, --account_name         (string) Storage account name (e.g. mystorage).

                            Can also be specified via the ACCOUNT_NAME environment variable.

 -k, --account_key          (string) Storage account key string.
                            
                            Can also be specified via the ACCOUNT_KEY environment variable.
 -s, --http_timeout         (int) HTTP client timeout in seconds. Default value is 600s.
 -d, --dup_check_level      (string) Desired level of effort to detect duplicate data blocks to minimize upload size.
 
                            Must be one of None, ZeroOnly, Full (default "None")
 -t, --transfer_type        (string) Defines the source and target of the transfer.
 
                            Must be one of file-blockblob, file-pageblob, http-blockblob, http-pageblob, blob-file, pageblock-file (alias of blob-file), blockblob-file (alias of blob-file), http-file, blob-pageblob, blob-blockblob, s3-pageblob and s3-blockblob.
 -m, --compute_blockmd5     (bool) If set, block level MD5 has will be computed and included as a header when the block is sent to blob storage.
 
                            Default is false.
 -q, --quiet_mode           (bool) If set, the progress indicator is not displayed. 

                            The files to transfer, errors, warnings and transfer completion summary is still displayed.
 -x, --files_per_transfer   (int) Number of files in a batch transfer. Default is 500.
 -h, --handles_per_file     (int) Number of open handles for concurrent reads and writes per file. Default is 2.
 -i, --remove_directories   (bool) If set blobs are downloaded or uploaded without keeping the directory structure of the source. 
                            
                            Not applicable when the source is a HTTP endpoint.
 -o, --read_token_exp       (int) Expiration in minutes of the read-only access token that will be generated to read from S3 or Azure Blob sources.
                            
                            Default value: 360.
 -l, --transfer_status      (string) Transfer status file location.
                            If set, blobporter will use this file to track the status of the transfer. 
                            
                            In case of failure and the same file is referrenced, the source files that were transferred will be skipped.
                            
                            If the transfer is successful a summary will be appended.