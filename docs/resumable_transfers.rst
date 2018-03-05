Resumable Transfers
======================================
BlobPorter supports resumable transfers. To enable this feature you need to set the -l option with a path to the transfer status file.

```
blobporter -f "manyfiles/*" -c "many" -l mylog
```

The status transfer file contains entries for when a file is queued and when it was succesfully tranferred.

The log entries are created with the following tab-delimited format:

```
[Timestamp] [Filename] [Status (1:Started,2:Completed,3:Ignored)] [Size] [Transfer ID ]
```

The following output from a transfer status file shows that three files were included in the transfer (file10, file11 and file15).
However, only two were successfully transferred: file10 and file11.

```
2018-03-05T03:31:13.034245807Z  file10  1       104857600       938520246_mylog
2018-03-05T03:31:13.034390509Z  file11  1       104857600       938520246_mylog
2018-03-05T03:31:13.034437109Z  file15  1       104857600       938520246_mylog
2018-03-05T03:31:25.232572306Z  file10  2       104857600       938520246_mylog
2018-03-05T03:31:25.591239355Z  file11  2       104857600       938520246_mylog
```

In case of failure, you can reference the same status file and BlobPorter will skip files that were already transferred.

Consider the previous scenario. After executing the transfer again, the status file has entries only for the missing file (file15).

```
2018-03-05T03:31:13.034245807Z  file10  1       104857600       938520246_mylog
2018-03-05T03:31:13.034390509Z  file11  1       104857600       938520246_mylog
2018-03-05T03:31:13.034437109Z  file15  1       104857600       938520246_mylog
2018-03-05T03:31:25.232572306Z  file10  2       104857600       938520246_mylog
2018-03-05T03:31:25.591239355Z  file11  2       104857600       938520246_mylog
2018-03-05T03:54:33.660161772Z  file15  1       104857600       495675852_mylog
2018-03-05T03:54:34.579295059Z  file15  2       104857600       495675852_mylog
```

When the transfer is sucessful, a summary is created at the end of the transfer status file.

```
----------------------------------------------------------
Transfer Completed----------------------------------------
Start Summary---------------------------------------------
Last Transfer ID:495675852_mylog
Date:Mon Mar  5 03:54:34 UTC 2018
File:file15     Size:104857600  TID:495675852_mylog
File:file10     Size:104857600  TID:938520246_mylog
File:file11     Size:104857600  TID:938520246_mylog
Transferred Files:3     Total Size:314572800
End Summary-----------------------------------------------
```


 

