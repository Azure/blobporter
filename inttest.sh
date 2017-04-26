#!/bin/bash
REF_FILE="tempfile"
UP_REF_FILE="dfile"
CONT=$1
FILE_SIZE=5242880 #5MiB
EXIT_CODE=0

#create a temp file
dd if=/dev/zero of=$REF_FILE bs=$FILE_SIZE count=1

#Set permissions
sudo chmod +x ./linux_amd64/blobporter

#Upload file
./linux_amd64/blobporter -f $REF_FILE -n $UP_REF_FILE -c $CONT 

#Download file
./linux_amd64/blobporter -c $CONT -n $UP_REF_FILE -t blob-file

#Compare MD5's
REF_MD5="$(md5sum $REF_FILE | awk '{print $1}')"
VAL_MD5="$(md5sum $DOWN_REF_FILE | awk '{print $1}')"

if [ "$REF_MD5" = "$VAL_MD5" ] ; then
    echo "Success!"
else
    echo $REF_MD5
    echo $VAL_MD5
    echo "Failure!"
    $EXIT_CODE=1
fi
#clean up
rm $REF_FILE
rm $DOWN_REF_FILE

exit $EXIT_CODE