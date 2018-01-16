#!/bin/bash
DOWN_F1=dref.blob
DOWN_P1=dref.vhd
WORKING_DIR=wd
F1=$WORKING_DIR"/"ref.blob
P1=$WORKING_DIR"/"ref.vhd

CONT="bptest"

calculateMD5 () {
REF_MD5="$(md5sum $1 | awk '{print $1}')"
VAL_MD5="$(md5sum $2 | awk '{print $1}')"

if [ "$REF_MD5" = "$VAL_MD5" ] ; then
    echo "Success!"
else
    echo $REF_MD5
    echo $VAL_MD5
    echo "Failure!"
    exit 1
fi
}

#Create random files for testing...
echo "Creating working files..."
mkdir -p $WORKING_DIR

dd if=/dev/urandom of=$F1 bs=64M count=8 iflag=fullblock
dd if=/dev/urandom of=$P1 bs=64M count=2 iflag=fullblock


#Scenario 1 - Simple upload/download with default values
#Upload file
./blobporter -f $F1 -c $CONT -n $DOWN_F1

#Download file
./blobporter -c $CONT -n $DOWN_F1 -t blob-file
calculateMD5 $F1 $DOWN_F1

#Scenario 2 - Simple page upload/download with default values
#Upload vhd
./blobporter -f $P1 -c $CONT -n $DOWN_P1 -t file-pageblob

#Download vhd
./blobporter -n $DOWN_P1 -c $CONT  -t pageblob-file
calculateMD5 $P1 $DOWN_P1

#Scenario 3 - Silent page upload/download with default values
#Upload vhd
./blobporter -f $P1 -c $CONT -n $DOWN_P1 -t file-pageblob -q
#Download vhd
./blobporter -n $DOWN_P1 -c $CONT -t pageblob-file -q
calculateMD5 $P1 $DOWN_P1

#Scenario 4 - Silent page upload/download with default values and md5
#Upload vhd
./blobporter -f $P1 -c $CONT -n $DOWN_P1 -t file-pageblob -q -m
#Download vhd
./blobporter -n $DOWN_P1 -c $CONT -t pageblob-file -q -m
calculateMD5 $P1 $DOWN_P1

#Scenario 5 - Simple upload/download with default values and md5
#Upload file
./blobporter -f $F1 -c $CONT -n $DOWN_F1  -m
#Download file
./blobporter -n $DOWN_F1 -c $CONT -t blob-file -m
calculateMD5 $F1 $DOWN_F1

#Scenario 6 - Simple download from a HTTP source
URL=http://video.ch9.ms/ch9/a44c/d57e542a-665a-4fdd-a29a-12c606fda44c/IntroASPNETMVCM01_mid.mp4
wget -O vd1f.mp4 $URL
./blobporter -f $URL -n vd1.mp4 -t http-file
calculateMD5 vd1.mp4 vd1f.mp4


#Scenario 7 - Simple upload/download with 16MB
#Upload file
./blobporter -f $F1 -c $CONT -n $DOWN_F1 -b 16MB

#Download file
./blobporter -n $DOWN_F1 -c $CONT -t blob-file -b 16MB
calculateMD5 $F1 $DOWN_F1


#Scenario 7 - Simple upload/download with file smaller than the block size
dd if=/dev/urandom of=$F1 bs=16M count=1 iflag=fullblock

#Upload file
./blobporter -f $F1 -c $CONT -n $DOWN_F1 -b 32MB

#Download file
./blobporter -n $DOWN_F1 -c $CONT -t blob-file -b 32MB
calculateMD5 $F1 $DOWN_F1

#Scenario 8 - Simple upload/download with file equal than the block size
dd if=/dev/urandom of=$F1 bs=36M count=1 iflag=fullblock

#Upload file
./blobporter -f $F1 -c $CONT -n $DOWN_F1 -b 32MB

#Download file
./blobporter -n $DOWN_F1 -c $CONT -t blob-file -b 32MB
calculateMD5 $F1 $DOWN_F1


#Scenario 9 - Synchronous copy from one container to another in the same storage account.
CONT2="syncopy"
SRC_URL="https://"$ACCOUNT_NAME".blob.core.windows.net/"$CONT
SRC_ACCOUNT_KEY=$ACCOUNT_KEY

./blobporter -f $SRC_URL -c $CONT2 -t blob-blockblob

