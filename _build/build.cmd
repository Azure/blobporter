@echo off
set GOOS=linux
go build  -o .\linux_amd64\blobporter github.com/Azure/blobporter
set GOOS=windows
go build  -o .\windows_amd64\BlobPorter.exe github.com/Azure/blobporter
