===============
Getting Started 
===============

Linux
-----

Download, extract and set permissions

::

    wget -O bp_linux.tar.gz https://github.com/Azure/blobporter/releases/download/v0.6.09/bp_linux.tar.gz
    tar -xvf bp_linux.tar.gz linux_amd64/blobporter
    chmod +x ~/linux_amd64/blobporter
    cd ~/linux_amd64

Set environment variables: ::

    export ACCOUNT_NAME=<STORAGE_ACCOUNT_NAME>
    export ACCOUNT_KEY=<STORAGE_ACCOUNT_KEY>

**Note:** You can also set these values via `options <options.html>`__

Windows
-------

Download `BlobPorter.exe <https://github.com/Azure/blobporter/releases/download/v0.6.10/bp_windows.zip>`_

Set environment variables (if using the command prompt): ::

    set ACCOUNT_NAME=<STORAGE_ACCOUNT_NAME>
    set ACCOUNT_KEY=<STORAGE_ACCOUNT_KEY>

Set environment variables (if using PowerShell): ::

    $env:ACCOUNT_NAME="<STORAGE_ACCOUNT_NAME>"
    $env:ACCOUNT_KEY="<STORAGE_ACCOUNT_KEY>"

