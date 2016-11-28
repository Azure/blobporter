# Introduction 
TODO: ... a parallel blob copier. 

# Getting Started
TODO: Guide users through getting your code up and running on their own system. 
In this section you can talk about:
1.	Installation process
2.	Software dependencies
3.	Latest releases
4.	API references

# Command Options

- `-a` string  
  `--account_name` string  
storage account name (e.g. mystorage). Can also be specified via the ACCOUNT_NAME environment variable.

- `-b` string  
`--block_size` string  
desired size of each blob block. 
Can be specified an integer byte count or integer suffixed with B, KB, MB, or GB. Maximum of 4MB (default "4MB")

- `-c` string  
`--container_name` string  
container name (e.g. `mycontainer`)

- `-d` string  
`--dup_check_level` string    
Desired level of effort to detect duplicate data blocks to minimize upload size. 
Must be one of None, ZeroOnly, Full (default "None")

- `-f` *string*  
`--source_file` string
source file to upload

- `-g` int  
`--concurrent_workers` int  
number of threads for parallel upload

- `-k` string  
`--account_key` string  
storage account key string 
(e.g. `4Rr8CpUM9Y/3k/SqGSr/oZcLo3zNU6aIo32NVzda4EJj0hjS2Jp7NVLAD3sFp7C67z/i7Rfbrpu5VHgcmOShTg==`).
Can also be specified via the ACCOUNT_KEY environment variable.

- `-n` string   
`--blob_name` string  
blob name (e.g. myblob.txt)

- `-r` int  
`--concurrent_readers` int  
number of threads for parallel reading of the input file

- `-v`  
`--verbose `
display verbose output.

# Contributors
- Jesus Aguilar
- Shawn Elliott
