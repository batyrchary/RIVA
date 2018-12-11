 ## Fast Integrity Verification Algorithm
 
**Abstract**: End-to-end integrity verification minimizes the likelihood of silent data corruption by comparing checksum of files at source and destination servers using secure hash algorithms such as MD5 and SHA1. However, it imposes significant performance penalty due to overhead of checksum computation. In this project, we propose Fast Integrity VERification (FIVER) algorithm which overlaps checksum computation and data transfer operations of files to minimize the cost of integrity verification. Extensive experiments show that FIVER is able to bring down the cost from 60% by the state-of-the-art solutions to below 10\% by concurrently executing transfer and checksum operations and enabling file I/O share between them. We also implemented FIVER-Hybrid to mimic disk access patterns of sequential integrity verification approach to capture possible data corruption that may occur during file write operations which FIVER may miss. Results show that FIVER-Hybrid is able to reduce execution time by 20% compared to sequential approach without compromising the reliability of integrity verification.
 
 Source code of algorithms implemented in the [paper](https://arxiv.org/abs/1811.01161)
 ##### 1. FIVER
 Compile and Run Receiver
 ```sh
 $ cd src/
$ javac FiverReceiver.java  # Compile Receiver
$ java FileReceiver [destination path] # Run Receiver, will be listening on port 2008
```
Compile and Run Sender
 ```sh
 $ cd src/
$ javac FiverSender.java  # Compile Sender
$ java FileReceiver <source_IP> <source_folder>  # Run Sender, will try connecting to port 2008
```

 ##### 2. Block-Level Pipelining. [Paper](https://ieeexplore.ieee.org/abstract/document/7840953)
 Compile and Run Receiver
 ```sh
 $ cd src/
$ javac BlockLevelPipeliningReceiver.java  # Compile Receiver
$ java BlockLevelPipeliningReceiver [destination path] # Run Receiver, will be listening on port 2038
```
Compile and Run Sender. Block-level pipelining works by sending/verifying blocks of  files. Block size is set to 256 MB by default by can be changed by setting up third command-line argument
 ```sh
 $ cd src/
$ javac BlockLevelPipeliningSender.java  # Compile Sender
$ javac BlockLevelPipeliningSender <source_IP> <source_folder> [block-size-in-byte] # Run Sender, will try connecting to port 2038
```

 ##### 3. File-Level Pipelining
 Compile and Run Receiver
 ```sh
 $ cd src/
$ javac FileLevelPipeliningReceiver.java  # Compile Receiver
$ java FileLevelPipeliningReceiver [destination path] # Run Receiver, will be listening on port 2028
```
Compile and Run Sender. It is possible to sort files by file name if file names are integer
 ```sh
 $ cd src/
$ javac FileLevelPipeliningSender.java  # Compile Sender
$ java FileLevelPipeliningSender <source_IP> <source_folder>  # Run Sender, will try connecting to port 2028
```
