 ## Robust  Integrity  Verification  Algorithm
 
**Abstract**: Scientific applications   generate   large   volumes   ofdata   that   often   needs   to   be   moved   between   geographically distributed  sites  for  collaboration  or  backup  which  has  led  toa  significant  increase  in  data  transfer  rates.  As  an  increasing number   of   scientific   applications   are   becoming   sensitive to silent data corruption, end-to-end integrity verification has been proposed.  It  minimizes  the  likelihood  of  silent  data  corruption by comparing checksum of files at the source and the destination using   secure   hash   algorithms   such   as   MD5   and   SHA1.   In this  paper,  we  investigate  the  robustness  of  existing  end-to-endintegrity  verification  approaches  against  silent  data  corruption and  propose  a  Robust  Integrity  Verification  Algorithm  (RIVA) to enhance data integrity. Extensive experiments show that unlike existing  solutions,  RIVA  is  able  to  detect  silent  disk  corruptions by  invalidating  file  contents  in  page  cache  and  reading  them directly  from  disk.  Since  RIVA  clears  page  cache  and  reads  file contents directly from the disk, it incurs delay to execution time. However, by running transfer, cache invalidation, and checksum operations concurrently, RIVA is able to keep its overhead below 15%  in  most  cases  compared  to  the  state-of-the-art  solutions  in exchange  of  increasing  the  robustness  to  silent  data  corruption. We also implemented dynamic transfer and checksum parallelism to  overcome  performance  bottlenecks  and  observed  more  than 5x  increase  in  RIVA’s  speed.
 
 <!-- Source code of algorithms implemented in the [paper](https://arxiv.org/abs/1811.01161)-->
 
  ##### 1. Dependency
  
  We used vmtouch to evict pages from cache. For details of installation and usage of vmtouch please check: https://hoytech.com/vmtouch/
  
  
  ##### 2. Notes
  
  - By default transfer threads will be connected with port 2010 and checksum threads with port 20180.
  
  - Default transfer block size is 256MB, which can be tuned with INTEGRITY_VERIFICATION_BLOCK_SIZE in source code.
  
  - We commented codes for **fault injection** to each block of transferred file, because if you will not be able to provide correct filesystem name, instead of injecting fault to transferred file it might inject it to some other parts of memory which might mess up your system. Please check (https://github.com/batyrchary) for detailed explanation of fault injection directly to disk.


 
 ##### 3. Cite
 Please cite: **"Towards Securing Data Transfers Against Silent Data Corruption." Batyr Charyyev, Ahmed Alhussen, Hemanta Sapkota, Eric Pouyoul, Mehmet Gunes and Engin Arslan IEEE/ACM International Symposium in Cluster, Cloud, and Grid Computing (CCGrid) 2019.**
 
 
 ##### 4. RIVA
 Compile and Run Receiver
 ```sh
$ javac RIVA_Receiver.java  # Compile Receiver
$ java RIVA_Receiver [destination path] # Run Receiver, will be listening on port 2010 
```
Compile and Run Sender
 ```sh
$ javac RIVA_Sender.java  # Compile Sender
$ java RIVA_Receiver <source_IP> <source_folder>  # Run Sender, will try connecting to port 2010 
```

