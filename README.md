 ## Robust  Integrity  Verification  Algorithm
 
**Abstract**: Scientific   applications   generate   large   volumes   ofdata   that   often   needs   to   be   moved   between   geographicallydistributed  sites  for  collaboration  or  backup  which  has  led  toa  significant  increase  in  data  transfer  rates.  As  an  increasingnumber   of   scientific   applications   are   becoming   sensitive   tosilent data corruption, end-to-end integrity verification has beenproposed.  It  minimizes  the  likelihood  of  silent  data  corruptionby comparing checksum of files at the source and the destinationusing   secure   hash   algorithms   such   as   MD5   and   SHA1.   Inthis  paper,  we  investigate  the  robustness  of  existing  end-to-endintegrity  verification  approaches  against  silent  data  corruptionand  propose  a  Robust  Integrity  Verification  Algorithm  (RIVA) to enhance data integrity. Extensive experiments show that unlikeexisting  solutions,  RIVA  is  able  to  detect  silent  disk  corruptionsby  invalidating  file  contents  in  page  cache  and  reading  themdirectly  from  disk.  Since  RIVA  clears  page  cache  and  reads  filecontents directly from the disk, it incurs delay to execution time.However, by running transfer, cache invalidation, and checksumoperations concurrently, RIVA is able to keep its overhead below15%  in  most  cases  compared  to  the  state-of-the-art  solutions  inexchange  of  increasing  the  robustness  to  silent  data  corruption.We also implemented dynamic transfer and checksum parallelismto  overcome  performance  bottlenecks  and  observed  more  than 5x  increase  in  RIVA’s  speed.
 
 Source code of algorithms implemented in the [paper](https://arxiv.org/abs/1811.01161)
 ##### 1. FIVER
 Compile and Run Receiver
 ```sh
 $ cd src/
$ javac RIVA_Receiver.java  # Compile Receiver
$ java RIVA_Receiver [destination path] # Run Receiver, will be listening on port 2010 and checksum thread will be connected with port 20180
```
Compile and Run Sender
 ```sh
 $ cd src/
$ javac RIVA_Sender.java  # Compile Sender
$ java RIVA_Receiver <source_IP> <source_folder>  # Run Sender, will try connecting to port 2010 
```

