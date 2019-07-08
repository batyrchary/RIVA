import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.Random;





public class DynamicSender
{

    public static volatile Random rand = new Random();

    public static volatile Map<Integer, SenderRunnable> senderThreads = new HashMap<>();
    public static volatile Map<Integer, SenderRunnable> senderThreads_tobedeleted = new HashMap<>();
    public static volatile Map<Integer, ChecksumRunnable>  checksumThreads = new HashMap<>();




    public static String destination_ip="-1";
    public static String base_directory="-1";
    public static int port=-1;


    public static volatile Long INTEGRITY_VERIFICATION_BLOCK_SIZE = 0l;  //256 * 1024 * 1024;

    public static volatile Integer sender_thread_id=1;
    public static volatile Integer checksum_thread_id=1;

    public static volatile Long fiver_id=0l;

    public static volatile boolean allFileTransfersCompleted = false;


    public static volatile LinkedList<DynamicCommon.FiverFile> files=new LinkedList<>();
    public static LinkedBlockingQueue<DynamicCommon.FiverFile> checksumFiles=new LinkedBlockingQueue<>(10000);

    public static volatile Map<Long, DynamicCommon.FiverFile> checksums_being_computed = new HashMap<>();
    public static volatile Map<Long, DynamicCommon.FiverFile> files_being_transferred = new HashMap<>();



    public static volatile Map<Long, String> computed_checksums = new HashMap<>();
    public static volatile Map<Long, DynamicCommon.FiverFile> computed_checksums_fiverFile = new HashMap<>();
    public static volatile Map<Long, String> received_checksums = new HashMap<>();


    public static volatile Integer lock_for_checksumFiles_inTransfer_threads=1;
    public static volatile Integer lock_for_checksumFiles_inChecksum_threads=1;



    public static volatile int total_files_sent=0;
    public static volatile int total_checksum_files=0;
    public static volatile int total_checksums_computed=0;
    public static volatile int total_checksums_received=0;
    public static volatile int total_checksums_matched=0;

    public static volatile Long totalTransferredBytes = 0l;
    public static volatile Long totalChecksumBytes = 0l;



    public static class SenderRunnable implements Runnable
    {
        int threadID;
        boolean shouldIfinish=false;
        Thread t;
        Socket s;

        public SenderRunnable(int senderthreadid)
        {
           try
           {
               threadID=senderthreadid;
               s = new Socket(destination_ip, (port + senderthreadid));
               s.setSoTimeout(10000);
               t = new Thread(this);
               t.start();
           }catch (Exception e){e.printStackTrace();}
        }

        @Override
        public void run()
        {
            try
            {
                DataOutputStream dos = new DataOutputStream(s.getOutputStream());

                byte[] buffer = new byte[128 * 1024];
                int n;
                while (!shouldIfinish)
                {

                    if(senderThreads_tobedeleted.containsKey(-1*threadID))//still not finished (deleted)
                    {
                        continue;
                    }

                    DynamicCommon.FiverFile currentFile = null;
                    synchronized (files)
                    {
                        if (!files.isEmpty())
                        {
                            currentFile=files.get(0);
                            long blockSize_forCurrentFile=INTEGRITY_VERIFICATION_BLOCK_SIZE;

                            if(blockSize_forCurrentFile >= currentFile.length)
                            {
                                files.remove(0);
                            }
                            else
                            {
                                currentFile.length=blockSize_forCurrentFile;

                                DynamicCommon.FiverFile newFile = new DynamicCommon.FiverFile(currentFile);

                                newFile.offset=newFile.offset+blockSize_forCurrentFile;
                                newFile.length=newFile.endoffset-newFile.offset;
                                newFile.id=fiver_id++;

                                files.set(0,newFile);
                            }
                        }
                    }

                    if (currentFile == null)
                    {
                        dos.writeUTF("done");
                        Thread.sleep(100);
                        continue;
                    }

                    synchronized (files_being_transferred)
                    {
                        files_being_transferred.put(currentFile.id, currentFile);

                        total_files_sent++;
                    }


                    String currentFileName = currentFile.file.getName();
                    dos.writeUTF(currentFileName);
                    dos.writeLong(currentFile.offset);
                    dos.writeLong(currentFile.length);
                    dos.writeLong(currentFile.id);

                    String checkprint="file"+currentFile.file+"\toffset="+currentFile.offset;
                    checkprint=checkprint+"\t"+"length="+currentFile.length+"\tid="+currentFile.id;

                   // System.out.println(checkprint);


                    FileInputStream fis = new FileInputStream(currentFile.file);
                    if (currentFile.offset > 0)
                    {
                        fis.getChannel().position(currentFile.offset);
                    }

                    Long remaining = currentFile.length;

                    while (remaining > 0)
                    {
                        int howmuchtoRead = (int) Math.min(buffer.length, remaining);

                        n = 0;
                        while (n != howmuchtoRead) {
                            int r = fis.read(buffer, n, howmuchtoRead - n);
                            n = n + r;
                        }
                        remaining -= n;
                        dos.write(buffer, 0, n);

                        synchronized (totalTransferredBytes)
                        {
                            totalTransferredBytes = totalTransferredBytes + n;
                        }
                    }

                    synchronized (lock_for_checksumFiles_inTransfer_threads)
                    {
                        checksumFiles.offer(currentFile);

                        total_checksum_files++;
                    }

                    synchronized (files_being_transferred)
                    {
                        files_being_transferred.remove(currentFile.id);
                    }

                    fis.close();
                }
                dos.writeUTF("done");
                //dos.close();

                synchronized (senderThreads_tobedeleted)
                {
                    senderThreads_tobedeleted.remove(this.threadID);
                //    System.out.println("i am exiting transfer thread="+threadID+"\tsthreadID="+sender_thread_id);
                }

            } catch (Exception e){
                //System.out.println("cant get output stream");
                }
        }
    }


    public static class ChecksumRunnable implements Runnable
    {
        int threadID;
        boolean shouldIfinish=false;
        Thread t;

        MessageDigest md = null;

        public ChecksumRunnable(int threadID)
        {
            this.threadID=threadID;
            t=new Thread(this);
            t.start();
        }

        public void reset () {
            md.reset();
        }

        @Override
        public void run()
        {
            try
            {
                md = MessageDigest.getInstance("MD5");
                md.reset();

                byte[] buffer = new byte[128 * 1024];
                while (!shouldIfinish)
                {
                    DynamicCommon.FiverFile currentFile = null;

                    synchronized (lock_for_checksumFiles_inChecksum_threads)
                    {
                        currentFile = checksumFiles.poll();
                    }


                    if (currentFile == null)
                    {
                    //    DynamicCommon.sleeper(100);
                        continue;
                    }

                    synchronized (checksums_being_computed)
                    {
                        checksums_being_computed.put(currentFile.id, currentFile);
                        total_checksums_computed++;
                    }


                    FileInputStream fis = new FileInputStream(currentFile.file);

                    if (currentFile.offset > 0)
                    {
                        fis.getChannel().position(currentFile.offset);
                    }

                    DigestInputStream dis = new DigestInputStream(fis, md);

                    long remaining = currentFile.length;

                    int read;

                    while (remaining > 0)
                    {
                        read = dis.read(buffer, 0, (int)Math.min(buffer.length, remaining));

                        synchronized (totalChecksumBytes)
                        {
                            totalChecksumBytes = totalChecksumBytes + read;
                        }

                        if (read == -1)
                        {
                            Thread.sleep(100);
                        }
                        else
                        {
                            remaining -= read;
                        }
                    }
                    dis.close();
                    fis.close();
                    byte[] digest = md.digest();
                    String hex = (new HexBinaryAdapter()).marshal(digest);

                    md.reset();

                    //String tobedigested="anasinisatim";
                    //String hex=tobedigested;

                    synchronized (computed_checksums)
                    {
                    //    System.out.println("putting id="+currentFile.id+"\tdigest="+hex);

                        computed_checksums.put(currentFile.id, hex);
                        computed_checksums_fiverFile.put(currentFile.id, currentFile);
                    }

                    synchronized (checksums_being_computed)
                    {
                        checksums_being_computed.remove(currentFile.id);
                    }
                }
            //    System.out.println("checksum thread is exiting="+threadID);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    public static class ChecksumReceiverRunnable implements Runnable
    {
        int threadID;
        Thread t;
        Socket s;

        public ChecksumReceiverRunnable(int threadID) {
            try {
                this.threadID = threadID;
                s = new Socket(destination_ip, 2060);

                t = new Thread(this);
                t.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run()
        {
            try
            {
                DataInputStream dataInputStream = new DataInputStream(s.getInputStream());

                while (!allFileTransfersCompleted)
                {

                    if(dataInputStream.available()>0)
                    {
                        long destination_file_id = dataInputStream.readLong();
                        String destinationHex = dataInputStream.readUTF();

                        total_checksums_received++;

                        if (computed_checksums.containsKey(destination_file_id))
                        {
                            total_checksums_matched++;

                            //System.out.println("exist id="+destination_file_id+"\tdigest="+destinationHex);

                            String hex;
                            DynamicCommon.FiverFile newFile;
                            synchronized (computed_checksums)
                            {
                                hex = computed_checksums.get(destination_file_id);
                                computed_checksums.remove(destination_file_id);
                                newFile = computed_checksums_fiverFile.get(destination_file_id);
                                computed_checksums_fiverFile.remove(destination_file_id);


                            }

                            if (hex.compareTo(destinationHex) != 0) // Checksums dont match
                            {
                                String printed=hex+"\n"+destinationHex;

                                System.out.println(printed);

                                synchronized (files)
                                {
                                    files.add(newFile);
                                    System.out.println("File added " + files.size() + " files left");
                                }
                            }
                        }
                        else
                        {
                            //    System.out.println("doesnot exist id="+destination_file_id+"\tdigest="+destinationHex);
                            received_checksums.put(destination_file_id, destinationHex);
                        }

                    }


                    for (Iterator<Map.Entry<Long, String>> it = received_checksums.entrySet().iterator(); it.hasNext(); )
                    {

                        Map.Entry<Long, String> entry = it.next();

                        String destinationHex = entry.getValue();
                        Long destination_file_id1 = entry.getKey();

                        if (computed_checksums.containsKey(destination_file_id1))
                        {
                            total_checksums_matched++;

                            String hex;
                            DynamicCommon.FiverFile newFile;
                            synchronized (computed_checksums)
                            {
                                hex = computed_checksums.get(destination_file_id1);
                                computed_checksums.remove(destination_file_id1);
                                newFile = computed_checksums_fiverFile.get(destination_file_id1);
                                computed_checksums_fiverFile.remove(destination_file_id1);
                            }

                            if (hex.compareTo(destinationHex) != 0) // Checksums dont match
                            {
                                String printed=hex+"\n"+destinationHex;
                                System.out.println(printed);

                                synchronized (files)
                                {
                                    files.add(newFile);
                                    System.out.println("File added " + files.size() + " files left");
                                }
                            }

                            it.remove();
                        }
                    }


                    if (files.isEmpty() && checksumFiles.isEmpty() && computed_checksums.isEmpty() && computed_checksums_fiverFile.isEmpty() && received_checksums.isEmpty()
                            && files_being_transferred.isEmpty() && checksums_being_computed.isEmpty()) {
                        allFileTransfersCompleted = true;
                    }

                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }
    }


    public static void changeThreads(String which, String type, int howmuch)
    {
        try
        {
            if (which.equals("transfer"))
            {
                synchronized (sender_thread_id)
                {
                    if (type.equals("increment"))
                    {
                        for (int i = 0; i < howmuch; i++)
                        {
                            SenderRunnable sender = new SenderRunnable(sender_thread_id);
                            senderThreads.put(sender_thread_id,sender);
                            sender_thread_id++;
                        }
                    }
                    else if (type.equals("decrement"))
                    {
                        for (int i = 0; i < howmuch; i++)
                        {
                            int sthreadIDTobeDeleted = sender_thread_id-1;
                            senderThreads.get(sthreadIDTobeDeleted).shouldIfinish=true;

                            SenderRunnable tobedeletedthread=senderThreads.get(sthreadIDTobeDeleted);
                            tobedeletedthread.threadID=-1*tobedeletedthread.threadID;
                            senderThreads_tobedeleted.put(-1*sthreadIDTobeDeleted,tobedeletedthread);

                            senderThreads.remove(sthreadIDTobeDeleted);
                            sender_thread_id=sthreadIDTobeDeleted;
                        }
                    }
                }
            }

            else if (which.equals("checksum"))
            {
                synchronized (checksum_thread_id)
                {
                    if (type.equals("increment"))
                    {
                        for (int i = 0; i < howmuch; i++)
                        {
                            ChecksumRunnable checksumRunnable = new ChecksumRunnable(checksum_thread_id);
                            checksumThreads.put(checksum_thread_id,checksumRunnable);
                            checksum_thread_id++;
                        }
                    }
                    else if (type.equals("decrement"))
                    {
                        for (int i = 0; i < howmuch; i++)
                        {
                            int checkidToBeDeleted=checksum_thread_id-1;
                            checksumThreads.get(checkidToBeDeleted).shouldIfinish = true;
                            checksumThreads.remove(checkidToBeDeleted);
                            checksum_thread_id=checkidToBeDeleted;
                        }
                    }
                }
            }

            else if (which.equals("blocksize"))
            {
                howmuch=howmuch * 1024 * 1024;
                if (type.equals("increment"))
                {
                    INTEGRITY_VERIFICATION_BLOCK_SIZE = INTEGRITY_VERIFICATION_BLOCK_SIZE + howmuch;
                }
                else if (type.equals("decrement"))
                {
                    INTEGRITY_VERIFICATION_BLOCK_SIZE = INTEGRITY_VERIFICATION_BLOCK_SIZE - howmuch;
                }
            }
        }

        catch (Exception e)
        {
            System.out.println(e);
        }
    }

    public static void initial(int number_of_senders, int number_of_checksummers, int blocksize)
    {
        changeThreads("blocksize","increment", blocksize);
        changeThreads("transfer","increment", number_of_senders);
        changeThreads("checksum","increment", number_of_checksummers);

        ChecksumReceiverRunnable checksumReceiverRunnable = new ChecksumReceiverRunnable(1);
    }


    public DynamicSender (String host, int port, String path)
    {
        filereader(path);

        initial(5,3,5);
        DynamicCommon.sleeper(1000);




        long lastTransferredBytes = 0;
        long lastChecksumBytes = 0;

        while(!allFileTransfersCompleted)
        {

            double transferThrInMbps = 8 * (totalTransferredBytes-lastTransferredBytes)/(1024*1024);
            double checksumThrInMbps = 8 * (totalChecksumBytes-lastChecksumBytes)/(1024*1024);

            System.out.println("Transfer throughput:" + transferThrInMbps + "Mb/s \t Checksum throughput:" + checksumThrInMbps + " Mb/s");
            lastTransferredBytes = totalTransferredBytes;
            lastChecksumBytes = totalChecksumBytes;







            String operation_transfer="nothing done for transfer";
            String operation_blocksize="nothing done for blocksize";
            String operation_checksum="nothing done for checksum";


            String increase_decrease="increment";



            int random = rand.nextInt(50)+1; //[1,50]

            if (random % 2 == 1) increase_decrease = "decrement";
            if ((increase_decrease.equals("decrement") && random >= sender_thread_id)) {
            } else if ((increase_decrease.equals("increment") && (random + sender_thread_id) > 50)) {
            } else {
                changeThreads("transfer", increase_decrease, random);
                operation_transfer="transfer "+increase_decrease+" by "+random;
            }


            increase_decrease="increment";
            random = rand.nextInt(5)+1; //[1,5]
            if(random%2==1) increase_decrease="decrement";
            if((increase_decrease.equals("decrement") && random >= INTEGRITY_VERIFICATION_BLOCK_SIZE/(1024*1024))) { }
            else if((increase_decrease.equals("increment") && (random + INTEGRITY_VERIFICATION_BLOCK_SIZE/(1024*1024))>=10)) { }
            else
            {
                changeThreads("blocksize", increase_decrease, random);
                operation_blocksize="blocksize "+increase_decrease+" by "+random;
            }

            random = (int )(Math.random() * 30 + 1);
            if(random%2==1) increase_decrease="decrement";
            if((increase_decrease.equals("decrement") && random >= checksum_thread_id)) { }
            else if ((increase_decrease.equals("increment") && (random + checksum_thread_id) > 50)) { }
            else
            {
                changeThreads("checksum", increase_decrease, random);
                operation_checksum="checksum "+increase_decrease+" by "+random;
            }


            DynamicCommon.sleeper(1000);
            //System.out.println("#transferThreads="+senderThreads.size()+"\tsthreadIDmax="+sender_thread_id+"\tblocksize="+INTEGRITY_VERIFICATION_BLOCK_SIZE/(1024*1024));//+"\t#checksum="+checksumThreads.size());




            String printlineOperations="<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n";
            printlineOperations=printlineOperations+operation_transfer+"\t"+operation_blocksize+"\t"+operation_checksum;
           // printlineOperations=printlineOperations+"\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>";

            String printline="------------------------------------------------------\n";
            printline=printline+"senderThreads_size="+senderThreads.size()+"\tsenderThreads_tobedeleted_size="+senderThreads_tobedeleted.size();
            printline=printline+"\tchecksumThreads_size="+checksumThreads.size()+"\tblocksize="+INTEGRITY_VERIFICATION_BLOCK_SIZE/(1024*1024);
            printline=printline+"\t\t\tsender_thread_id="+sender_thread_id+"\t\t\tchecksum_thread_id="+checksum_thread_id;
            printline=printline+"\nfiles_size="+files.size()+"\t\tchecksumFiles_size="+checksumFiles.size()+"\t\tchecksums_being_computed_size="+checksums_being_computed.size();
            printline=printline+"\nfiles_being_transferred_size="+files_being_transferred.size()+"\t\tcomputed_checksums_size="+computed_checksums.size();
            printline=printline+"\tcomputed_checksums_fiverFile_size="+computed_checksums_fiverFile.size()+"\t\t\treceived_checksums_size="+received_checksums.size();
            //printline=printline+"\n---------------------------------------------------";



            String counter_printline="+++++++++++++++++++++++++++++++++++++++++++++++++\n";
            counter_printline=counter_printline+"total_files_sent="+total_files_sent+"\ttotal_checksum_files="+total_checksum_files;
            counter_printline=counter_printline+"\ttotal_checksums_computed="+total_checksums_computed+"\ttotal_checksums_received="+total_checksums_received;
            counter_printline=counter_printline+"\ttotal_checksums_matched="+total_checksums_matched;
            counter_printline=counter_printline+"\n++++++++++++++++++++++++++++++++++++++++++++++++++";


            System.out.println(printlineOperations);
            System.out.println(printline);
            System.out.println(counter_printline);

        }

        System.out.println("finished");
        changeThreads("transfer","decrement", sender_thread_id-1);
        changeThreads("checksum","decrement", checksum_thread_id-1);
    }


    public static void main(String[] args)
    {
        String path="/home/bcharyyev/Desktop/RIVA_Dynamic/s/";
        destination_ip="localhost";
        base_directory=path;
        port=2008;

        DynamicSender ds = new DynamicSender(destination_ip, port, base_directory);
    }

    public static void filereader(String path)
    {
        File file = new File(path);

        if(file.isDirectory())
        {
            for (File f : file.listFiles())
            {
                files.add(new DynamicCommon.FiverFile(f, 0, f.length(), fiver_id++));
            }
        }
        else
        {
            files.add(new DynamicCommon.FiverFile(file, 0, file.length(), fiver_id++));
        }
    }
}