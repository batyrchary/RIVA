import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
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

    public static volatile LinkedList<DynamicCommon.FiverFile> files=new LinkedList<>();
    public static LinkedBlockingQueue<DynamicCommon.FiverFile> checksumFiles=new LinkedBlockingQueue<>(10000);

    public static volatile Map<Long, String> computed_checksums = new HashMap<>();
    public static volatile Map<Long, DynamicCommon.FiverFile> computed_checksums_fiverFile = new HashMap<>();
    public static volatile Map<Long, String> received_checksums = new HashMap<>();


    public static String destination_ip="-1";
    public static String base_directory="-1";
    public static int port=-1;


    public static volatile Long INTEGRITY_VERIFICATION_BLOCK_SIZE = 0l;  //256 * 1024 * 1024;

    public static volatile Integer sender_thread_id=1;
    public static volatile Integer checksum_thread_id=1;

    public static volatile Long fiver_id=0l;

    public static volatile boolean allFileTransfersCompleted = false;


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

                    //send file metadata
                    System.out.println("sending from thread=" + this.threadID+"\tnumber of threads="+senderThreads.size());
                    String currentFileName = currentFile.file.getName();
                    dos.writeUTF(currentFileName);
                    dos.writeLong(currentFile.offset);
                    dos.writeLong(currentFile.length);
                    dos.writeLong(currentFile.id);

                    FileInputStream fis = new FileInputStream(currentFile.file);
                    if (currentFile.offset > 0)
                    {
                        fis.getChannel().position(currentFile.offset);
                    }

                    Long remaining = currentFile.length;

                    byte[] bufferChecksum = new byte[Math.toIntExact(remaining)];

                    int bufferindex = 0;

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

                        for (int bi = 0; bi < n; bi++) {
                            bufferChecksum[bufferindex + bi] = buffer[bi];
                        }

                        bufferindex = bufferindex + n;
                    }

                    currentFile.buffer = bufferChecksum;


                    checksumFiles.offer(currentFile);


                    fis.close();
                }
                dos.writeUTF("done");
                //dos.close();

                synchronized (senderThreads_tobedeleted)
                {
                    senderThreads_tobedeleted.remove(this.threadID);
                    System.out.println("i am exiting transfer thread="+threadID+"\tsthreadID="+sender_thread_id);
                }

            } catch (Exception e){ System.out.println("cant get output stream"); }
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

                while (!shouldIfinish)
                {
                    DynamicCommon.FiverFile currentFile = null;
                //    synchronized (checksumFiles)
                //    {
                        currentFile = checksumFiles.poll(1, TimeUnit.SECONDS);
                 //   }
                    if (currentFile == null)
                    {
                    //    DynamicCommon.sleeper(100);
                        continue;
                    }

                    //md.update(currentFile.buffer, 0, Math.toIntExact(currentFile.length));

                    String tobedigested="anasinisatim";
                    //md.update(tobedigested.getBytes(),0,tobedigested.length());

                    //byte[] digest = md.digest();
                    //String hex = (new HexBinaryAdapter()).marshal(digest);
                    //String hex = digest.toString();

                    String hex=tobedigested;

                    currentFile.buffer=null;
                    //synchronized (computed_checksums)
                    //{
                        computed_checksums.put(currentFile.id, hex);
                        computed_checksums_fiverFile.put(currentFile.id, currentFile);
                    //}
                    md.reset();
                }
                System.out.println("checksum thread is exiting="+threadID);
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

        public ChecksumReceiverRunnable(int threadID)
        {
            try
            {
                this.threadID = threadID;
                s = new Socket(destination_ip, 20180);

                t = new Thread(this);
                t.start();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        @Override
        public void run()
        {
            try
            {
                DataInputStream dataInputStream = new DataInputStream(s.getInputStream());

                long destination_file_id = dataInputStream.readLong();
                String destinationHex = dataInputStream.readUTF();

                if(computed_checksums.containsKey(destination_file_id))
                {
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
                        synchronized (files)
                        {
                            files.add(newFile);
                            System.out.println("File added " + files.size() + " files left");
                        }
                    }
                }
                else
                {
                    received_checksums.put(destination_file_id,destinationHex);
                }


                for ( Long destination_file_id1 : received_checksums.keySet())
                {
                    destinationHex = received_checksums.get(destination_file_id1);

                    if(computed_checksums.containsKey(destination_file_id1))
                    {
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
                            synchronized (files)
                            {
                                files.add(newFile);
                                System.out.println("File added " + files.size() + " files left");
                            }
                        }
                    }
                }
            }
            catch (Exception e)
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
        DynamicCommon.sleeper(100);

        while(!allFileTransfersCompleted)
        {
            DynamicCommon.sleeper(100);

            String increase_decrease="increment";

            int random = rand.nextInt(50)+1; //[1,50]

            if (random % 2 == 1) increase_decrease = "decrement";
            if ((increase_decrease.equals("decrement") && random >= sender_thread_id)) {
            } else if ((increase_decrease.equals("increment") && (random + sender_thread_id) > 50)) {
            } else {
                System.out.println("operation=" + increase_decrease + "\trandom=" + random);
                changeThreads("transfer", increase_decrease, random);
            }

            increase_decrease="increment";
            random = (int )(Math.random() * 10 + 1);
            if(random%2==1) increase_decrease="decrement";
            if((increase_decrease.equals("decrement") && random >= INTEGRITY_VERIFICATION_BLOCK_SIZE)) { }
            else if((increase_decrease.equals("increment") && (random + INTEGRITY_VERIFICATION_BLOCK_SIZE)>=10*1024*1024)) { }
            else
            {

                changeThreads("blocksize", increase_decrease, random);
            }

            random = (int )(Math.random() * 30 + 1);
            if(random%2==1) increase_decrease="decrement";
            if((increase_decrease.equals("decrement") && random >= checksum_thread_id)) { }
            else if ((increase_decrease.equals("increment") && (random + checksum_thread_id) > 50)) { }
            else
            {
                changeThreads("checksum", increase_decrease, random);
            }


            DynamicCommon.sleeper(100);
            System.out.println("#transferThreads="+senderThreads.size()+"\tsthreadIDmax="+sender_thread_id+"\tblocksize="+INTEGRITY_VERIFICATION_BLOCK_SIZE/(1024*1024));//+"\t#checksum="+checksumThreads.size());
        }

        System.out.println("finished");
        changeThreads("transfer","decrement", sender_thread_id-1);
        changeThreads("checksum","decrement", checksum_thread_id-1);
    }


    public static void main(String[] args)
    {
        String path="/Users/batyrchary/Desktop/projects/RIVA_Dynamic/s/";
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
                files.add(new DynamicCommon.FiverFile(f, 0, f.length(),null, fiver_id++));
            }
        }
        else
        {
            files.add(new DynamicCommon.FiverFile(file, 0, file.length(),null, fiver_id++));
        }
    }
}