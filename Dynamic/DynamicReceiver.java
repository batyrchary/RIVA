import javax.sound.midi.Receiver;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class DynamicReceiver
{


    public static String baseDir="-1";
    public static int port=-1;



    public static volatile Map<Integer, ReceiverRunnable> receiverThreads=new HashMap<>();
    public static volatile Map<Integer, ChecksumRunnable>  checksumThreads = new HashMap<>();


    public static volatile LinkedBlockingQueue<DynamicCommon.FiverFile> checksumFiles=new LinkedBlockingQueue<>();

    public static volatile int receiver_thread_id=1;
    public static volatile Integer checksum_thread_id=1;


    public static volatile ServerSocket checksumServer;
    public static volatile Socket checksumSocket;

    public static volatile Integer lock_for_checksumFiles_inChecksum_threads=1;


    public static volatile Long totalTransferredBytes = 0l;
    public static volatile Long totalChecksumBytes = 0l;

    public static volatile boolean allFileTransfersCompleted = false;

    public static class ReceiverRunnable implements Runnable
    {
        int threadID;
        boolean shouldIfinish=false;
        Thread t;
        ServerSocket ss;

        public ReceiverRunnable(int receiverthreadid)
        {
            try
            {
                this.threadID=receiverthreadid;
                ss = new ServerSocket(port+receiverthreadid);
                t=new Thread(this);
                t.start();
            }
            catch (Exception e){e.printStackTrace();}
        }

        @Override
        public void run()
        {
            try
            {
                while (!allFileTransfersCompleted)
                {
                    Socket clientSock = ss.accept();
                    //System.out.println("Connection established from  " + clientSock.getInetAddress());

                    DataInputStream dataInputStream = new DataInputStream(clientSock.getInputStream());

                    while (!shouldIfinish)
                    {
                        if (dataInputStream.available() == 0)
                            continue;

                        String fileName = dataInputStream.readUTF();
                        if (fileName.equals("done"))
                        {
                        //    System.out.println("done received by thread=" + threadID);
                            break;
                        }
                        Long offset = dataInputStream.readLong();
                        Long fileSize = dataInputStream.readLong();
                        Long file_id = dataInputStream.readLong();

                        DynamicCommon.FiverFile checkfile = new DynamicCommon.FiverFile(new File(baseDir + fileName), offset, fileSize, file_id);

                        String checkprint="file"+checkfile.file+"\toffset="+checkfile.offset;
                        checkprint=checkprint+"\t"+"length="+checkfile.length+"\tid="+checkfile.id;

                       // System.out.println("checkprint=\t"+checkprint);

                        String checkprint1="file"+fileName+"\toffset="+offset;
                        checkprint1=checkprint1+"\t"+"length="+fileSize+"\tid="+file_id;

                        //System.out.println("checkprint1=\t"+checkprint1);


                        RandomAccessFile randomAccessFile = new RandomAccessFile(baseDir + fileName, "rw");

                        if (offset > 0)
                        {
                            randomAccessFile.getChannel().position(offset);
                        }

                        long remaining = fileSize;
                        int read = 0;

                        while (remaining > 0)
                        {
                            byte[] buffer = new byte[128 * 1024];

                            read = dataInputStream.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                            if (read == -1)
                                break;

                            remaining -= read;

                            randomAccessFile.write(buffer, 0, read);

                            synchronized (totalTransferredBytes)
                            {
                                totalTransferredBytes=totalTransferredBytes+read;
                            }

                        }
                        randomAccessFile.close();
                        if (read == -1)
                        {
                            System.out.println("Read -1, closing the connection...");
                            return;
                        }

                        synchronized (checksumFiles)
                        {
                            checksumFiles.offer(checkfile);
                        }

                        CacheEvictRunnable cacheEvictRunnable = new CacheEvictRunnable(checkfile);
                        new Thread(cacheEvictRunnable).start();
                    }
                    dataInputStream.close();

                    clientSock.close();

                }
                } catch(Exception e){
                    System.out.println("exeception in saver");
                    e.printStackTrace();
                }

        }
    }


    public static class CacheEvictRunnable implements Runnable
    {
        DynamicCommon.FiverFile fiverFile;
        public CacheEvictRunnable(DynamicCommon.FiverFile fiverFile)
        {
            this.fiverFile = fiverFile;
        }

        public void run ()
        {
            try
            {
                long start = (fiverFile.offset / (1024*1024));
                long end = (fiverFile.offset + fiverFile.length) / (1024*1024);

                String evictionRange = start + "m-" + end + "m";

                while(InCache(fiverFile.file.getPath(),evictionRange))
                {
                    String cmd2[] = {"vmtouch", "-e", "-p", evictionRange, fiverFile.file.getPath()};
                    Process proc2 = Runtime.getRuntime().exec(cmd2);

                    proc2.waitFor();
                }

                //////////////////////////////////////////////////////////////////////////////////
                //IMPORTANT -- DONT UNCOMMENT THIS PART UNLESS YOU SURE WHAT YOU DOING//
                //IT WILL MAKE FAULT INJECTION TO TRANSFERRED FILES AND IF YOU DID NOT//
                //PROVIDE CORRECT FILESYSTEM NAME AND BASE DIRECTORY IT CAN MESS YOUR SYSTEM//
                //injectToFiverFile(fiverFile);
                //////////////////////////////////////////////////////////////////////////

                synchronized (fiverFile)
                {
                    fiverFile.isEvicted = true;
                }
            }
            catch (Exception e)
            {
                System.out.println("Exception in cache evict");
            }
        }
    }


    public static boolean InCache(String filename, String range)
    {
        String percentage = "";
        try
        {
            String cmd2[] = {"/bin/bash", "-c", "vmtouch -p " + range + " " + filename};
            Process proc2 = Runtime.getRuntime().exec(cmd2);

            // System.out.println(Arrays.toString(cmd2));
            BufferedReader input2 = new BufferedReader(new InputStreamReader(proc2.getInputStream()));

            String line;
            while ((line = input2.readLine()) != null)
            {
                if (line.contains("Resident"))
                {
                    line = line.replaceAll("\\s+", ",");
                    String splitted[] = line.split(",");
                    percentage = splitted[splitted.length - 1];
                    break;
                }
            }
            input2.close();
        }
        catch (Exception e)
        {
            System.out.println("Exception in cache check");
        }

        float p;
        try
        {
            p = Float.parseFloat(percentage.split("%")[0]);
        }
        catch (Exception e)
        {
            System.out.println("Exception inCache fun for input: range=" + range + "\tfilename=" + filename + "\tp=" + percentage);
            p = 3;
        }
        if (p < 1)
            return false;
        else
            return true;
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

        @Override
        public void run()
        {
            try
            {
                DataOutputStream dataOutputStream = new DataOutputStream(checksumSocket.getOutputStream());

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
                        continue;
                    }
                    while (true) {
                        synchronized (currentFile) {
                            if (currentFile.isEvicted)
                                break;
                        }
                        Thread.sleep(10);
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

                    synchronized (checksumSocket)
                    {
                        //System.out.println(currentFile.id+","+hex);

                        dataOutputStream.writeLong(currentFile.id);
                        dataOutputStream.writeUTF(hex);
                    }
                }


            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }



    public static void changeThreads(String which, String type, int howmuch)
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


    public DynamicReceiver ()
    {

        changeThreads("checksum","increment", 10);

        DynamicCommon.sleeper(1000);

        long lastReceivedBytes = 0;
        long lastReadBytes = 0;


        int counter=0;
        while(true)
        {

            double transferThrInMbps = 8 * (totalTransferredBytes - lastReceivedBytes) / (1024*1024);
            double checksumThrInMbps = 8 * (totalChecksumBytes - lastReadBytes) / (1024*1024);
            System.out.println("Transfer throughput:" + transferThrInMbps + "Mb/s \t Checksum throughput:" + checksumThrInMbps + " Mb/s");
            lastReceivedBytes = totalTransferredBytes;
            lastReadBytes = totalChecksumBytes;


            if(transferThrInMbps==0 && checksumThrInMbps==0)
            {
                counter++;
            }
            else
            {
                counter=0;
            }
            if(counter==5)//5seconds passed
            {
                break;
            }



            String increase_decrease="increment";

            int random = (int )(Math.random() * 30 + 1);
            if(random%2==1) increase_decrease="decrement";
            if((increase_decrease.equals("decrement") && random >= checksumThreads.size())) { }
            else if ((increase_decrease.equals("increment") && (random + checksumThreads.size()) > 50)) { }
            else
            {
                changeThreads("checksum", increase_decrease, random);
            }

            DynamicCommon.sleeper(1000);
        }
        allFileTransfersCompleted=true;
        changeThreads("checksum","decrement", checksum_thread_id-1);


        for(int i=0; i<50; i++)
        {
            int rthreadIDTobeDeleted = receiver_thread_id-1;
            receiverThreads.get(rthreadIDTobeDeleted).shouldIfinish=true;

            receiverThreads.remove(rthreadIDTobeDeleted);
            receiver_thread_id=rthreadIDTobeDeleted;
        }

    }

    public static void main(String[] args)
    {
        String path="/home/bcharyyev/Desktop/RIVA_Dynamic/r/";
        baseDir=path;
        port=2008;

        for(int i=0; i<50; i++)
        {
            ReceiverRunnable receiver = new ReceiverRunnable(receiver_thread_id);
            receiverThreads.put(receiver_thread_id,receiver);
            receiver_thread_id++;
        }


        try {
            checksumServer = new ServerSocket(2060);
            checksumSocket = checksumServer.accept();

        } catch (IOException e) {
            e.printStackTrace();
        }


        DynamicReceiver dr = new DynamicReceiver();
    }
}
