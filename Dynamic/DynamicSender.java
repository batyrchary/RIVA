

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DynamicSender {


    public static boolean allFileTransfersCompleted = false;

    public static LinkedList<DynamicCommon.FiverFile> files=new LinkedList<>();
    public static LinkedList<Socket> sockets=new LinkedList<>();
    public static LinkedList<SenderRunnable> senderThreads=new LinkedList<>();
    public static LinkedList<ChecksumRunnable> checksumThreads=new LinkedList<>();

    public static String destIp="-1";
    public static String baseDir="-1";
    public static int port=-1;
    public static long INTEGRITY_VERIFICATION_BLOCK_SIZE = 0;  //256 * 1024 * 1024;

    public static LinkedBlockingQueue<DynamicCommon.FiverFile> checksumFiles=new LinkedBlockingQueue<>(10000);
    public static LinkedBlockingQueue<DynamicCommon.Item> items = new LinkedBlockingQueue<>(10000);


    public static int threadID=0;
    public static int CHthreadID=0;




    public static void tune()
    {
    }







    public static class SenderRunnable implements Runnable
    {
        int threadID;
        boolean shouldIfinish=false;
        Thread t;
        Socket s;
        //long blockSize;

        public SenderRunnable(int threadID, Socket s)
        {
            this.threadID=threadID;
            this.s=s;
            t=new Thread(this);
            t.start();
        }

        @Override
        public void run()
        {
            try {
                DataOutputStream dos = new DataOutputStream(s.getOutputStream());
                dos = new DataOutputStream(s.getOutputStream());

                byte[] buffer = new byte[128 * 1024];
                int n;
                while (!shouldIfinish)
                {
                    DynamicCommon.FiverFile currentFile = null;
                    synchronized (files)
                    {
                        if (!files.isEmpty())
                        {
                            DynamicCommon.FiverFile ff=files.get(0);
                            long len=INTEGRITY_VERIFICATION_BLOCK_SIZE;

                            if((ff.offset+INTEGRITY_VERIFICATION_BLOCK_SIZE) > ff.length)
                            {
                                currentFile = files.remove(0);
                                len=ff.length-ff.offset;
                            }
                            else
                            {
                                currentFile=new DynamicCommon.FiverFile(ff);
                                ff.offset=ff.offset+INTEGRITY_VERIFICATION_BLOCK_SIZE;
                                files.set(0,ff);
                            }
                            currentFile.length=len;
                        }
                    }
                    if (currentFile == null)
                    {
                        Thread.sleep(100);
                        continue;
                    }


                    //send file metadata
                    String currentFileName = currentFile.file.getName();
                    dos.writeUTF(currentFileName);
                    dos.writeLong(currentFile.offset);
                    dos.writeLong(currentFile.length);

                    FileInputStream fis = new FileInputStream(currentFile.file);
                    if (currentFile.offset > 0) {
                        fis.getChannel().position(currentFile.offset);
                    }

                    Long remaining = currentFile.length;

                    byte[] bufferChecksum = new byte[Math.toIntExact(remaining)];

                    int bufferindex=0;

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

                        for(int bi=0; bi<n; bi++)
                        {
                            bufferChecksum[bufferindex+bi]=buffer[bi];
                        }

                        bufferindex=bufferindex+n;
                    }
                    currentFile.buffer=bufferChecksum;
                    checksumFiles.offer(currentFile);


                    fis.close();
                    System.out.println(currentFile.offset+"\t"+currentFile.file.getName());
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

        Socket s;

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
        public void run() {

            try {


                DataInputStream dataInputStream = null;
                while (true) {
                    try {
                        s = new Socket(destIp, 20180);
                        dataInputStream = new DataInputStream(s.getInputStream());
                        break;
                    } catch (IOException e) {
                        System.out.println("Trying to connect to checksum thread");
                        DynamicCommon.sleeper(100);
                    }
                }

                md = MessageDigest.getInstance("MD5");
                md.reset();

                while (!shouldIfinish)
                {
                    DynamicCommon.FiverFile currentFile = null;
                    synchronized (checksumFiles)
                    {
                        currentFile = checksumFiles.poll(1000, TimeUnit.SECONDS);
                    }
                    if (currentFile == null)
                    {
                        DynamicCommon.sleeper(100);
                        continue;
                    }

                    md.update(currentFile.buffer, 0, Math.toIntExact(currentFile.length));

                    byte[] digest = md.digest();
                    //String hex = (new HexBinaryAdapter()).marshal(digest);
                    String hex = "aa";
                    String destinationHex = dataInputStream.readUTF();
                    if (hex.compareTo(destinationHex) != 0) { // Checksums dont match

                        synchronized (files) {
                            files.add(new DynamicCommon.FiverFile(currentFile));
                            System.out.println("File added " + files.size() + " files left");
                        }
                    }

                    md.reset();
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }



    public static void changeThreads(String which, String type, int howmuch) {
        try {

            if (which.equals("transfer")) {
                if (type.equals("increment")) {

                    for (int i = 0; i < howmuch; i++) {
                        Socket s = new Socket(destIp, port);
                        s.setSoTimeout(10000);
                        sockets.add(s);

                        SenderRunnable sender = new SenderRunnable(threadID++, s);
                        senderThreads.add(sender);
                    }
                }
                else if (type.equals("decrement")) {
                    for (int i = 0; i < howmuch; i++) {
                        sockets.remove(sockets.size() - 1);
                        senderThreads.get(senderThreads.size() - 1).shouldIfinish = true; //finish last thread
                        senderThreads.remove(senderThreads.size() - 1);            //last one
                    }
                }
            }



            else if (which.equals("checksum")) {
                if (type.equals("increment")) {

                    for (int i = 0; i < howmuch; i++)
                    {
                        ChecksumRunnable checksumRunnable = new ChecksumRunnable(CHthreadID++);
                        checksumThreads.add(checksumRunnable);
                    }
                } else if (type.equals("decrement")) {

                    for (int i = 0; i < howmuch; i++) {
                        checksumThreads.remove(checksumThreads.size() - 1);
                        checksumThreads.get(checksumThreads.size() - 1).shouldIfinish = true; //finish last thread
                        checksumThreads.remove(checksumThreads.size() - 1);            //last one
                    }

                }
            } else if (which.equals("blocksize")) {
                if (type.equals("increment")) {
                    INTEGRITY_VERIFICATION_BLOCK_SIZE = INTEGRITY_VERIFICATION_BLOCK_SIZE + howmuch;
                } else if (type.equals("decrement")) {
                    INTEGRITY_VERIFICATION_BLOCK_SIZE = INTEGRITY_VERIFICATION_BLOCK_SIZE - howmuch;
                }
            }
        } catch (Exception e) {
        System.out.println(e);
        }
    }





    public static void initial(int number_of_senders, int number_of_checksummers, int blocksize )
    {
        changeThreads("blocksize","increment", blocksize);
        changeThreads("transfer","increment", number_of_senders);
        changeThreads("checksum","increment", number_of_checksummers);

        DynamicCommon.sleeper(3000);
    }



    public DynamicSender (String host, int port, String path)
    {
        filereader(path);

        initial(2,1,256);

        DynamicCommon.sleeper(100);
        while(!files.isEmpty())
        {
           // tune();
            DynamicCommon.sleeper(100);
        }

        System.out.println("finished");
        changeThreads("transfer","decrement", senderThreads.size());
    }




    public static void main(String[] args)
    {
        //server ss=new server();

        //destIp = args[0];
        //String path = args[1];


        destIp="localhost";
        String path="/Users/batyrchary/Desktop/journalFIVER/sender/";
        baseDir=path;
        port=2008;

        DynamicSender dc = new DynamicSender(destIp, port, path);

        /*
        try {
            ss.t.join();
        }catch (Exception e){e.printStackTrace();}
        */
    }







    public static void filereader(String path)
    {
        File file = new File(path);
        files = new LinkedList<>();

        if(file.isDirectory())
        {
            for (File f : file.listFiles())
            {
                files.add(new DynamicCommon.FiverFile(f, 0, f.length(),null));
            }
        }
        else
        {
            files.add(new DynamicCommon.FiverFile(file, 0, file.length(),null));
        }

    }
}
