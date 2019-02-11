import javax.sound.midi.Receiver;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class DynamicReceiver {


    public static boolean allFileTransfersCompleted = false;

    public static LinkedList<DynamicCommon.FiverFile> files=new LinkedList<>();
    public static LinkedList<ServerSocket> servers=new LinkedList<>();

    public static LinkedList<ReceiverRunnable> receiverThreads=new LinkedList<>();
    public static LinkedList<ChecksumRunnable> checksumThreads=new LinkedList<>();

    public static LinkedList<Socket> sockets=new LinkedList<>();





    public static String baseDir="-1";
    public static int port=-1;
    public static long INTEGRITY_VERIFICATION_BLOCK_SIZE = 0;  //256 * 1024 * 1024;

    public static LinkedBlockingQueue<DynamicCommon.FiverFile> checksumFiles=new LinkedBlockingQueue<>();

    public static int threadID=0;
    public static int CHthreadID=0;


    public static ServerSocket ss;









    public static void tune()
    {
    }







    public static class ReceiverRunnable implements Runnable
    {
        int threadID;
        boolean shouldIfinish=false;
        Thread t;
        Socket clientSock;
        //long blockSize;

        public ReceiverRunnable(int threadID, Socket s)
        {
            this.threadID=threadID;
            this.clientSock=s;
            t=new Thread(this);
            t.start();
        }

        @Override
        public void run() {
            try
            {
                DataInputStream dataInputStream = new DataInputStream(clientSock.getInputStream());

                while (!shouldIfinish)
                {
                    if(dataInputStream.available()==0)
                        continue;
                    String fileName = dataInputStream.readUTF();
                    long offset = dataInputStream.readLong();
                    long fileSize = dataInputStream.readLong();


                    RandomAccessFile randomAccessFile = new RandomAccessFile(baseDir + fileName, "rw");

                    if (offset > 0) {
                        randomAccessFile.getChannel().position(offset);
                    }
                    long remaining = fileSize;
                    int read = 0;


                    byte[] bufferChecksum = new byte[Math.toIntExact(remaining)];
                    int bufferindex=0;

                    while (remaining > 0) {

                        byte[] buffer = new byte[128 * 1024];

                        read = dataInputStream.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                        if (read == -1)
                            break;
                        remaining -= read;
                        randomAccessFile.write(buffer, 0, read);

                        for(int bi=0; bi<read; bi++)
                        {
                            bufferChecksum[bufferindex+bi]=buffer[bi];
                        }

                        bufferindex=bufferindex+read;

                    }
                    randomAccessFile.close();
                    if (read == -1) {
                        System.out.println("Read -1, closing the connection...");
                        return;
                    }
                    checksumFiles.offer(new DynamicCommon.FiverFile(new File(baseDir + fileName), offset, fileSize, bufferChecksum));

                }
                dataInputStream.close();
                clientSock.close();
            } catch (Exception e) {
                System.out.println("exeception in saver");
                e.printStackTrace();
            }
        }
    }


    public static void changeThreads(String which, String type, int howmuch)
    {
        if(which.equals("transfer"))
        {
            if (type.equals("increment"))
            {
                try
                {
                    while (true)
                    {
                        Socket clientSock = ss.accept();
                        clientSock.setSoTimeout(10000);
                        System.out.println("Connection established from  " + clientSock.getInetAddress());
                        sockets.add(clientSock);

                        ReceiverRunnable receiver= new ReceiverRunnable(threadID++, clientSock);
                        receiverThreads.add(receiver);
                    }
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
            else if (type.equals("decrement"))
            {
                for(int i=0; i<howmuch; i++)
                {
                    sockets.remove(sockets.size() - 1);
                    receiverThreads.get(receiverThreads.size() - 1).shouldIfinish = true; //finish last thread
                    receiverThreads.remove(receiverThreads.size() - 1);            //last one
                }
            }
        }



        else if(which.equals("checksum"))
        {
            if (type.equals("increment"))
            {
                for (int i = 0; i < howmuch; i++)
                {
                    ChecksumRunnable checksumRunnable = new ChecksumRunnable(CHthreadID++);
                    checksumThreads.add(checksumRunnable);
                }

            }
            else if (type.equals("decrement"))
            {
                for (int i = 0; i < howmuch; i++) {
                    checksumThreads.remove(checksumThreads.size() - 1);
                    checksumThreads.get(checksumThreads.size() - 1).shouldIfinish = true; //finish last thread
                    checksumThreads.remove(checksumThreads.size() - 1);            //last one
                }

            }
        }
    }





    public static void initial(int number_of_receivers, int number_of_checksummers, int blocksize )
    {
        //changeThreads("blocksize","increment", blocksize);
        changeThreads("transfer","increment", number_of_receivers);
        changeThreads("checksum","increment", number_of_checksummers);

        DynamicCommon.sleeper(3000);
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


        long totalChecksumTime = 0;

        @Override
        public void run() {

            try {
                md = MessageDigest.getInstance("MD5");

                md.reset();
                DataOutputStream dataOutputStream = null;

                ServerSocket socket = new ServerSocket(20180);
                dataOutputStream = new DataOutputStream(socket.accept().getOutputStream());
                System.out.println("Checksum Connection accepted");


                while (!shouldIfinish) {
                    DynamicCommon.FiverFile fiverFile = null;

                    fiverFile = checksumFiles.poll(Long.MAX_VALUE, TimeUnit.SECONDS);

                    if (fiverFile == null) {
                        continue;
                    }
                    String fileName = fiverFile.file.getName();
                    long fileOffset = fiverFile.offset;
                    long fileLength = fiverFile.length;

                    md.update(fiverFile.buffer, 0, Math.toIntExact(fiverFile.length));
                    byte[] digest = md.digest();

                    //String hex = (new HexBinaryAdapter()).marshal(digest);
                    String hex = "aa";

                    System.out.println("Sending hex:" + hex + " bytes:" + fileLength);
                    dataOutputStream.writeUTF(hex);
                    md.reset();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public DynamicReceiver (int port, String path)
    {
        try {
            ss = new ServerSocket(2008);
        }catch (Exception e){e.printStackTrace();}



        initial(1,1,256);


        /*
        DynamicCommon.sleeper(100);
        while(!files.isEmpty())
        {
            // tune();
            DynamicCommon.sleeper(100);
            // System.out.println("karoce burdayim");
        }

        System.out.println("finished");
        changeThreads("transfer","decrement", senderThreads.size());
        */

    }




    public static void main(String[] args)
    {
        //String path = args[1];

        String path="/Users/batyrchary/Desktop/journalFIVER/receiver/";
        baseDir=path;
        port=2008;

        DynamicReceiver dc = new DynamicReceiver(port, path);
    }
}
