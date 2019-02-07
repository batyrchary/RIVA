import javax.sound.midi.Receiver;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DynamicReceiver {


    public static boolean allFileTransfersCompleted = false;

    public static LinkedList<DynamicCommon.FiverFile> files=new LinkedList<>();
    public static LinkedList<ServerSocket> servers=new LinkedList<>();

    public static LinkedList<ReceiverRunnable> receiverThreads=new LinkedList<>();
    public static LinkedList<Socket> sockets=new LinkedList<>();





    public static String baseDir="-1";
    public static int port=-1;
    public static long INTEGRITY_VERIFICATION_BLOCK_SIZE = 0;  //256 * 1024 * 1024;

    public static LinkedBlockingQueue<DynamicCommon.FiverFile> checksumFiles=new LinkedBlockingQueue<>();

    public static int threadID=0;


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
                byte[] buffer = new byte[128 * 1024];
                while (!shouldIfinish) {
                    String fileName = dataInputStream.readUTF();
                    long offset = dataInputStream.readLong();
                    long fileSize = dataInputStream.readLong();

                    checksumFiles.offer(new DynamicCommon.FiverFile(new File(baseDir + fileName), offset, fileSize));


                    RandomAccessFile randomAccessFile = new RandomAccessFile(baseDir + fileName, "rw");

                    if (offset > 0) {
                        randomAccessFile.getChannel().position(offset);
                    }
                    long remaining = fileSize;
                    int read = 0;
                    long transferStartTime = System.currentTimeMillis();
                    while (remaining > 0) {
                        read = dataInputStream.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                        if (read == -1)
                            break;
                        remaining -= read;
                        randomAccessFile.write(buffer, 0, read);
                    }
                    randomAccessFile.close();
                    if (read == -1) {
                        System.out.println("Read -1, closing the connection...");
                        return;
                    }
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
                    for(int i=0; i<howmuch; i++)
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

            }
            else if (type.equals("decrement"))
            {

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



    public DynamicReceiver (int port, String path)
    {
        try {

            ss = new ServerSocket(2008);
      /*      while (true)
            {
                Socket clientSock = ss.accept();
                clientSock.setSoTimeout(10000);
                System.out.println("Connection established from  " + clientSock.getInetAddress());
                ReceiverRunnable receiver= new ReceiverRunnable(clientSock);
            }
        */}catch (Exception e){e.printStackTrace();}



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
