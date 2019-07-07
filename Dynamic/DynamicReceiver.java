import javax.sound.midi.Receiver;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
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
    public static volatile int receiver_thread_id=1;



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
            try {
                while (true)
                {
                    Socket clientSock = ss.accept();
                    System.out.println("Connection established from  " + clientSock.getInetAddress());

                    DataInputStream dataInputStream = new DataInputStream(clientSock.getInputStream());

                    while (!shouldIfinish) {
                        if (dataInputStream.available() == 0)
                            continue;

                        String fileName = dataInputStream.readUTF();
                        if (fileName.equals("done")) {
                            System.out.println("done received by thread=" + threadID);
                            break;
                        }
                        long offset = dataInputStream.readLong();
                        long fileSize = dataInputStream.readLong();
                        long file_id = dataInputStream.readLong();

                        RandomAccessFile randomAccessFile = new RandomAccessFile(baseDir + fileName, "rw");

                        if (offset > 0) {
                            randomAccessFile.getChannel().position(offset);
                        }

                        long remaining = fileSize;
                        int read = 0;

                        while (remaining > 0) {
                            byte[] buffer = new byte[128 * 1024];

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

                        //    synchronized (checksumFiles)
                        //  {
                        //     checksumFiles.offer(new DynamicCommon.FiverFile(new File(baseDir + fileName), offset, fileSize, null, file_id));
                        //}
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


    public DynamicReceiver ()
    {
        /*
        changeThreads("checksum","increment", 1);

        while(!files.isEmpty())
        {
            String increase_decrease="increment";

            int random = (int )(Math.random() * 30 + 1);
            if(random%2==1) increase_decrease="decrement";
            if((increase_decrease.equals("decrement") && random >= checksumThreads.size())) { }
            else if ((increase_decrease.equals("increment") && (random + checksumThreads.size()) > 50)) { }
            else
            {
                changeThreads("checksum", increase_decrease, random);
            }

            DynamicCommon.sleeper(100);
            System.out.println("#checksum="+checksumThreads.size());
        }
        */
    }

    public static void main(String[] args)
    {
        String path="/Users/batyrchary/Desktop/projects/RIVA_Dynamic/r/";
        baseDir=path;
        port=2008;

        for(int i=0; i<50; i++)
        {
            ReceiverRunnable receiver = new ReceiverRunnable(receiver_thread_id);
            receiverThreads.put(receiver_thread_id,receiver);
            receiver_thread_id++;
        }

        DynamicReceiver dr = new DynamicReceiver();
    }
}
