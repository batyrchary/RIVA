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

public class DynamicSender {


    public static boolean allFileTransfersCompleted = false;

    public static LinkedList<FiverFile> files=new LinkedList<>();
    public static LinkedList<Socket> sockets=new LinkedList<>();
    public static LinkedList<SenderRunnable> senderThreads=new LinkedList<>();



    public static String destIp="-1";
    public static String baseDir="-1";
    public static int port=-1;
    public static long INTEGRITY_VERIFICATION_BLOCK_SIZE = 0;  //256 * 1024 * 1024;


    public static int threadID=0;





    public static class FiverFile {
        public FiverFile(File file, long offset, long length) {
            this.file = file;
            this.offset = offset;
            this.length = length;
        }
        File file;
        Long offset;
        Long length;
    }






    public static void tune()
    {
    }







    public static class SenderRunnable implements Runnable
    {
        int threadID;
        boolean shouldIfinish=false;
        Thread t;

        public SenderRunnable(int threadID)
        {
            this.threadID=threadID;
            t=new Thread(this);
            t.start();
        }

        @Override
        public void run()
        {

            while(!shouldIfinish)
            {
                sleeper(1000);
                System.out.println("thread=" + threadID);
            }

/*
            startTime = System.currentTimeMillis();
            //DataOutputStream dos = new DataOutputStream(s.getOutputStream());
            dos = new DataOutputStream(s.getOutputStream());

            File file = new File(path);
            files = new LinkedList<>();
            checksumFiles = new LinkedBlockingQueue<>();
            if (file.isDirectory()) {
                for (File f : file.listFiles()) {
                    files.add(new FiverFile(f, 0, f.length()));
                }
            } else {
                files.add(new FiverFile(file, 0, file.length()));
            }
            System.out.println("Will transfer " + files.size() + " files");

            if (fileOrdering.compareTo("shuffle") == 0) {
                Collections.shuffle(files);
            } else if (fileOrdering.compareTo("sort") == 0) {
                Collections.sort(files, new Comparator<FiverFile>() {
                    public int compare(FiverFile f1, FiverFile f2) {
                        try {
                            int i1 = Integer.parseInt(f1.file.getName());
                            int i2 = Integer.parseInt(f2.file.getName());
                            return i1 - i2;
                        } catch (NumberFormatException e) {
                            throw new AssertionError(e);
                        }
                    }
                });
            } else {
                System.out.println("Undefined file ordering:" + fileOrdering);
                System.exit(-1);
            }

            ChecksumRunnable checksumRunnable = new ChecksumRunnable();
            Thread checksumThread = new Thread(checksumRunnable, "checksumThread");
            checksumThread.start();

            dos.writeLong(INTEGRITY_VERIFICATION_BLOCK_SIZE);

            byte[] buffer = new byte[128 * 1024];
            int n;
            while (true) {
                if (everythingEnded)
                    break;
                FiverFile currentFile = null;
                synchronized (files) {
                    if (!files.isEmpty()) {
                        currentFile = files.remove(0);
                    }
                }
                if (currentFile == null) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }

                checksumFiles.offer(currentFile);
                //send file metadata
                String currentFileName = currentFile.file.getName();
                dos.writeUTF(currentFileName);
                dos.writeLong(currentFile.offset);
                dos.writeLong(currentFile.length);


                System.out.println("Transfer START file " + currentFile.file.getName() + "offset" + currentFile.offset + " size:" + humanReadableByteCount(currentFile.file.length(), false) + " time:" + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");


                long fileTransferStartTime = System.currentTimeMillis();

                FileInputStream fis = new FileInputStream(currentFile.file);
                if (currentFile.offset > 0) {
                    fis.getChannel().position(currentFile.offset);
                }
                Long remaining = currentFile.length;
                long readTotal = 0;
                long counter = 0;
                try {


                    while (remaining > 0) {

                        int howmuchtoRead = (int) Math.min(buffer.length, remaining);

                        n = 0;
                        while (n != howmuchtoRead) {
                            int r = fis.read(buffer, n, howmuchtoRead - n);
                            n = n + r;
                        }
                        //n = fis.read(buffer, 0, (int) Math.min((long)buffer.length, remaining));

                        remaining -= n;
                        totalTransferredBytes += n;
                        readTotal = readTotal + n;
                        items.offer(new Item(currentFileName, n));
                        dos.write(buffer, 0, n);

                    counter++;
                    if(counter%10000==0)
                        System.out.println("items size="+items.size());
                    }
                }

                System.out.println("last items size=" + items.size());
                //  System.out.println("readTotal="+readTotal);
                //  System.out.println("counter="+counter);
                System.out.println("Transfer END file " + currentFile.file.getName() + "\t duration:" + (System.currentTimeMillis() - fileTransferStartTime) / 1000.0 + " time:" + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

                System.out.println("Total transferred bytes:" + totalTransferredBytes);
                fis.close();

            }
*/        }
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
                        Socket s = new Socket(destIp, port);
                        s.setSoTimeout(10000);
                        sockets.add(s);

                        SenderRunnable sender = new SenderRunnable(threadID++);
                        senderThreads.add(sender);
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
                    senderThreads.get(senderThreads.size() - 1).shouldIfinish = true; //finish last thread
                    senderThreads.remove(senderThreads.size() - 1);            //last one
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
        else if(which.equals("blocksize"))
        {
            if (type.equals("increment"))
            {
                INTEGRITY_VERIFICATION_BLOCK_SIZE=INTEGRITY_VERIFICATION_BLOCK_SIZE+howmuch;
            }
            else if (type.equals("decrement"))
            {
                INTEGRITY_VERIFICATION_BLOCK_SIZE=INTEGRITY_VERIFICATION_BLOCK_SIZE-howmuch;
            }
        }
    }





    public static void initial(int number_of_senders, int number_of_checksummers, int blocksize )
    {
        changeThreads("blocksize","increment", blocksize);
        changeThreads("transfer","increment", number_of_senders);
        changeThreads("checksum","increment", number_of_checksummers);

        sleeper(3000);
    }



    public DynamicSender (String host, int port, String path)
    {
        filereader(path);

        initial(2,1,256);


    /*    sleeper(100);
        while(!allFileTransfersCompleted)
        {
            tune();
            sleeper(1000);
            System.out.println("karoce burdayim");
        }
    */
    }



    public static class server implements Runnable {
        Thread t;

        public server() {
            t = new Thread(this);
            t.start();
        }
        @Override
        public void run() {
            try {

                ServerSocket ss;
                ss = new ServerSocket(2008);

                while(true) {

                   Socket clientSock = ss.accept();
                   System.out.println("Connection established from  " + clientSock.getInetAddress());
                }

            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("cant create server");
            }
            System.out.println("server ends");
        }
    }


    public static void main(String[] args)
    {

        server ss=new server();



        //destIp = args[0];
        //String path = args[1];

        destIp="localhost";
        String path="/Users/batyrchary/Desktop/journalFIVER/sender/";
        baseDir=path;
        port=2008;


        DynamicSender dc = new DynamicSender(destIp, port, path);




        try {
            ss.t.join();
        }catch (Exception e){e.printStackTrace();}
    }







    public static void filereader(String path)
    {
        File file = new File(path);
        files = new LinkedList<>();

        if(file.isDirectory())
        {
            for (File f : file.listFiles())
            {
                files.add(new FiverFile(f, 0, f.length()));
            }
        }
        else
        {
            files.add(new FiverFile(file, 0, file.length()));
        }
    }

    public static void sleeper(int howmuch)
    {
        try {
            Thread.sleep(howmuch);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
