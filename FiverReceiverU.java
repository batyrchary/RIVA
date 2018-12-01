import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class FiverReceiverU extends Thread{

    private ServerSocket ss;
    static AtomicBoolean allTransfersCompleted = new AtomicBoolean(false);
    static String baseDir = "/Users/earslan/receivedFiles/";

    static long totalTransferredBytes = 0L;
    static long totalChecksumBytes = 0L;
    long INTEGRITY_VERIFICATION_BLOCK_SIZE = 256 *1024 * 1024;

    boolean debug = false;
    long startTime;

    public static boolean everythingEnded=false;

    static LinkedBlockingQueue<Item> items = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<FiverFile> checksumFiles;

    class Item
    {
        String filename;
        int length;

        public Item(String filename, int length)
        {
            this.filename= filename;
            this.length = length;
        }
    }

    public class FiverFile {
        public FiverFile(File file, long offset, long length) {
            this.file = file;
            this.offset = offset;
            this.length = length;
        }
        File file;
        Long offset;
        Long length;
    }


    public FiverReceiverU(int port) {
        try {
            ss = new ServerSocket(port);
            checksumFiles = new LinkedBlockingQueue<>();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        while (true) {
            try {
                Socket clientSock = ss.accept();
                clientSock.setSoTimeout(60000);
                System.out.println("Connection established from  " + clientSock.getInetAddress());
                saveFile(clientSock);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void saveFile(Socket clientSock) throws IOException, InterruptedException {


        startTime = System.currentTimeMillis();
        DataInputStream dataInputStream  = new DataInputStream(clientSock.getInputStream());
        INTEGRITY_VERIFICATION_BLOCK_SIZE = dataInputStream.readLong();
        //System.out.println("Will receive " + numOfFiles +" file(s)");

        allTransfersCompleted.set(false);
        totalTransferredBytes = 0L;
        totalChecksumBytes = 0L;

        ChecksumRunnable checksumRunnable  = new ChecksumRunnable();
        Thread checksumThread = new Thread(checksumRunnable, "checksumThread");
        checksumThread.start();


        byte[] buffer = new byte[128 * 1024];
        while(true)
        {
            String fileName = dataInputStream.readUTF();
            if(fileName.equals("done")){
                break;
            }
            long offset = dataInputStream.readLong();
            long fileSize = dataInputStream.readLong();

            System.out.println("File " + fileName + "\t" + humanReadableByteCount(fileSize, false) + " bytes" + " time:" + (System.currentTimeMillis() - startTime)/1000.0 + " s");


            checksumFiles.offer(new FiverFile(new File(baseDir + fileName), offset, fileSize));
            RandomAccessFile randomAccessFile = new RandomAccessFile(baseDir + fileName, "rw");

            if (offset > 0)
            {
                randomAccessFile.getChannel().position(offset);
            }
            long remaining = fileSize;
            int read = 0;
            long transferStartTime = System.currentTimeMillis();

            while (remaining > 0)
            {
                int howmuchtoRead=(int) Math.min(buffer.length, remaining);

                read=0;
                while(read!=howmuchtoRead)
                {
                    int r= dataInputStream.read(buffer, read,howmuchtoRead-read);
                    read=read+r;
                }

                if (read == -1)
                    break;

                totalTransferredBytes += read;
                remaining -= read;
                randomAccessFile.write(buffer, 0, read);

                items.offer(new Item(fileName, read));
            }
            randomAccessFile.close();
            if (read == -1)
            {
                System.out.println("Read -1, closing the connection...");
                return;
            }
            System.out.println("lastitems size="+items.size());


            System.out.println("Transfer End " + fileName + " size:" + fileSize + " duration:" + (System.currentTimeMillis() - transferStartTime)/1000.0 + " time:" + (System.currentTimeMillis() - startTime)/1000.0);

        }
        everythingEnded=true;
       // checksumThread.interrupt();
        System.out.println("receiver done");
        //dataInputStream.close();
        //clientSock.close();
        //allTransfersCompleted.set(true);
        //System.out.println("FEVER Total Time " + (System.currentTimeMillis() - startTime)/1000.0 + " s");

    }


    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }


    public static void main (String[] args) {
        if (args.length > 0) {
            baseDir = args[0];
        }
        FiverReceiverU fs = new FiverReceiverU(2008);
        fs.start();
    }



    public class ChecksumRunnable implements Runnable {
        MessageDigest md = null;

        long totalChecksumTime = 0;

        @Override
        public void run() {
            System.out.println("MyThread - START "+Thread.currentThread().getName());
            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            md.reset();
            DataOutputStream dataOutputStream = null;
            try {
                ServerSocket socket  = new ServerSocket(20180);
                dataOutputStream  = new DataOutputStream(socket.accept().getOutputStream());
                System.out.println("Checksum Connection accepted");
            } catch (IOException e) {
                e.printStackTrace();
            }

            int vmTouchCounter=0;
            int injectioncounter=0;
            while (true)
            {
                if(everythingEnded)
                    break;
                FiverFile fiverFile = null;
                try {
                    fiverFile = checksumFiles.poll();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (fiverFile == null) {
                    continue;
                }


                String fileName = fiverFile.file.getName();
                long fileOffset = fiverFile.offset;
                long fileLength = fiverFile.length;


                long init = System.currentTimeMillis();
                Long remaining = fileLength;
                Long processedBytes = 0L;
                Item item;

                try
                {
                    String fileIsmi=fileName;
                    Long offset=0L;

                    RandomAccessFile randomAccessFile = new RandomAccessFile(baseDir + fileName, "r");

                    while (remaining > 0)
                    {
                        randomAccessFile.getChannel().position(offset);

                        Long currentBlockSize = Math.min(INTEGRITY_VERIFICATION_BLOCK_SIZE, remaining);

                        int usedBytes;
                        while (processedBytes < currentBlockSize)
                        {

                            while(items.size()==0)
                                Thread.sleep(10);
                            item = items.poll();


                            if (item == null)
                            {
                                continue;
                            }
                            fileIsmi = item.filename;
                            usedBytes = item.length;

                            remaining -= usedBytes;
                            totalChecksumBytes += usedBytes;
                            processedBytes += usedBytes;
                        }

                        long leftOver=processedBytes-currentBlockSize;
                        processedBytes=currentBlockSize;

                        long start =  (offset / 1048576);
                        long end = ( (offset + processedBytes) / 1048576);


                        if(offset+processedBytes >= fileLength)
                        {
                            System.out.println("end");
                            start=0;
                        }
                        String range = start + "m-" + end + "m";

                        System.out.println("incache loopa giryom remaining="+remaining+"\trange="+start+", "+end+"\tprocessed="+processedBytes);


                        evictFromCache(fileIsmi, range);


//                        Thread.sleep(100);
                        vmTouchCounter++;

                        while (InCache(fileIsmi, range))
                        {
                            vmTouchCounter++;


                            evictFromCache(fileIsmi, range);
                            //    range = start + "m-" + (end + 5) + "m";//just in case evict extra 5MB

  //                          Thread.sleep(100);
                            //    range = start + "m-" + (end - 5) + "m";
                        }
                        System.out.println("Vmcounter="+vmTouchCounter);

                        System.out.println("processedBytes="+processedBytes+"\toffset="+offset);
                        offset = offset + processedBytes;


                        System.out.println("in processedBytes loopa giryom remaining="+remaining+"\trange="+start+", "+end);
                        while (processedBytes > 0)
                        {
                            byte[] buffer = new byte[128 * 1024];

                            int howMuchToRead = buffer.length;
                            if (processedBytes - buffer.length < 0)
                            {
                                howMuchToRead = (int) (long) (processedBytes);
                            }
                            randomAccessFile.read(buffer, 0, howMuchToRead);
                            md.update(buffer, 0, howMuchToRead);
                            processedBytes=processedBytes-howMuchToRead;

                        }

                        byte[] digest = md.digest();
                        String hex = (new HexBinaryAdapter()).marshal(digest);

/*                        if(remaining==0 && injectioncounter<=6)//injection
                        {
                            injectioncounter=injectioncounter+1;
                            hex=hex.toLowerCase();
                        }
*/
//                      System.out.println("processedBytes="+processedBytes+"\toffset="+offset);
                        System.out.println("Sending hex:" + hex);
                        dataOutputStream.writeUTF(hex);
                        md.reset();

                        processedBytes = 0l;
                        processedBytes = leftOver;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                System.out.println("File:" + fileName + " finished checksum ");


                totalChecksumTime += (System.currentTimeMillis() - init);
            }

            System.out.println("counter="+vmTouchCounter);


            System.out.println("Recevier checksum ended");
        }
    }

    public static void evictFromCache(String filename, String range)
    {
        try {

            String cmd2[] = {"/bin/bash", "-c", "vmtouch -e "+baseDir + filename};//-p "+range+" "+baseDir + filename};
            Process proc2 = Runtime.getRuntime().exec(cmd2);
            BufferedReader input2 = new BufferedReader(new InputStreamReader(proc2.getInputStream()));
            String line;
            while ((line = input2.readLine()) != null) {

            }

            input2.close();
        }
        catch (Exception e){
            System.out.println("Exception in cache evict");
        }
    }

    public static boolean InCache(String filename, String range)
    {
        String percentage = "";
        try {

            String cmd2[] = {"/bin/bash", "-c", "vmtouch -p "+range+" "+baseDir+ filename};
            Process proc2 = Runtime.getRuntime().exec(cmd2);

            BufferedReader input2 = new BufferedReader(new InputStreamReader(proc2.getInputStream()));

            String line;
            while ((line = input2.readLine()) != null) {
                //    System.out.println("cacheline="+line);
                if (line.contains("Resident")) {
                    //       System.out.println("cacheline="+line);
                    line = line.replaceAll("\\s+", ",");
                    //     System.out.println("cacheline="+line);
                    String splitted[] = line.split(",");

                    percentage = splitted[splitted.length-1];
                    //   System.out.println("percentage="+percentage);
                    break;
                }
            }
            input2.close();


        }
        catch (Exception e){
            System.out.println("Exception in cache check");
        }
        float p;
        try {
            p = Float.parseFloat(percentage.split("%")[0]);
        }catch (Exception e)
        {
            System.out.println("Exception inCache fun for input: range="+range+"\tfilename="+filename+"\tp="+percentage.split("%")[0]);
            p=3;
        }
      //  System.out.println(filename+" percentage="+p);
        if(p<2)
            return false;
        else
            return true;
    }
}