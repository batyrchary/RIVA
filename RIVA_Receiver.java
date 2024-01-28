import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


test
public class RIVA_Receiver extends Thread {

    private ServerSocket ss;
    static AtomicBoolean allTransfersCompleted = new AtomicBoolean(false);
    static String baseDir = "./";
    static String baseDirInjected= "./";

    public static int injectedPages=0;

    static long totalTransferredBytes = 0L;
    static long totalChecksumBytes = 0L;
    static long INTEGRITY_VERIFICATION_BLOCK_SIZE =  256 * 1024 * 1024;

    public static volatile  boolean transferdone=false;
    public static volatile boolean allPagesinjected=false;

    boolean debug = false;
    long startTime;

    public static  boolean everythingEnded = false;
    public static String filesystemName;

    static LinkedBlockingQueue<FiverFile> files = new LinkedBlockingQueue<>();
    static LinkedBlockingQueue<FiverFile> injectedfiles = new LinkedBlockingQueue<>();
    static HashMap<String, ArrayList> PageOffsetForEachFile =new HashMap<>();

    public static class FiverFile {
        public FiverFile(File file, long offset, long length) {
            this.file = file;
            this.offset = offset;
            this.length = length;
        }

        File file;
        Long offset;
        Long length;
        boolean isEvicted = false;
    }


    public RIVA_Receiver(int port) {
        try {
            ss = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        while (true) {
            try {
                Socket clientSock = ss.accept();
            //    clientSock.setSoTimeout(60000);
                System.out.println("Connection established from  " + clientSock.getInetAddress());
                saveFile(clientSock);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void saveFile(Socket clientSock) throws IOException, InterruptedException {


        startTime = System.currentTimeMillis();
        DataInputStream dataInputStream = new DataInputStream(clientSock.getInputStream());
        INTEGRITY_VERIFICATION_BLOCK_SIZE = dataInputStream.readLong();

        allTransfersCompleted.set(false);
        totalTransferredBytes = 0L;
        totalChecksumBytes = 0L;

        ChecksumRunnable checksumRunnable = new ChecksumRunnable();
        Thread checksumThread = new Thread(checksumRunnable, "checksumThread");
        checksumThread.start();


        byte[] buffer = new byte[128 * 1024];
        while (true) {
            String fileName = dataInputStream.readUTF();
            if (fileName.equals("done")) {
                break;
            }
            long offset = dataInputStream.readLong();
            long fileSize = dataInputStream.readLong();

            System.out.println("File " + fileName + "\t" + humanReadableByteCount(fileSize, false) + " bytes" + " time:" + (System.currentTimeMillis() - startTime) / 1000.0 + " s");
            RandomAccessFile randomAccessFile = new RandomAccessFile(baseDir + fileName, "rw");

            if (offset > 0) {
                randomAccessFile.getChannel().position(offset);
            }
            long remaining = fileSize;
            int readBytes = 0;
            long currentBlockSize = Math.min(INTEGRITY_VERIFICATION_BLOCK_SIZE, fileSize);

            int currentBlock = 0;
            long bytesReadForBlock = 0L;
            while (remaining > 0) {
                readBytes = dataInputStream.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                randomAccessFile.write(buffer, 0, readBytes);

                if (readBytes == -1)
                    break;
                totalTransferredBytes += readBytes;
                remaining -= readBytes;
                bytesReadForBlock += readBytes;
                if (bytesReadForBlock >= currentBlockSize) {
                    if (debug) {
                        System.out.println("Adding item queue " + fileName + " currentBlock" + currentBlock + " BytesRead:" + bytesReadForBlock
                                +" offset:" + (currentBlock * INTEGRITY_VERIFICATION_BLOCK_SIZE) + " blocksize:" + currentBlockSize);
                    }
                    FiverFile fiverFile = new FiverFile(new File(baseDir + fileName),
                            currentBlock * INTEGRITY_VERIFICATION_BLOCK_SIZE, currentBlockSize);
                    files.offer(fiverFile);
                    CacheEvictRunnable cacheEvictRunnable = new CacheEvictRunnable(fiverFile);
                    new Thread(cacheEvictRunnable).start();
                    bytesReadForBlock = bytesReadForBlock - currentBlockSize;
                    currentBlockSize = Math.min(INTEGRITY_VERIFICATION_BLOCK_SIZE, remaining + bytesReadForBlock);
                    currentBlock++;
                }

            }
            transferdone=true;
            System.out.println("transferdone in writer");
            randomAccessFile.close();
            if (readBytes == -1) {
                System.out.println("Read -1, closing the connection...");
                return;
            }
        }
        everythingEnded = true;
        System.out.println("receiver done");
        dataInputStream.close();
    }


    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public static void getPagesAndInject(String path)
    {
        File file =new File(path);
        int counter=0;

        try
        {
            List<File> listOfFiles =  new LinkedList<>();

            if(file.isDirectory())
            {
                for (File f : file.listFiles())
                {
                    listOfFiles.add(f);
                }
            }
            else {
                listOfFiles.add(file);
            }
            System.out.println("There are " + listOfFiles.size() + " files");
            for (File f : listOfFiles) {
                String fileName = f.getPath();

                System.out.println(f.getPath());

                String cmd2[] = {"/bin/bash", "-c", "filefrag -v " + f.getPath()};
                Process proc2 = null;

                proc2 = Runtime.getRuntime().exec(cmd2);

                proc2.waitFor();

                BufferedReader input2 = new BufferedReader(new InputStreamReader(proc2.getInputStream()));

                String line;
                String phy_offset = "";
                int length;
                boolean start = false;

                while ((line = input2.readLine()) != null) {
                    //System.out.println(line);
                    if (line.contains("ext")) {
                        start = true;
                        continue;
                    }

                    if (start) {
                        line = line.replaceAll("\\s+", "");
                        line = line.replaceAll("\\.\\.", ":");
                        String splitted[] = line.split(":");

                        if (splitted[0].equals(fileName)) {
                            start = false;
                            continue;
                        }

                        phy_offset = splitted[3];
                        length = Integer.parseInt(splitted[5]);
                        System.out.println("phy_offset=" + phy_offset + "\tlength=" + length);


                        //  for(int i=0; i<length; i++)
                        // {
                        String cmd3[] = {"/bin/bash", "-c", "sudo dd if=/dev/zero of=" + filesystemName + " bs=4096 seek=" + phy_offset + " count=" + length +
                        " oflag=direct"};
                        Process proc3 = Runtime.getRuntime().exec(cmd3);
                        proc3.waitFor();
                        counter++;
                        //}
                    }
                }
                input2.close();
                System.out.println("totalInjectedPages=" + counter);
            }
        }catch (Exception e) { e.printStackTrace(); }
    }

    public static void CheckSumForInjected(String path)
    {
        File file =new File(path);
        byte[] buffer = new byte[128 * 1024];
        try
        {
            if(file.isDirectory())
            {
                for (File f : file.listFiles())
                {
                    String fileName = f.getName();
                    long fileSize = f.length();

                    FileInputStream fis = new FileInputStream(f);

                    long remaining = fileSize;
                    int readBytes = 0;
                    long currentBlockSize = Math.min(INTEGRITY_VERIFICATION_BLOCK_SIZE, fileSize);

                    int currentBlock = 0;
                    long bytesReadForBlock = 0L;
                    while (remaining > 0) {
                        readBytes = fis.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                        if (readBytes == -1)
                            break;

                        remaining -= readBytes;
                        bytesReadForBlock += readBytes;
                        if (bytesReadForBlock >= currentBlockSize) {

                            FiverFile fiverFile = new FiverFile(new File(baseDirInjected + fileName),
                                    currentBlock * INTEGRITY_VERIFICATION_BLOCK_SIZE, currentBlockSize);
                            injectedfiles.offer(fiverFile);

                            bytesReadForBlock = bytesReadForBlock - currentBlockSize;
                            currentBlockSize = Math.min(INTEGRITY_VERIFICATION_BLOCK_SIZE, remaining + bytesReadForBlock);
                            currentBlock++;
                        }
                    }
                    fis.close();

                }
            }



            MessageDigest md = null;
            md = MessageDigest.getInstance("MD5");
            md.reset();

            buffer = new byte[128 * 1024];
            while (!injectedfiles.isEmpty())
            {

                    FiverFile fiverFile = injectedfiles.poll(100, TimeUnit.MILLISECONDS);

                    FileInputStream fis = new FileInputStream(fiverFile.file);
                    if (fiverFile.offset > 0) {
                        fis.getChannel().position(fiverFile.offset);
                    }
                    DigestInputStream dis = new DigestInputStream(fis, md);


                    long remaining = fiverFile.length;
                    int read;

                    while (remaining > 0)
                    {
                        read = dis.read(buffer, 0, (int)Math.min(buffer.length, remaining));
                        if (read == -1) {
                            Thread.sleep(100);
                        }
                        else {
                            totalChecksumBytes += read;
                            remaining -= read;
                        }
                    }
                    dis.close();
                    fis.close();
                    byte[] digest = md.digest();
                    String hex = (new HexBinaryAdapter()).marshal(digest);

                System.out.println(hex);
                    md.reset();
                }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length > 0) {
            baseDir = args[0];
            baseDirInjected = args[0];
        }



        //////////////////////////////////////////////////////////////////////////////////
        //IMPORTANT -- DONT UNCOMMENT THIS PART UNLESS YOU SURE WHAT YOU DOING//
        //IT WILL MAKE FAULT INJECTION TO TRANSFERRED FILES AND IF YOU DID NOT//
        //PROVIDE CORRECT FILESYSTEM NAME AND BASE DIRECTORY IT CAN MESS YOUR SYSTEM//
        filesystemName="/dev/sda2";
        //getPagesAndInject(baseDir );
        //CheckSumForInjected(baseDir);
        //////////////////////////////////////////////////////////////////////////////////




        RIVA_Receiver fs = new RIVA_Receiver(2010);
        fs.start();
    }


    public class ChecksumRunnable implements Runnable {
        MessageDigest md = null;

        long totalChecksumTime = 0;

        @Override
        public void run() {
            System.out.println("MyThread - START " + Thread.currentThread().getName());
            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            md.reset();
            DataOutputStream dataOutputStream = null;
            try {
                ServerSocket socket = new ServerSocket(20180);
                dataOutputStream = new DataOutputStream(socket.accept().getOutputStream());
                System.out.println("Checksum Connection accepted");
            } catch (IOException e) {
                e.printStackTrace();
            }

            byte[] buffer = new byte[128 * 1024];
            while (true) {
                if (everythingEnded)
                    break;
                try {
                    FiverFile fiverFile = files.poll(100, TimeUnit.MILLISECONDS);
                    if (fiverFile == null) {
                        continue;
                    }
                    while (true) {
                        synchronized (fiverFile) {
                            if (fiverFile.isEvicted)
                                break;
                        }
                        Thread.sleep(10);
                    }

                    FileInputStream fis = new FileInputStream(fiverFile.file);
                    if (fiverFile.offset > 0) {
                        fis.getChannel().position(fiverFile.offset);
                    }
                    DigestInputStream dis = new DigestInputStream(fis, md);

                    long startingTime = System.currentTimeMillis();
                    long remaining = fiverFile.length;
                    int read;

                    while (remaining > 0) {
                        read = dis.read(buffer, 0, (int)Math.min(buffer.length, remaining));
                        if (read == -1) {
                            System.out.println("Read -1, size:" + humanReadableByteCount(fiverFile.length, false) +
                                    " remaining:" + remaining +
                                    " time:" + (System.currentTimeMillis() - startTime) / 1000.0 + " sec");
                            Thread.sleep(100);
                        }
                        else {
                            totalChecksumBytes += read;
                            remaining -= read;
                        }
                    }
                    dis.close();
                    fis.close();
                    byte[] digest = md.digest();
                    String hex = (new HexBinaryAdapter()).marshal(digest);

                    System.out.println("Sending hex:" + hex + " duration:" + (System.currentTimeMillis() - startingTime)/1000.0 + " time " + (System.currentTimeMillis() - startTime)/1000.0);
                    dataOutputStream.writeUTF(hex);
                    System.out.println(hex);
                    //System.out.println("injectedPages="+injectedPages);
                    injectedPages++;

                    md.reset();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static void injectToFiverFile(FiverFile fiverFile)
    {

            try {
                File f = fiverFile.file;

                String fileName = f.getName();

                // System.out.println(fileName);

                String cmd2[] = {"/bin/bash", "-c", "filefrag -v " + baseDir + fileName};
                Process proc2 = null;

                proc2 = Runtime.getRuntime().exec(cmd2);

                proc2.waitFor();

                BufferedReader input2 = new BufferedReader(new InputStreamReader(proc2.getInputStream()));

                String line;
                String phy_offset = "";
                int length;
                boolean start = false;



                int lengthCumulative = 0;

                while ((line = input2.readLine()) != null) {

                    if (line.contains("ext")) {
                        start = true;
                        continue;
                    }

                    if (start) {
                        line = line.replaceAll("\\s+", "");
                        line = line.replaceAll("\\.\\.", ":");
                        String splitted[] = line.split(":");

                        if (splitted[0].equals(fileName)) {
                            start = false;
                            continue;
                        }

                        phy_offset = splitted[3];
                        length = Integer.parseInt(splitted[5]);

                        lengthCumulative = lengthCumulative + length;

                            if (Integer.parseInt(phy_offset) > 3000) {
                                String cmd3[] = {"/bin/bash", "-c", "sudo dd if=/dev/zero of=" + filesystemName + " bs=4096 seek=" + phy_offset + " count=" + length + " oflag=direct"};

                                Process proc3 = Runtime.getRuntime().exec(cmd3);
                                proc3.waitFor();

                        }
                    }
                }

                input2.close();

            } catch (Exception e) {
                e.printStackTrace();
            }
    }

    public static class CacheEvictRunnable implements Runnable{
        FiverFile fiverFile;
        public CacheEvictRunnable(FiverFile fiverFile) {
            this.fiverFile = fiverFile;
        }



        public void run () {
            try {


                long start = (fiverFile.offset / 1048576);
                long end = (fiverFile.offset + fiverFile.length) / 1048576;



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


                synchronized (fiverFile) {
                    fiverFile.isEvicted = true;
                }
            } catch (Exception e) {
                System.out.println("Exception in cache evict");
            }
        }
    }

    public static boolean InCache(String filename, String range) {
        String percentage = "";
        try {

            String cmd2[] = {"/bin/bash", "-c", "vmtouch -p " + range + " " + filename};
            Process proc2 = Runtime.getRuntime().exec(cmd2);

           // System.out.println(Arrays.toString(cmd2));
            BufferedReader input2 = new BufferedReader(new InputStreamReader(proc2.getInputStream()));

            String line;
            while ((line = input2.readLine()) != null) {
                    //System.out.println(line);
                if (line.contains("Resident")) {
                           //System.out.println(line);
                    line = line.replaceAll("\\s+", ",");
                         //System.out.println(line);
                    String splitted[] = line.split(",");

                    percentage = splitted[splitted.length - 1];
                       //System.out.println("percentage="+percentage);
                    break;
                }
            }
            input2.close();


        } catch (Exception e) {
            System.out.println("Exception in cache check");
        }

        float p;
        try {
            p = Float.parseFloat(percentage.split("%")[0]);
        } catch (Exception e) {
            System.out.println("Exception inCache fun for input: range=" + range + "\tfilename=" + filename + "\tp=" + percentage);
            p = 3;
        }
        if (p < 1)
            return false;
        else
            return true;

    }
}
