import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import javax.xml.bind.annotation.adapters.HexBinaryAdapter;


public class FiverSenderU
{
    private Socket s;

    static boolean allFileTransfersCompleted = false;
    static String destIp;

    public static String baseDir="";
    List<FiverFile> files;
    static LinkedBlockingQueue<FiverFile> checksumFiles;

    static long totalTransferredBytes = 0;
    static long totalChecksumBytes = 0;
    long INTEGRITY_VERIFICATION_BLOCK_SIZE = 256 * 1024 * 1024;
    long startTime;
    boolean debug = true;

    public static DataOutputStream dos;

    static String fileOrdering = "shuffle";
    public static boolean everythingEnded=false;

    public FiverSenderU(String host, int port)
    {
        try {
            s = new Socket(host, port);
            s.setSoTimeout(10000);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public class FiverFile
    {
        public FiverFile(File file, long offset, long length) {
            this.file = file;
            this.offset = offset;
            this.length = length;
        }
        File file;
        Long offset;
        Long length;
    }


    public void sendFile(String path) throws IOException
    {

        startTime = System.currentTimeMillis();
        //DataOutputStream dos = new DataOutputStream(s.getOutputStream());
        dos = new DataOutputStream(s.getOutputStream());

        File file =new File(path);
        files = new LinkedList<>();
        checksumFiles = new LinkedBlockingQueue<>();
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
        System.out.println("Will transfer " + files.size()+ " files");

        if (fileOrdering.compareTo("shuffle") == 0)
        {
            Collections.shuffle(files);
        }
        else if (fileOrdering.compareTo("sort") == 0)
        {
            Collections.sort(files, new Comparator<FiverFile>()
            {
                public int compare(FiverFile f1, FiverFile f2) {
                    try {
                        int i1 = Integer.parseInt(f1.file.getName());
                        int i2 = Integer.parseInt(f2.file.getName());
                        return i1 - i2;
                    } catch(NumberFormatException e) {
                        throw new AssertionError(e);
                    }
                }
            });
        }
        else
        {
            System.out.println("Undefined file ordering:" + fileOrdering);
            System.exit(-1);
        }

        ChecksumRunnable checksumRunnable  = new ChecksumRunnable();
        Thread checksumThread = new Thread(checksumRunnable, "checksumThread");
        checksumThread.start();

        dos.writeLong(INTEGRITY_VERIFICATION_BLOCK_SIZE);

        byte[] buffer = new byte[128 * 1024];
        int n;
        while (true)
        {
            if(everythingEnded)
                break;
            FiverFile currentFile = null;
            synchronized (files)
            {
                if (!files.isEmpty())
                {
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
            String currentFileName=currentFile.file.getName();
            dos.writeUTF(currentFileName);
            dos.writeLong(currentFile.offset);
            dos.writeLong(currentFile.length);


            System.out.println("Transfer START file " + currentFile.file.getName() + "offset" + currentFile.offset + " size:" + humanReadableByteCount(currentFile.file.length(), false) + " time:" + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");


            long fileTransferStartTime = System.currentTimeMillis();

            FileInputStream fis = new FileInputStream(currentFile.file);
            if (currentFile.offset > 0)
            {
                fis.getChannel().position(currentFile.offset);
            }
            Long remaining = currentFile.length;
            long readTotal=0;
            long counter=0;
            try
            {
                while (remaining > 0)
                {

                    int howmuchtoRead=(int) Math.min(buffer.length, remaining);

                    n=0;
                    while(n!=howmuchtoRead)
                    {
                        int r= fis.read(buffer, n,howmuchtoRead-n);
                        n=n+r;
                    }
                    //n = fis.read(buffer, 0, (int) Math.min((long)buffer.length, remaining));

                    remaining -= n;
                    totalTransferredBytes += n;
                    readTotal=readTotal+n;
                    //items.offer(new Item(currentFileName, n));
                    dos.write(buffer, 0, n);

                    /*counter++;
                    if(counter%10000==0)
                        System.out.println("items size="+items.size());*/
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }

          //  System.out.println("readTotal="+readTotal);
          //  System.out.println("counter="+counter);
            System.out.println("Transfer END file " + currentFile.file.getName() + "\t duration:" + (System.currentTimeMillis() - fileTransferStartTime) / 1000.0 + " time:" + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

            System.out.println("Total transferred bytes:" + totalTransferredBytes);
            fis.close();

        }


        System.out.println("sender ended");
    }



    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public static void main(String[] args) {
        destIp = args[0];
        String path = args[1];
        baseDir=path;
        if (args.length > 2) {
            fileOrdering = args[2];
        }
        FiverSenderU fc = new FiverSenderU(destIp, 2008);
        try {
            fc.sendFile(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    class Item {
        String filename;
        int length;

        public Item(String filename, int length){
            this.filename =filename;
            this.length = length;
        }
    }


    public class ChecksumRunnable implements Runnable
    {
        MessageDigest md = null;


        public void reset () {
            md.reset();
        }

        @Override
        public void run()
        {
            System.out.println("MyThread - START " + Thread.currentThread().getName());
            DataInputStream dataInputStream = null;
            while (true) {
                try {
                    s = new Socket(destIp, 20180);
                    dataInputStream = new DataInputStream(s.getInputStream());
                    break;
                } catch (IOException e) {
                    System.out.println("Trying to connect to checksum thread");
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
            }

            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            int totalIntegrityFailures = 0;

            try {
                while (true)
                {
                    FiverFile currentFile = checksumFiles.poll(100, TimeUnit.MILLISECONDS);
                    if(currentFile == null)
                        break;
                    long fileSize = currentFile.length;
                    long remaining = fileSize;
                    String filename=currentFile.file.getPath();
                    long offset;
                    System.out.println(filename);
                    FileInputStream fis = new FileInputStream(filename);
                    byte[] buffer = new byte[128 * 1024];
                    while (remaining > 0) {
                        long currentBlockSize = Math.min(INTEGRITY_VERIFICATION_BLOCK_SIZE, remaining );
                        long blockRemaining=currentBlockSize;
                        offset = fileSize - remaining;

                        while (blockRemaining > 0) {
                            int howMuchToRead = buffer.length;
                            if (blockRemaining - buffer.length < 0) {
                                howMuchToRead = (int)blockRemaining;
                            }
                            fis.read(buffer, 0, howMuchToRead);
                            md.update(buffer, 0, howMuchToRead);
                            blockRemaining -= howMuchToRead;
                            remaining -= howMuchToRead;

                        }
                        byte[] digest = md.digest();
                        String hex = (new HexBinaryAdapter()).marshal(digest);
                        String destinationHex = dataInputStream.readUTF();

                        if(hex.equals(destinationHex))
                        {
                            System.out.println("equal time:" + (System.currentTimeMillis()-startTime)/1000.0);
                        }
                        else
                        {

                            FiverFile file=new FiverFile(currentFile.file,offset,currentBlockSize);
                            files.add(file);
                            System.out.println("notequal");
                        }
                        md.reset();
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("MyThread - END " + Thread.currentThread().getName());
            double totalDuration = (System.currentTimeMillis() - startTime) / 1000.0;
            System.out.println("Total duration: " + totalDuration);

            try {
                dos.writeUTF("done");
            } catch (IOException e) {
                e.printStackTrace();
            }
            everythingEnded=true;
            return;
        }
    }
    public static void evictFromCache(String filename, String range)
    {
        try {

            String cmd2[] = {"/bin/bash", "-c", "vmtouch -e -p "+range+" "+baseDir + filename};
            Process proc2 = Runtime.getRuntime().exec(cmd2);
            BufferedReader input2 = new BufferedReader(new InputStreamReader(proc2.getInputStream()));

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
        //System.out.println(filename+" percentage="+p);
        if(p<2)
            return false;
        else
            return true;
    }
}
