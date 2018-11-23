import javax.xml.bind.DatatypeConverter;
import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Receiver3 {

    public static ReceiverRunnable receiver= new ReceiverRunnable();
    public static Thread receiverThread = new Thread(receiver, "receiverThread");

    public static ChecksumRunnable checksummer= new ChecksumRunnable();
    public static Thread checksumThread = new Thread(checksummer, "checksumThread");

    public static ChecksumSenderRunnable checksumsender= new ChecksumSenderRunnable();
    public static Thread checksumSenderThread = new Thread(checksumsender, "checksumSenderThread");
    public static ChecksumEvict checksumevicter = new ChecksumEvict();
    public static Thread checksumevicterThread = new Thread(checksumevicter, "checksumevicterThread");

    private static ServerSocket ss;
    private static Socket s;
    private static int receivePort=1988;
    private static int sendPort=1989;
    private static String pairIP;
    static String baseDir = "./testR/";
    static double memory=0.3e+9;

    public static String CurrentCheckSumFileName="";
    public  static long CurrentCheckSumFileLength=0;
    public  static String CurrentTransferFileName="";
    public static long CurrentTransferFileLength=0;

    public static long totalTransferredBytes = 0;
    public  static long totalCheksumComputedBytes= 0;
    public static long windowChecksumTransfer=0;
    public  static long remainingToTransferCurrentFile=0;

    static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss");
    public static long transferStartTime=0;
    public static long checksumStartTime=0;

    public static ArrayList<String> checksumReady=new ArrayList<>();
    public static ArrayList<String> filesTransferred = new ArrayList<>();
    public static ArrayList<Long> filesTransferredSizes = new ArrayList<>();
    public static ArrayList<String> filesEvicted=new ArrayList<>();
    public static ArrayList<Long> filesEvictedSizes=new ArrayList<>();

    public static boolean allFileTransfersCompleted = false;
    public static int onlyonce = 0;
    public static Map<String, String> ChecksumFileNamePairReceiver = new HashMap<String, String>();

    static int numOfFiles=1;


    public static class ReceiverRunnable implements Runnable
    {

        @Override
        public void run()
        {
            try
            {
                ss = new ServerSocket(receivePort);
                System.out.println(" The Port is " + receivePort + " ,the Receiver waiting the sender .....");

                while (true)
                {
                    Socket clientSock = ss.accept();
                    //  clientSock.setSoTimeout(1000000000);
                    pairIP=clientSock.getInetAddress().toString().replace("/","");
                    checksumSenderThread.interrupt();//PairIp exists
                    saveFile(clientSock);
                    // ss.close();
                }
            } catch (Exception e) { e.printStackTrace(); }
        }

        private void saveFile(Socket clientSock) throws IOException
        {

            DataInputStream dataInputStream = new DataInputStream(clientSock.getInputStream());
            while (numOfFiles != -1)
            {
                transferStartTime = System.currentTimeMillis();


                numOfFiles = dataInputStream.readInt();

                if (numOfFiles==-1) {
                    checksumSenderThread.interrupt();
                    checksumThread.interrupt();
                    break;
                }
                System.out.println(dtf.format(LocalDateTime.now()) + "\tWill receive " + numOfFiles + " file(s)");


                for (int i = 0; i < numOfFiles; i++)
                {
                    byte[] buffer = new byte[128 * 1024];

                    CurrentTransferFileName = dataInputStream.readUTF();
                    CurrentTransferFileLength = dataInputStream.readLong();


                    System.out.println(dtf.format(LocalDateTime.now()) + "\tStarting to transfer file " + i + "\t" + CurrentTransferFileName + "\t" +
                            humanReadableByteCount(CurrentTransferFileLength, false) + " bytes");


                    OutputStream fos = new FileOutputStream(baseDir + CurrentTransferFileName);
                    remainingToTransferCurrentFile = CurrentTransferFileLength;

                    int n = 0;
                    while (remainingToTransferCurrentFile > 0) {

                        n = dataInputStream.read(buffer, 0, (int) Math.min((long) buffer.length, remainingToTransferCurrentFile));
                        remainingToTransferCurrentFile -= n;
                        totalTransferredBytes += n;

                        fos.write(buffer, 0, n);
                        fos.flush();
                    }

                    if (n == -1) {
                        System.out.println("Read -1, closing the connection...");
                        return;
                    }
                    System.out.println(dtf.format(LocalDateTime.now()) + "\tFinished to transferring file " + i + "\t" + CurrentTransferFileName);
                    update(CurrentTransferFileName,"add","filesTransferred",CurrentTransferFileLength);
                    checksumevicterThread.interrupt();

                    //   if(CurrentTransferFileName.equals("5-5test2") || CurrentTransferFileName.equals("6-test10GB"))
                    //   {
                    //      onlyonce=onlyonce+1;

                    //      if(onlyonce<4) {
                    //          System.out.println("onlyonce="+onlyonce);

                    //          injection(false, CurrentTransferFileName, "sda5");
                    //      }
                    //   }

                    fos.close();
                }

                allFileTransfersCompleted = true;

                System.out.println(dtf.format(LocalDateTime.now()) + "\tFile level Total Time " + (System.currentTimeMillis() - transferStartTime) / 1000.0 + " s");
            }
            dataInputStream.close();
        }
    }


    public static class ChecksumEvict implements Runnable {
        public void Sleep() {
            System.out.println(dtf.format(LocalDateTime.now()) + "\tChecksumEvict will sleep till sth is added to filesTransferred, only sender can interrupt me");
            try {
                Thread.sleep(10000); //ms
                //Thread.interrupted(); //clear
            } catch (InterruptedException e) {
                System.out.println(dtf.format(LocalDateTime.now()) + "\tChecksumEvict: wake up sth is added to filesTransferred.");
            }
        }

        @Override
        public void run() {

            while (numOfFiles != -1) {
                {

                    Sleep();
                    if (numOfFiles == -1) {
                        checksumThread.interrupt();
                        break;
                    }

                    while (!filesTransferred.isEmpty()) {

                        String currentevicted = filesTransferred.get(0);
                        long length=filesTransferredSizes.get(0);
                        evictFromCache(currentevicted);
evictFromCache(currentevicted);
evictFromCache(currentevicted);

                        update(currentevicted, "remove", "filesTransferred", 0);
                        update(currentevicted, "add", "filesEvicted", length);

                        checksumThread.interrupt();
                    }
                }
            }
        }
    }

    public static class ChecksumRunnable implements Runnable {
        MessageDigest md = null;

        public void Sleep() {
            // System.out.println(dtf.format(LocalDateTime.now()) + "\tchecksum will sleep till sth is added to filesTransferred");
            try {
                Thread.sleep(3000); //ms
                //Thread.interrupted(); //clear
            } catch (InterruptedException e) {
                //   System.out.println(dtf.format(LocalDateTime.now()) + "\tchecksum: wake up sth is added to filesTransferred.");
            }
        }

        @Override
        public void run() {
            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            byte[] buffer = new byte[128 * 1024]; //byte increase decrease
            InputStream is = null;

            while (numOfFiles != -1) {
                Sleep(); //first file will interrupt it
                if(numOfFiles==-1)
                    break;
                while (!allFileTransfersCompleted) {
                    Sleep(); //second file will interrupt it

                    while (!filesEvicted.isEmpty()) {

                        //while (totalTransferredBytes - totalCheksumComputedBytes < memory && !allFileTransfersCompleted)
                        //  Sleep();

                        try {
                            Thread.sleep(2000); //ms
                            //Thread.interrupted(); //clear
                        } catch (InterruptedException e) {
                            //  System.out.println(dtf.format(LocalDateTime.now()) + "\tchecksum: wake up sth is added to filesTransferred.");
                        }

                        String evicted = "nothing";
                        long evicted_length = 0;

                        for (int i = 0; i < filesEvicted.size(); i++) {
                            if (InCache(filesEvicted.get(i)))
                                continue;
                            evicted = filesEvicted.get(i);
                            evicted_length = filesEvictedSizes.get(i);
                            break;
                        }
                        if (evicted.equals("nothing") && !allFileTransfersCompleted)
                            continue;
                        else if (evicted.equals("nothing") && allFileTransfersCompleted) {
                            evicted = filesEvicted.get(0);
                        }

                        CurrentCheckSumFileName = evicted;
                        CurrentCheckSumFileLength = evicted_length;

                        try {
                            File file = new File(baseDir + CurrentCheckSumFileName);
                            is = new FileInputStream(file);

                            System.out.println(dtf.format(LocalDateTime.now()) + "\tStarting to checksum:" + CurrentCheckSumFileName + " size:" + CurrentCheckSumFileLength);

                            checksumStartTime = System.currentTimeMillis();
                            DigestInputStream dis = new DigestInputStream(is, md);
                            long read;

                            while ((read = dis.read(buffer)) > 0) {
                                totalCheksumComputedBytes += read;
                            }

                            dis.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        byte[] digest = md.digest();
                        String hex = (new HexBinaryAdapter()).marshal(digest);

                        System.out.println(dtf.format(LocalDateTime.now()) + "\tFinished checksum :" + CurrentCheckSumFileName + "\t Checksum  " + hex);

                        ChecksumFileNamePairReceiver.put(CurrentCheckSumFileName, hex);
                        update(CurrentCheckSumFileName, "add", "checksumReady", 0);
                        update(evicted, "remove", "filesEvicted", evicted_length);
                        md.reset();

                    }

                }
                checksumSenderThread.interrupt();
            }
        }
    }

    public static class ChecksumSenderRunnable implements Runnable
    {
        @Override
        public void run()
        {
            while(pairIP==null){Sleep();}
            try
            {
                s = new Socket(pairIP, sendPort);
                System.out.println(pairIP);
                //    s.setSoTimeout(1000000000);
                sendCheckSums();
                s.close();
            } catch (Exception e) { e.printStackTrace(); }
        }

        public void sendCheckSums() throws IOException
        {
            DataOutputStream dos = new DataOutputStream(s.getOutputStream());

            while (numOfFiles != -1)
            {
                while (checksumReady.size() != numOfFiles)
                {
                    Sleep();
                }
                if(numOfFiles==-1)
                    break;


                for (int i = 0; i < checksumReady.size(); ) {
                    String key = checksumReady.get(i);

                    update(key, "remove", "checksumReady", 0);
                    System.out.println("Sendin " + key + " " + ChecksumFileNamePairReceiver.get(key));
                    dos.writeUTF(key);
                    dos.writeUTF(ChecksumFileNamePairReceiver.get(key));
                    dos.flush();
                }
                filesTransferred.clear();
                filesEvictedSizes.clear();
                filesEvicted.clear();
                checksumReady.clear();
                ChecksumFileNamePairReceiver.clear();
                baseDir=baseDir+"1";
                allFileTransfersCompleted = false;
                numOfFiles=1;
                receiverThread.interrupt();
            }
            dos.close();
        }



        public void Sleep()
        {
            System.out.println(dtf.format(LocalDateTime.now())+"\tchecksumsender will sleep till connection is established");
            try
            {
                Thread.sleep(1000000); //ms
                //Thread.interrupted(); //clear
            }
            catch (InterruptedException e)
            {
                System.out.println(dtf.format(LocalDateTime.now())+"\tchecksumSender: wake up connection established.");
            }
        }

    }



    public static synchronized void update(String filename, String method, String listName, long length)
    {
        if(listName.equals("filesTransferred")) {
            if (method.equals("remove")) {
                int index=filesTransferred.indexOf(filename);
                filesTransferred.remove(filename);
                filesTransferredSizes.remove(index);

            } else if (method.equals("add")) {
                filesTransferred.add(filename);
                filesTransferredSizes.add(length);
            }
        }
        if(listName.equals("filesEvicted"))
        {
            if (method.equals("remove")) {
                int index=filesEvicted.indexOf(filename);
                filesEvicted.remove(filename);
                filesEvictedSizes.remove(index);
            } else if (method.equals("add")) {
                filesEvicted.add(filename);
                filesEvictedSizes.add(length);
            }
        }
        if(listName.equals("checksumReady")) {
            if (method.equals("remove")) {
                checksumReady.remove(filename);
            } else if (method.equals("add")) {
                checksumReady.add(filename);
            }
        }
        System.out.println("filesTransferred=");
        for(int i=0; i<filesTransferred.size(); i++)
        {
            System.out.println(filesTransferred.get(i)+"="+filesTransferredSizes.get(i));
        }
        System.out.println("checksumReady=");
        for(int i=0; i<checksumReady.size(); i++)
        {
            System.out.println(checksumReady.get(i));
        }
        System.out.println("filesEvicted=");
        for(int i=0; i<filesEvicted.size(); i++)
        {
            System.out.println(filesEvicted.get(i));
        }

    }

    public static String humanReadableByteCount(long bytes, boolean si)
    {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public static void injection(boolean firstTime, String fileName, String filesystemName)throws IOException
    {
        String line;

        if(firstTime==true)
        {

            String cmd1[]={"/bin/bash","-c","sudo ls"};
            Process proc1 = Runtime.getRuntime().exec(cmd1);

            BufferedReader input1 = new BufferedReader(new InputStreamReader(proc1.getInputStream()));
            while ((line = input1.readLine()) != null) {}
            input1.close();
        }
        else
        {
            String cmd2[]={"/bin/bash","-c","sudo filefrag -v "+baseDir+fileName};
            Process proc2 = Runtime.getRuntime().exec(cmd2);

            BufferedReader input2 = new BufferedReader(new InputStreamReader(proc2.getInputStream()));

            String phy_offset="";
            while ((line = input2.readLine()) != null)
            {
                if(line.contains("ext"))
                {
                    line = input2.readLine();
                    // System.out.println("injectionline="+line);
                    line=line.replaceAll("\\s+","");
                    line=line.replaceAll("\\.\\.",":");
                    String splitted[]=line.split(":");

                    phy_offset=splitted[3];
                    //System.out.println("phy_offset="+phy_offset);

                    break;
                }
            }
            input2.close();

            if(!phy_offset.equals(""))
            {
                String cmd3[]={"/bin/bash","-c","sudo dd if=/dev/urandom of=/dev/"+filesystemName+" bs=4096 seek="+phy_offset+" count=1"};
                Process proc3 = Runtime.getRuntime().exec(cmd3);
            }
        }
    }


    public static void evictFromCache(String filename)
    {
        try {

            String cmd2[] = {"/bin/bash", "-c", "vmtouch -e "+baseDir + filename};
            Process proc2 = Runtime.getRuntime().exec(cmd2);
            BufferedReader input2 = new BufferedReader(new InputStreamReader(proc2.getInputStream()));
            String line;
            while ((line = input2.readLine()) != null) {
                System.out.println("evict=" + line);
            }

            String cmd3[] = {"/bin/bash", "-c", "vmtouch "+baseDir + filename};
            Process proc3 = Runtime.getRuntime().exec(cmd3);
            BufferedReader input3 = new BufferedReader(new InputStreamReader(proc3.getInputStream()));

            while ((line = input3.readLine()) != null) {
                System.out.println("cacheline=" + line);
            }
            input2.close();
            input3.close();

        }
        catch (Exception e){
            System.out.println("Exception in cache evict");
        }
    }

    public static boolean InCache(String filename)
    {
        String percentage = "";
        try {

            String cmd2[] = {"/bin/bash", "-c", "vmtouch "+baseDir+ filename};
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
        float p= Float.parseFloat(percentage.split("%")[0]);
        //System.out.println(filename+" percentage="+p);
        if(p<3)
            return false;
        else
            return true;
    }
    public static void main(String[] args)
    {
        try{
            injection(true, null, null);
        }
        catch(Exception e){}

        memory=1.6e+10;
        receiverThread.start();
        checksumevicterThread.start();
        checksumThread.start();
        checksumSenderThread.start();

        System.out.println("waiting");
        try
        {
            receiverThread.join();
            System.out.println("I joined receiverThread");
            checksumevicterThread.join();
            System.out.println("I joined checksumevicterThread");
            checksumThread.join();
            System.out.println("I joined checksumThread");
            checksumSenderThread.join();
            System.out.println("I joined checksumSenderThread");
        }
        catch (InterruptedException e) { e.printStackTrace(); }

    }
}

