import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.io.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.xml.bind.annotation.adapters.HexBinaryAdapter;


public class FileSenderHybrid {
    private Socket s;

    static boolean allFileTransfersCompleted = false;

    static Semaphore checksumLock = new Semaphore(0);
    static Semaphore transferLock = new Semaphore(0);

    static long totalTransferredBytes = 0;
    static long totalChecksumBytes = 0;

    long cutoffFileLength;

    static String fileOrdering = "shuffle";

    static LinkedBlockingQueue<Item> items = new LinkedBlockingQueue<>(10000);
    static LinkedBlockingQueue<Item> items2 = new LinkedBlockingQueue<>(10000);

    public FileSenderHybrid(String host, int port, long cutoffFileLength) {
        try {
            s = new Socket(host, port);
            s.setSoTimeout(1000000);
        } catch (Exception e) {
            System.out.println(e);
        }
        this.cutoffFileLength = cutoffFileLength;
    }

    public static void main(String[] args) {
        String destIp = args[0];
        String path = args[1];
        long cutoffFileLength = Long.parseLong(args[2]);
        FileSenderHybrid fc = new FileSenderHybrid(destIp, 2018, cutoffFileLength);
        try {
            fc.sendFile(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void sendFile(String path) throws IOException {

        new MonitorThread().start();


        long startTime = System.currentTimeMillis();
        DataOutputStream dos = new DataOutputStream(s.getOutputStream());

        File file =new File(path);
        File[] files;
        if(file.isDirectory()) {
            files = file.listFiles();
        } else {
            files = new File[] {file};
        }
        System.out.println("Will transfer " + files.length+ " files");

        if (fileOrdering.compareTo("shuffle") == 0){
            List<File> fileList = Arrays.asList(files);
            Collections.shuffle(fileList);
            files = fileList.toArray(new File[fileList.size()]);
        } else if (fileOrdering.compareTo("sort") == 0) {
            Arrays.sort(files, new Comparator<File>() {
                public int compare(File f1, File f2) {
                    try {
                        int i1 = Integer.parseInt(f1.getName());
                        int i2 = Integer.parseInt(f2.getName());
                        return i1 - i2;
                    } catch(NumberFormatException e) {
                        throw new AssertionError(e);
                    }
                }
            });
        } else {
            System.out.println("Undefined file ordering:" + fileOrdering);
            System.exit(-1);
        }


        dos.writeLong(cutoffFileLength);
        dos.writeInt(files.length);
        int fastChecksumFileCount = 0;
        for (File f : files){
            if (f.length() < cutoffFileLength) {
                fastChecksumFileCount++;
            }
        }
        ChecksumThread worker = new ChecksumThread(fastChecksumFileCount);
        Thread thread = new Thread(worker);
        thread.start();

        byte[] buffer = new byte[128 * 1024];
        int n;
        for (int i = 0; i <files.length ; i++) {
            //send file metadata
            File currentFile = files[i];
            dos.writeUTF(currentFile.getName());
            dos.writeLong(currentFile.length());
            boolean isFastChecksum = currentFile.length() < cutoffFileLength ? true : false;

            worker.setFileMetadata(currentFile.getName(), currentFile.length());
            System.out.println("File "  + i + "\t" + currentFile.getName()+ " size:" +
                    humanReadableByteCount(currentFile.length(), false) + " fastChecksum:" + isFastChecksum);

            FileInputStream fis = new FileInputStream(files[i]);
            totalTransferredBytes = 0;
            try {
                while ((n = fis.read(buffer)) > 0) {
                    dos.write(buffer, 0, n);
                    if (isFastChecksum && totalTransferredBytes == 0)
                        transferLock.release();
                    if (isFastChecksum) {
                        items.offer(new Item(buffer, n), Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                    }
                    totalTransferredBytes += n;

                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Finished transferring file " + currentFile.getName() + "\t" +
                    (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
            if (isFastChecksum) {
                try {
                    checksumLock.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("Starting to post transfer checksum");
                MessageDigest md = null;
                try {
                    md = MessageDigest.getInstance("MD5");
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                fis = new FileInputStream(files[i]);
                DigestInputStream dis = new DigestInputStream(fis, md);
                while ((n = dis.read(buffer)) > 0) {
                    totalChecksumBytes +=n;
                }
                byte[] digest = md.digest();
                String hex = (new HexBinaryAdapter()).marshal(digest);
                System.out.println("Checksum  " + hex + " time " + (System.currentTimeMillis() - startTime) + " ms");
                dis.close();
            }
            fis.close();
        }
        dos.close();
        allFileTransfersCompleted = true;
    }


    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }




    class Item {
        byte[] buffer;
        int length;

        public Item(byte[] buffer, int length){
            this.buffer = buffer;
            this.length = length;
        }
    }


    public class ChecksumThread implements Runnable {
        String fileName;
        long fileSize;
        int totalFileCount;
        int finishedFileCount = 0;

        public ChecksumThread (int totalFileCount) {
            this.totalFileCount = totalFileCount;
        }

        MessageDigest md = null;
        public void setFileMetadata (String fileName, long fileSize) {
            this.fileName = fileName;
            this.fileSize = fileSize;
        }

        public void reset () {
            md.reset();
        }

        @Override
        public void run() {
            System.out.println("MyThread - START " + Thread.currentThread().getName());
            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            long startTime = System.currentTimeMillis();
            while (finishedFileCount < totalFileCount) {
                try {
                    transferLock.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                long fileStartTime = System.currentTimeMillis();
                totalChecksumBytes = 0;
                while (totalChecksumBytes < fileSize || items.size() > 0) {
                    Item item = null;
                    try {
                        item = items.poll(100, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (item == null) {
                        continue;
                    }
                    md.update(item.buffer, 0, item.length);
                    totalChecksumBytes += item.length;
                }
                byte[] digest = md.digest();
                String hex = (new HexBinaryAdapter()).marshal(digest);
                double duration = (System.currentTimeMillis() - fileStartTime) / 1000.0;
                System.out.println("Checksum  " + hex + " file:" + fileName + "  duration:" + duration + " items size:"+items.size() );
                //checksumCompleted.set(true);
                finishedFileCount++;
                checksumLock.release();

            }
            System.out.println("MyThread - END " + Thread.currentThread().getName());
            double totalDuration = (System.currentTimeMillis() - startTime) / 1000.0;
            System.out.println("Total duration: " + totalDuration);

        }

    }


    public class MonitorThread extends Thread {
        long lastTransferredBytes = 0;
        long lastChecksumBytes = 0;

        @Override
        public void run() {
            try {
                while (!allFileTransfersCompleted || !items.isEmpty()) {
                    double transferThrInMbps = 8 * (totalTransferredBytes-lastTransferredBytes)/(1000*1000);
                    double checksumThrInMbps = 8 * (totalChecksumBytes-lastChecksumBytes)/(1024*1024);
                    System.out.println("Network thr:" + transferThrInMbps + "Mb/s I/O thr:" + checksumThrInMbps + " Mb/s queue size:" + items.size());
                    lastTransferredBytes = totalTransferredBytes;
                    lastChecksumBytes = totalChecksumBytes;
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}
