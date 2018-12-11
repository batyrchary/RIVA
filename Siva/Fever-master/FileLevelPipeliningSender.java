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


public class FileLevelPipeliningSender {
    private Socket s;

    static AtomicBoolean fileTransferCompleted = new AtomicBoolean(false);
    static boolean allFileTransfersCompleted = false;

    static Semaphore checksumLock = new Semaphore(2);
    static Semaphore transferLock = new Semaphore(0);

    static long totalTransferredBytes = 0;
    static long totalChecksumBytes = 0;
    long startTime;
    boolean debug = false;

    static String fileOrdering = "shuffle";

    public FileLevelPipeliningSender(String host, int port) {
        try {
            s = new Socket(host, port);
        //    s.setSoTimeout(10000);
        } catch (Exception e) {
            System.out.println(e);
        }
    }


    public void sendFile(String path) throws IOException {

        //new MonitorThread().start();

        startTime = System.currentTimeMillis();
        DataOutputStream dos = new DataOutputStream(s.getOutputStream());

        File file =new File(path);
        File[] files;
        if(file.isDirectory()) {
            files = file.listFiles();
        } else {
            files = new File[] {file};
        }

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


        System.out.println("Will transfer " + files.length+ " files");
        dos.writeInt(files.length);

        ChecksumThread worker = new ChecksumThread(files.length);
        Thread thread = new Thread(worker);
        thread.start();

        byte[] buffer = new byte[128 * 1024];
        int n;
        for (int i = 0; i <files.length ; i++) {
            //send file metadata
            File currentFile = files[i];
            dos.writeUTF(currentFile.getName());
            dos.writeLong(currentFile.length());
            fileTransferCompleted.set(false);
            try {
                checksumLock.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (debug) {
                System.out.println("Transfer START " + i + "\t" + currentFile.getName() + " size:" +
                        humanReadableByteCount(currentFile.length(), false) + " time:" +
                        (System.currentTimeMillis() - startTime) / 1000.0 + "seconds");
            }
            FileInputStream fis = new FileInputStream(files[i]);
            long remaining = currentFile.length();
            while ((n = fis.read(buffer, 0, (int) Math.min((long)buffer.length, remaining))) > 0) {
                dos.write(buffer, 0, n);
                remaining -=n;
                totalTransferredBytes += n;

            }
            if (debug) {
                System.out.println("TRANSFER END file " + currentFile.getName() + "\t duration:" +
                        (System.currentTimeMillis() - startTime) / 1000.0 + " seconds time:" +
                        (System.currentTimeMillis() - startTime) / 1000.0 + "seconds");
            }
            fileTransferCompleted.set(true); // Mark that file transfer completed
            fis.close();
            worker.setFileName(files[i].getAbsolutePath());
            transferLock.release();
        }
        dos.close();
        allFileTransfersCompleted = true;
        double totalDuration = (System.currentTimeMillis() - startTime) / 1000.0;
        System.out.println("Total duration: " + totalDuration);
    }


    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public static void main(String[] args) {
        String destIp = args[0];
        String path = args[1];
        if (args.length > 2) {
            fileOrdering = args[2];
        }
        FileLevelPipeliningSender fileLevelPipeliningSender = new FileLevelPipeliningSender(destIp, 2028);
        try {
            fileLevelPipeliningSender.sendFile(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public class ChecksumThread implements Runnable {
        String fileName;
        int totalFileCount;
        int checksumComputedFiles = 0;
        MessageDigest md = null;

        public ChecksumThread (int totalFileCount) {
            this.totalFileCount = totalFileCount;
        }

        public void setFileName (String fileName) {
            this.fileName = fileName;
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
            while (checksumComputedFiles < totalFileCount) {
                try {
                    transferLock.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (debug) {
                    System.out.println("Checksum START " + fileName + " time:" +
                            (System.currentTimeMillis() - startTime) / 1000.0 + "seconds");
                }
                String currentFileName = fileName;
                long fileStartTime = System.currentTimeMillis();
                byte[] buffer = new byte[128*1024];
                int n;
                try {
                    FileInputStream fis = new FileInputStream(currentFileName);
                    DigestInputStream dis = new DigestInputStream(fis, md);
                    long total = 0;
                    while ((n = dis.read(buffer)) > 0) {
                        totalChecksumBytes +=n;
                        total += n;
                    }
                    byte[] digest = md.digest();
                    String hex = (new HexBinaryAdapter()).marshal(digest);
                    if (debug) {
                        System.out.println("Checksum END File" + currentFileName + " Checksum  " + hex + " duration:" +
                                (System.currentTimeMillis() - fileStartTime) + " ms time:" +
                                (System.currentTimeMillis() - startTime) / 1000.0 + "seconds " + humanReadableByteCount(total, false));
                    }
                    dis.close();
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                checksumComputedFiles ++;
                checksumLock.release();

            }
            System.out.println("MyThread - END " + Thread.currentThread().getName());
            }

        }

    }

/*
    public class MonitorThread extends Thread {
        long lastTransferredBytes = 0;
        long lastChecksumBytes = 0;

        @Override
        public void run() {
            try {
                while (!allFileTransfersCompleted) {
                    double transferThrInMbps = 8 * (totalTransferredBytes-lastTransferredBytes)/(1000*1000);
                    double checksumThrInMbps = 8 * (totalChecksumBytes-lastChecksumBytes)/(1024*1024);
                    System.out.println("Network thr:" + transferThrInMbps + "Mb/s I/O thr:" + checksumThrInMbps + " Mb/s");
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
*/