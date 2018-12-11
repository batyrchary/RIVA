import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Semaphore;

public class FileLevelPipeliningReceiver{

    private ServerSocket ss;
    boolean allTransfersCompleted = false;
    static String baseDir = "/data/";

    static long startTime;

    static long totalTransferredBytes = 0L;
    static long totalChecksumBytes = 0L;

    Semaphore checksumLock = new Semaphore(2);
    Semaphore transferLock = new Semaphore(0);

    boolean debug = false;

    public FileLevelPipeliningReceiver(int port) {
        try {
            ss = new ServerSocket( port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        while (true) {
            try {
                Socket clientSock = ss.accept();
            //    clientSock.setSoTimeout(10000);
                System.out.println("Connection established from  " + clientSock.getInetAddress());
                saveFile(clientSock);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void saveFile(Socket clientSock) throws IOException {
        startTime = System.currentTimeMillis();
        DataInputStream dataInputStream  = new DataInputStream(clientSock.getInputStream());
        int numOfFiles = dataInputStream.readInt();
        System.out.println("Will receive " + numOfFiles +" file(s)");

        allTransfersCompleted = false;
        ChecksumRunnable runnable  = new ChecksumRunnable(numOfFiles);
        Thread checksumThread = new Thread(runnable, "checksumThread");
        checksumThread.start();
        MonitorThread monitorThread = new MonitorThread();
        monitorThread.start();
        byte[] buffer = new byte[128 * 1024];
        for (int i = 0; i < numOfFiles; i++) {
            try {
                checksumLock.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String fileName = dataInputStream.readUTF();
            long fileSize = dataInputStream.readLong();
            if (debug) {
                System.out.println("Starting to transfer file " + i + "\t" + fileName + "\t" +
                        humanReadableByteCount(fileSize, false) + " bytes");
            }
            monitorThread.setFileSize(fileSize);

            OutputStream fos = new FileOutputStream(baseDir+fileName);
            long remaining = fileSize;
            int read = 0;
            while (remaining > 0) {
                read = dataInputStream.read(buffer, 0, (int) Math.min((long) buffer.length, remaining));
                remaining -= read;
                totalTransferredBytes += read;
                fos.write(buffer, 0, read);
            }
            if (read == -1) {
                System.out.println("Read -1, closing the connection...");
                return;
            }
            if (debug) {
                System.out.println("Finished to transferring file " + i + "\t" + fileName);
            }
            fos.close();
            runnable.setFileMetadata(baseDir+fileName, fileSize);
            transferLock.release();
        }
        dataInputStream.close();
        allTransfersCompleted = true;
        System.out.println("File level Total Time " + (System.currentTimeMillis() - startTime)/1000.0 + " s");


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
        FileLevelPipeliningReceiver fileLevelPipeliningReceiver = new FileLevelPipeliningReceiver(2028);
        fileLevelPipeliningReceiver.run();
    }

    public class ChecksumRunnable implements Runnable {
        MessageDigest md = null;
        String fileName;
        long fileSize;
        int totalFileCount;

        public ChecksumRunnable (int totalFileCount) {
            this.totalFileCount = totalFileCount;
        }

        public void setFileMetadata (String fileName, long fileSize) {
            this.fileName = fileName;
            this.fileSize = fileSize;
        }

        @Override
        public void run() {
            System.out.println("MyThread - START "+Thread.currentThread().getName());
            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            byte[] buffer = new byte[128 * 1024];
            InputStream is = null;
            int checksumCompletedFileCount  = 0;
            while (checksumCompletedFileCount <  totalFileCount) {
                String currentFileName = null;
                long currentFileSize = 0;
                try {
                    transferLock.acquire();
                    currentFileName = fileName;
                    currentFileSize = fileSize;
                    File file = new File(currentFileName);
                    is = new FileInputStream(file);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (debug) {
                    System.out.println("Starting to checksum:" + currentFileName + " size:" + currentFileSize);
                }
                long startTime = System.currentTimeMillis();
                DigestInputStream dis = new DigestInputStream(is, md);
                long read;
                try {
                    while ((read = dis.read(buffer)) > 0) {
                        totalChecksumBytes += read;
                    }
                    dis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                byte[] digest = md.digest();
                String hex = (new HexBinaryAdapter()).marshal(digest);
                if (debug) {
                    System.out.println("Finished checksum :" + currentFileName + "\t Checksum  " + hex + " time " +
                            (System.currentTimeMillis() - startTime) / 1000.0 + " s");
                }

                checksumCompletedFileCount++;
                md.reset();
                checksumLock.release();
            }
            System.out.println("MyThread - END " + Thread.currentThread().getName() + " time:" +
                    (System.currentTimeMillis()-startTime)/1000.0);
        }

    }


    public class MonitorThread extends Thread {
        long lastReceivedBytes = 0;
        long lastReadBytes = 0;
        long fileSize;
        public void setFileSize(long fileSize) {
            this.fileSize = fileSize;
        }
        @Override
        public void run() {
            try {
                while (!allTransfersCompleted) {
                    double thrInMbps = 8 * (totalTransferredBytes - lastReceivedBytes) / (1000*1000);
                    double readThrInMbps = 8 * (totalChecksumBytes - lastReadBytes) / (1000*1000);
                    System.out.println("Transfer:" + thrInMbps + " Mb/s Checksum:" + readThrInMbps + "Mb/s");
                    lastReceivedBytes = totalTransferredBytes;
                    lastReadBytes = totalChecksumBytes;
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

}
