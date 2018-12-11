import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Semaphore;

public class FileReceiverHybrid{

    private ServerSocket ss;
    boolean allTransfersCompleted = false;
    String baseDir = "/storage/data1/earslan/mixed/";

    static long receivedBytes = 0L;
    static long readBytes = 0L;

    Semaphore transferLock = new Semaphore(0);
    Semaphore checksumLock = new Semaphore(0);


    public FileReceiverHybrid(int port) {
        try {
            ss = new ServerSocket( port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main (String[] args) {
        FileReceiverHybrid fs = new FileReceiverHybrid(2018);
        fs.transfer();
    }

    public void transfer() {
        while (true) {
            try {
                Socket clientSock = ss.accept();
                clientSock.setSoTimeout(1000000);
                System.out.println("Connection established from  " + clientSock.getInetAddress());
                saveFile(clientSock);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void saveFile(Socket clientSock) throws IOException {


        long startTime = System.currentTimeMillis();
        DataInputStream dataInputStream  = new DataInputStream(clientSock.getInputStream());
        long cutoffFileLength = dataInputStream.readLong();
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
            receivedBytes =  0;
            String fileName = dataInputStream.readUTF();
            long fileSize = dataInputStream.readLong();
            boolean isFastChecksum = fileSize < cutoffFileLength ? true : false;

            System.out.println("File " + i + "\t" + fileName + "\t" + humanReadableByteCount(fileSize, false) +" bytes fastChecksum:" + isFastChecksum);

            if (isFastChecksum) {
                runnable.setFileMetadata(baseDir+fileName, fileSize);
            }
            monitorThread.setFileSize(fileSize);

            OutputStream fos = new FileOutputStream(baseDir+fileName);
            long remaining = fileSize;
            int read;
            while ((read = dataInputStream.read(buffer, 0, (int) Math.min((long) buffer.length, remaining))) > 0) {
                remaining -= read;
                if (isFastChecksum && receivedBytes == 0)
                    transferLock.release();
                receivedBytes += read;
                fos.write(buffer, 0, read);
                fos.flush();
            }
            fos.close();
            receivedBytes = 0L;
            if (isFastChecksum) {
                try {
                    checksumLock.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                MessageDigest md = null;
                try {
                    md = MessageDigest.getInstance("MD5");
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                FileInputStream fis = new FileInputStream(new File(baseDir+fileName));
                DigestInputStream dis = new DigestInputStream(fis, md);
                while ((read = dis.read(buffer)) > 0) {
                    readBytes +=read;
                }
                byte[] digest = md.digest();
                String hex = (new HexBinaryAdapter()).marshal(digest);
                System.out.println("Checksum  " + hex + " time " + (System.currentTimeMillis() - startTime) + " ms");
                dis.close();
                fis.close();
            }
        }
        dataInputStream.close();
        allTransfersCompleted = true;
        System.out.println("Total Time " + (System.currentTimeMillis() - startTime)/1000.0 + " s");


    }


    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
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
                try {
                    transferLock.acquire();
                    File file = new File(fileName);
                    while (!file.exists()) {
                        Thread.sleep(100);
                    }
                    is = new FileInputStream(file);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                long startTime = System.currentTimeMillis();
                DigestInputStream dis = new DigestInputStream(is, md);
                long read;
                readBytes = 0;
                try {
                    while (readBytes < fileSize) {
                        read = dis.read(buffer);
                        if (read > 0)
                            readBytes += read;
                    }
                    dis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                byte[] digest = md.digest();
                String hex = (new HexBinaryAdapter()).marshal(digest);
                System.out.println("File:" + fileName + "Checksum  " + hex + " time " + (System.currentTimeMillis() - startTime)/1000.0 + " s");

                checksumCompletedFileCount++;
                readBytes = 0;
                md.reset();
                checksumLock.release();
            }
            System.out.println("MyThread - END " + Thread.currentThread().getName());
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
                    double thrInMbps = 8 * (receivedBytes - lastReceivedBytes) / (1000*1000);
                    double readThrInMbps = 8 * (readBytes - lastReadBytes) / (1000*1000);
                    System.out.println("Network:" + thrInMbps + " Mb/s I/O:" + readThrInMbps +
                            " Mb/s totalRead:" + humanReadableByteCount(readBytes, false) +
                            "/" + humanReadableByteCount(fileSize, false));
                    lastReceivedBytes = receivedBytes;
                    lastReadBytes = readBytes;
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

}
