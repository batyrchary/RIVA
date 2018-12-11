import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import javax.xml.crypto.Data;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

public class BlockLevelPipeliningReceiver extends Thread{

    private ServerSocket ss;
    Socket clientSock;
    boolean allTransfersCompleted = false;
    static String baseDir = "/Users/earslan/Downloads/java_concurrency/ReceivedFiles/";

    static long totalTransferredBytes = 0L;
    static long totalChecksumBytes = 0L;

    Semaphore checksumLock = new Semaphore(0);
    Semaphore transferLock = new Semaphore(0);

    AtomicBoolean isInterrupted = new AtomicBoolean(false);


    boolean debug = false;
    long startTime;
    public class Block {
        String path;
        String fileName;
        long length;
        long offset;
        public Block (String path, String fileName, long offset, long length) {
            this.path = path;
            this.fileName = fileName;
            this.length = length;
            this.offset = offset;
        }
    }

    public BlockLevelPipeliningReceiver(int port) {
        try {
            ss = new ServerSocket( port);
            //ss.setReceiveBufferSize(33554432);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        while (true) {
            try {
                clientSock = ss.accept();
            //    clientSock.setSoTimeout(10000);
                System.out.println("Connection established from  " + clientSock.getInetAddress());
                saveFile(clientSock);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void saveFile(Socket clientSock) throws IOException {
        isInterrupted.set(false);
        checksumLock.drainPermits();
        transferLock.drainPermits();
        checksumLock.release(2);

        startTime = System.currentTimeMillis();
        DataInputStream dataInputStream  = new DataInputStream(clientSock.getInputStream());

        allTransfersCompleted = false;
        ChecksumRunnable checksumRunnable = new ChecksumRunnable();
        Thread checksumThread = new Thread(checksumRunnable, "checksumThread");
        checksumThread.start();
        MonitorThread monitorThread = new MonitorThread();
        //monitorThread.start();
        byte[] buffer = new byte[128 * 1024];
        while (true) {
            try {
                checksumLock.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String fileName = dataInputStream.readUTF();
            long offset = dataInputStream.readLong();
            long length = dataInputStream.readLong();
            Long blockStartTime = System.currentTimeMillis();
            if (debug) {
                System.out.println("Starting to transfer file:" + baseDir + fileName +
                        " offset: " + offset +
                        " length:" + length);
            }
            RandomAccessFile randomAccessFile = new RandomAccessFile(baseDir + fileName, "rw");
            if (offset > 0) {
                randomAccessFile.getChannel().position(offset);
            }

            long remaining = length;
            int read;
            while ((read = dataInputStream.read(buffer, 0, (int) Math.min(buffer.length, remaining))) > 0) {
                remaining -= read;
                totalTransferredBytes += read;
                randomAccessFile.write(buffer, 0, read);
            }
            if (read == -1) {
                System.out.println("Read -1, closing the connection...");
                isInterrupted.set(true);
                if (transferLock.getQueueLength() > 0) {
                    transferLock.release();
                }
                return;
            }
            randomAccessFile.close();

            if (debug) {
                System.out.println("Transfer END File" + fileName +
                        " offset:" + humanReadableByteCount(offset, false) +
                        " length:" + humanReadableByteCount(length, false) +
                        " duration:" + (System.currentTimeMillis() - blockStartTime) / 1000.0 + " seconds" +
                        " time:" + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
            }
            Block b = new Block(baseDir + fileName, fileName, offset, length);
            checksumRunnable.steCurrentBlock(b);
            transferLock.release();
        }
        //dataInputStream.close();
        //allTransfersCompleted = true;
        //System.out.println("Block level total Time " + (System.currentTimeMillis() - startTime)/1000.0 + " s");
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
        BlockLevelPipeliningReceiver fs = new BlockLevelPipeliningReceiver(2038);
        fs.start();
    }

    public class ChecksumRunnable implements Runnable {
        MessageDigest md = null;
        Block block;

        public void steCurrentBlock(Block block) {
            this.block = block;
        }

        @Override
        public void run() {
            System.out.println("MyThread - START "+Thread.currentThread().getName());
            DataOutputStream dataOutputStream = null;
            try {
                dataOutputStream = new DataOutputStream(clientSock.getOutputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            Random generator = new Random(10101010); // Used to inject checksum faults
            byte[] buffer = new byte[128 * 1024];
            int checksumCompletedBlockCount  = 0;
            int totalFaultInjections = 0;
            while (true) {
                try {
                    transferLock.acquire();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (isInterrupted.get()) {
                    break;
                }
                Block currentBlock = block;
                if (debug) {
                    System.out.println("Starting to checksum:" + currentBlock.path +
                            " offset:" + humanReadableByteCount(currentBlock.offset, false) +
                            " length:" + humanReadableByteCount(currentBlock.length, false) +
                            " time:" + (System.currentTimeMillis() - startTime) / 1000.0 + " sec");
                }
                File f = new File(currentBlock.path);
                long init = System.currentTimeMillis();
                long read;
                try {
                    FileInputStream fis = new FileInputStream(currentBlock.path);
                    if (currentBlock.offset > 0) {
                        fis.getChannel().position(currentBlock.offset);
                    }

                    DigestInputStream dis = new DigestInputStream(fis, md);
                    long remaining = currentBlock.length;

                    while (remaining > 0) {
                            read = dis.read(buffer, 0, (int)Math.min(buffer.length, remaining));
                            if (read == -1) {
                                System.out.println("Read -1, size:" + humanReadableByteCount(f.length(), false) +
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
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                float random  = generator.nextFloat();
              /*  if (random < 0.1 && totalFaultInjections < 8) { //  error rate
                    totalFaultInjections++;
                    System.out.println("Injecting failure to file:" + currentBlock.fileName + " offset: " +
                            currentBlock.offset + " size : " + currentBlock.length + "total injections:" + totalFaultInjections);
                    md.update(Byte.parseByte("1"));
                }*/

                byte[] digest = md.digest();
                String hex = (new HexBinaryAdapter()).marshal(digest);
                try {
                    dataOutputStream.writeUTF(hex);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (debug) {
                    System.out.println("Finished checksum :" + currentBlock.fileName +
                            " offset:" + humanReadableByteCount(currentBlock.offset, false) +
                            " length:" + humanReadableByteCount(currentBlock.length, false) +
                            " Checksum  " + hex + " duration " + (System.currentTimeMillis() - init) / 1000.0 + " s" +
                            " time:" + (System.currentTimeMillis() - startTime) / 1000.0 + " sec");
                }

                checksumCompletedBlockCount++;
                md.reset();
                checksumLock.release();
                if (isInterrupted.get()) {
                    break;
                }
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
                    System.out.println("Transfer:" + thrInMbps + " Mb/s Checksum:" + readThrInMbps + " Mb/s");
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
