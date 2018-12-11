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
public class FiverReceiver extends Thread{

    private ServerSocket ss;
    static AtomicBoolean allTransfersCompleted = new AtomicBoolean(false);
    static String baseDir = "/Users/earslan/receivedFiles/";

    static long totalTransferredBytes = 0L;
    static long totalChecksumBytes = 0L;
    long INTEGRITY_VERIFICATION_BLOCK_SIZE = 256 *1024 * 1024;

    boolean debug = false;
    long startTime;

    static LinkedBlockingQueue<Item> items = new LinkedBlockingQueue<>(10000);
    LinkedBlockingQueue<FiverFile> checksumFiles;

    class Item {
        byte[] buffer;
        int length;

        public Item(byte[] buffer, int length){
            this.buffer = Arrays.copyOf(buffer, length);
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


    public FiverReceiver(int port) {
        try {
            ss = new ServerSocket(port);
            //ss.setReceiveBufferSize(134217728);
            //ss.setReceiveBufferSize(33554432);
            checksumFiles = new LinkedBlockingQueue<>();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        while (true) {
            try {
                Socket clientSock = ss.accept();
                clientSock.setSoTimeout(10000);
                System.out.println("Connection established from  " + clientSock.getInetAddress());
                saveFile(clientSock);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
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


        MonitorThread monitorThread = new MonitorThread();
        //monitorThread.start();
        byte[] buffer = new byte[128 * 1024];
        while(true) {
            String fileName = dataInputStream.readUTF();
            long offset = dataInputStream.readLong();
            long fileSize = dataInputStream.readLong();
            if (debug) {
                System.out.println("File " + fileName + "\t" +
                        humanReadableByteCount(fileSize, false) + " bytes" +
                        " time:" + (System.currentTimeMillis() - startTime)/1000.0 + " s");
            }
            checksumFiles.offer(new FiverFile(new File(baseDir + fileName), offset, fileSize));
            //transferLock.release();

            RandomAccessFile randomAccessFile = new RandomAccessFile(baseDir + fileName, "rw");

            if (offset > 0) {
            randomAccessFile.getChannel().position(offset);
            }
            long remaining = fileSize;
            int read = 0;
            long transferStartTime = System.currentTimeMillis();
            while (remaining > 0) {
                read = dataInputStream.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                if (read == -1)
                    break;
                items.offer(new Item(buffer, read), Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                totalTransferredBytes += read;
                remaining -= read;
                randomAccessFile.write(buffer, 0, read);
            }
            randomAccessFile.close();
            if (read == -1) {
                System.out.println("Read -1, closing the connection...");
                return;
            }
            if (debug) {
                System.out.println("Transfer End " + fileName + " size:" + fileSize +
                        " duration:" + (System.currentTimeMillis() - transferStartTime)/1000.0 +
                        " time:" + (System.currentTimeMillis() - startTime)/1000.0);
            }
        }
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
        FiverReceiver fs = new FiverReceiver(2008);
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

            Random generator = new Random(10101010); // Used to inject checksum faults
            int totalFaultInjections = 0;

            int checksumCompletedFileCount  = 0;

            while (true) {
                FiverFile fiverFile = null;
                try {
                    fiverFile = checksumFiles.poll(Long.MAX_VALUE, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (fiverFile == null) {
                    continue;
                }
                String fileName = fiverFile.file.getName();
                long fileOffset = fiverFile.offset;
                long fileLength = fiverFile.length;
                if (debug) {
                    System.out.println("Starting to checksum:" + fileName + " size:" + fileLength + " time:" +
                    (System.currentTimeMillis() - startTime) / 1000.0);
                }
                long init = System.currentTimeMillis();
                Long remaining = fileLength;
                Long processedBytes = 0L;
                Item item;
                //INTEGRITY_VERIFICATION_BLOCK_SIZE = fileLength;
                float random  = generator.nextFloat();
                int blockToInjectFault = -1;
                //System.out.println("Random:" + random);
                if (random < 0) { // error rate
                    totalFaultInjections++;
                    int totalBlocks = (int)(fileLength/INTEGRITY_VERIFICATION_BLOCK_SIZE);
                    blockToInjectFault = ThreadLocalRandom.current().nextInt(Math.max(1,totalBlocks));
                    System.out.println("Injecting failure to file:" + fileName +
                            " block count : " + blockToInjectFault + " totalFaults:" + totalFaultInjections +
                    " fileSize: " + fileLength + " blocksize:" + INTEGRITY_VERIFICATION_BLOCK_SIZE);
                }
                try {
                    int blockCount = 0;
                    while (remaining > 0 || processedBytes > 0L) {
                        Long currentBlockSize = Math.min (INTEGRITY_VERIFICATION_BLOCK_SIZE, remaining + processedBytes);
                        System.out.println("Block size:" + currentBlockSize);
                        byte[] leftOverBytes = null;
                        while (processedBytes < currentBlockSize ) {
                            item = items.poll(100, TimeUnit.MILLISECONDS);
                            if (item == null) {
                                continue;
                            }
                            int usedBytes = item.length;
                            if (item.length + processedBytes > currentBlockSize) {
                                usedBytes = (int)(currentBlockSize - processedBytes);
                                md.update(item.buffer, 0, usedBytes);
                                leftOverBytes = Arrays.copyOfRange(item.buffer, usedBytes , item.length);
                            } else {
                                md.update(item.buffer, 0, item.length);
                            }
                            remaining -= usedBytes;
                            totalChecksumBytes += usedBytes;
                            processedBytes += usedBytes;
                        }
                   /*     if (blockCount == blockToInjectFault) {
                            md.update(Byte.parseByte("1"));
                            System.out.println("Inserted error:" + blockCount );
                        }
                     */   blockCount++;
                        long offset = fileLength - remaining - processedBytes;
                        byte[] digest = md.digest();
                        String hex = (new HexBinaryAdapter()).marshal(digest);
                        if(debug) {
                            System.out.println("File:" + fileName + " offset: " + offset +
                                    " block size : " + processedBytes + " hex  " + hex +
                                    " duration:" +  (System.currentTimeMillis() - init) / 1000.0 +
                                    " s time:" + (System.currentTimeMillis() - startTime) / 1000.0);
                        }

                        System.out.println("Sending hex:" + hex + " bytes:" + processedBytes);
                        dataOutputStream.writeUTF(hex);
                        md.reset();
                        processedBytes = 0L;
                        if (leftOverBytes != null) {
                            md.update(leftOverBytes);
                            remaining -= leftOverBytes.length;
                            totalChecksumBytes += leftOverBytes.length;
                            processedBytes += leftOverBytes.length;
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                //byte[] digest = md.digest();
                //String hex = (new HexBinaryAdapter()).marshal(digest);
                //if (debug)
                System.out.println("File:" + fileName + " finished: " +
                    " time:" + (System.currentTimeMillis() - startTime) / 1000.0);
                //checksumLock.release();
                totalChecksumTime += (System.currentTimeMillis() - init);
                checksumCompletedFileCount++;

            }
            //System.out.println("MyThread - END " + Thread.currentThread().getName() + " total checksum time " +
            //totalChecksumTime/1000.0 + " time: " + (System.currentTimeMillis() - startTime)/1000.0);
        }

    }

    public class MonitorThread extends Thread {
        long lastReceivedBytes = 0;
        long lastReadBytes = 0;
        String currentFile;
        public void setCurrentFile(String currentFile) {
            this.currentFile = currentFile;
        }
        @Override
        public void run() {
            try {
                while (!allTransfersCompleted.get()) {
                    double thrInMbps = 8 * (totalTransferredBytes - lastReceivedBytes) / (1000*1000);
                    double readThrInMbps = 8 * (totalChecksumBytes - lastReadBytes) / (1000*1000);
                    System.out.println("Transfer Thr:" + thrInMbps + " Mb/s Checksum thr:" + readThrInMbps + "items:" + items.size());
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
